use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{broadcast, oneshot, RwLock};
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

use crate::api::AppState;
use crate::auth::access_control::{AccessControlDb, SqlMode};
use crate::auth::{authenticate, AuthResult};

// ============================================================================
// Types
// ============================================================================

pub enum ApprovalDecision {
    Approved,
    Rejected { reason: String },
}

pub struct PendingApproval {
    pub id: String,
    pub user_email: String,
    pub tool_name: String,
    pub sql_statements: Vec<String>,
    pub target_connection: String,
    pub target_database: String,
    pub context: String,
    pub created_at: DateTime<Utc>,
    pub response_tx: oneshot::Sender<ApprovalDecision>,
}

/// Summary returned in list endpoint (no SQL).
#[derive(Clone, Serialize)]
pub struct ApprovalSummary {
    pub id: String,
    pub user_email: String,
    pub tool_name: String,
    pub target_connection: String,
    pub target_database: String,
    pub context: String,
    pub created_at: String,
}

/// Full detail returned in get endpoint (includes SQL).
#[derive(Serialize)]
pub struct ApprovalDetail {
    pub id: String,
    pub user_email: String,
    pub tool_name: String,
    pub sql_statements: Vec<String>,
    pub target_connection: String,
    pub target_database: String,
    pub context: String,
    pub created_at: String,
}

/// Event broadcast when a new approval is submitted.
#[derive(Debug, Clone, Serialize)]
pub struct ApprovalEvent {
    pub event_type: String, // "new_approval", "resolved"
    pub approval_id: String,
    pub user_email: String,
    pub tool_name: String,
    pub target_connection: String,
    pub target_database: String,
    pub context: String,
}

// ============================================================================
// Registry
// ============================================================================

pub struct ApprovalRegistry {
    pending: RwLock<HashMap<String, PendingApproval>>,
    pub event_tx: broadcast::Sender<ApprovalEvent>,
}

impl ApprovalRegistry {
    pub fn new() -> Self {
        let (event_tx, _) = broadcast::channel(64);
        Self {
            pending: RwLock::new(HashMap::new()),
            event_tx,
        }
    }

    /// Submit a pending approval. Returns a receiver the caller awaits for the decision.
    /// Enforces per-user pending limit (default 6).
    pub async fn submit(
        &self,
        approval: PendingApproval,
        max_pending: u32,
    ) -> Result<oneshot::Receiver<ApprovalDecision>, String> {
        let (tx, rx) = oneshot::channel();
        let mut map = self.pending.write().await;

        // Enforce per-user pending limit
        let count = map
            .values()
            .filter(|a| a.user_email == approval.user_email)
            .count();
        if count >= max_pending as usize {
            return Err(format!("Too many pending approvals (max {}). Wait for existing approvals to be resolved.", max_pending));
        }

        let event = ApprovalEvent {
            event_type: "new_approval".to_string(),
            approval_id: approval.id.clone(),
            user_email: approval.user_email.clone(),
            tool_name: approval.tool_name.clone(),
            target_connection: approval.target_connection.clone(),
            target_database: approval.target_database.clone(),
            context: approval.context.clone(),
        };

        let id = approval.id.clone();
        let entry = PendingApproval {
            id: approval.id,
            user_email: approval.user_email,
            tool_name: approval.tool_name,
            sql_statements: approval.sql_statements,
            target_connection: approval.target_connection,
            target_database: approval.target_database,
            context: approval.context,
            created_at: approval.created_at,
            response_tx: tx,
        };
        map.insert(id, entry);

        // Broadcast event (ignore if no listeners)
        let _ = self.event_tx.send(event);

        Ok(rx)
    }

    /// List pending approvals visible to this user.
    /// Admin sees all. Otherwise uses can_approve() for delegated authority.
    pub async fn list_pending(
        &self,
        email: &str,
        is_admin: bool,
        access_db: Option<&AccessControlDb>,
    ) -> Vec<ApprovalSummary> {
        let map = self.pending.read().await;
        map.values()
            .filter(|a| {
                if is_admin {
                    return true;
                }
                if a.user_email == email {
                    return true;
                }
                if let Some(db) = access_db {
                    return db.can_approve(email, &a.user_email);
                }
                false
            })
            .map(|a| ApprovalSummary {
                id: a.id.clone(),
                user_email: a.user_email.clone(),
                tool_name: a.tool_name.clone(),
                target_connection: a.target_connection.clone(),
                target_database: a.target_database.clone(),
                context: a.context.clone(),
                created_at: a.created_at.to_rfc3339(),
            })
            .collect()
    }

    /// Get full detail for a specific approval.
    pub async fn get_pending(
        &self,
        id: &str,
        email: &str,
        is_admin: bool,
        access_db: Option<&AccessControlDb>,
    ) -> Option<ApprovalDetail> {
        let map = self.pending.read().await;
        map.get(id)
            .filter(|a| {
                if is_admin || a.user_email == email {
                    return true;
                }
                if let Some(db) = access_db {
                    return db.can_approve(email, &a.user_email);
                }
                false
            })
            .map(|a| ApprovalDetail {
                id: a.id.clone(),
                user_email: a.user_email.clone(),
                tool_name: a.tool_name.clone(),
                sql_statements: a.sql_statements.clone(),
                target_connection: a.target_connection.clone(),
                target_database: a.target_database.clone(),
                context: a.context.clone(),
                created_at: a.created_at.to_rfc3339(),
            })
    }

    /// Approve a pending request. Returns Err if not found or not authorized.
    pub async fn approve(
        &self,
        id: &str,
        email: &str,
        is_admin: bool,
        access_db: Option<&AccessControlDb>,
    ) -> Result<(), String> {
        let mut map = self.pending.write().await;
        let entry = map.get(id).ok_or("Approval not found or already resolved")?;
        let is_self = entry.user_email == email;
        let self_blocked = is_self && access_db.map_or(false, |db| {
            db.get_sql_mode(&entry.user_email) == SqlMode::Supervised
        });
        let authorized = !self_blocked && (is_admin
            || is_self
            || access_db.map_or(false, |db| db.can_approve(email, &entry.user_email)));
        if !authorized {
            return Err("Permission denied".into());
        }
        let entry = map.remove(id).unwrap();

        // Broadcast resolved event
        let _ = self.event_tx.send(ApprovalEvent {
            event_type: "resolved".to_string(),
            approval_id: id.to_string(),
            user_email: entry.user_email.clone(),
            tool_name: entry.tool_name.clone(),
            target_connection: entry.target_connection.clone(),
            target_database: entry.target_database.clone(),
            context: entry.context.clone(),
        });

        let _ = entry.response_tx.send(ApprovalDecision::Approved);
        Ok(())
    }

    /// Reject a pending request. Returns Err if not found or not authorized.
    pub async fn reject(
        &self,
        id: &str,
        email: &str,
        is_admin: bool,
        reason: String,
        access_db: Option<&AccessControlDb>,
    ) -> Result<(), String> {
        let mut map = self.pending.write().await;
        let entry = map.get(id).ok_or("Approval not found or already resolved")?;
        let is_self = entry.user_email == email;
        let self_blocked = is_self && access_db.map_or(false, |db| {
            db.get_sql_mode(&entry.user_email) == SqlMode::Supervised
        });
        let authorized = !self_blocked && (is_admin
            || is_self
            || access_db.map_or(false, |db| db.can_approve(email, &entry.user_email)));
        if !authorized {
            return Err("Permission denied".into());
        }
        let entry = map.remove(id).unwrap();

        // Broadcast resolved event
        let _ = self.event_tx.send(ApprovalEvent {
            event_type: "resolved".to_string(),
            approval_id: id.to_string(),
            user_email: entry.user_email.clone(),
            tool_name: entry.tool_name.clone(),
            target_connection: entry.target_connection.clone(),
            target_database: entry.target_database.clone(),
            context: entry.context.clone(),
        });

        let _ = entry.response_tx.send(ApprovalDecision::Rejected { reason });
        Ok(())
    }

    /// Remove an entry by id (used on timeout).
    pub async fn remove(&self, id: &str) {
        let mut map = self.pending.write().await;
        map.remove(id);
    }

    /// Background cleanup: remove entries older than 5 minutes.
    pub fn spawn_cleanup_task(self: &Arc<Self>) {
        let registry = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                let now = Utc::now();
                let mut map = registry.pending.write().await;
                map.retain(|_, a| {
                    (now - a.created_at).num_seconds() < 300
                });
            }
        });
    }
}

impl PendingApproval {
    pub fn new(
        user_email: String,
        tool_name: &str,
        sql_statements: Vec<String>,
        target_connection: &str,
        target_database: &str,
        context: &str,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            user_email,
            tool_name: tool_name.to_string(),
            sql_statements,
            target_connection: target_connection.to_string(),
            target_database: target_database.to_string(),
            context: context.to_string(),
            created_at: Utc::now(),
            response_tx: oneshot::channel().0, // placeholder — caller replaces via submit()
        }
    }
}

// ============================================================================
// Webhook Helper
// ============================================================================

/// Fire webhooks for the user's teams (Slack-compatible payload).
/// Runs fire-and-forget — errors are logged but don't block.
pub fn fire_webhooks(
    access_db: Option<&Arc<AccessControlDb>>,
    user_email: &str,
    tool_name: &str,
    target_connection: &str,
    target_database: &str,
    base_url: &str,
) {
    let db = match access_db {
        Some(db) => Arc::clone(db),
        None => return,
    };
    let urls = db.get_team_webhooks_for_user(user_email);
    if urls.is_empty() {
        return;
    }
    let user_email = user_email.to_string();
    let tool_name = tool_name.to_string();
    let target = format!("{}.{}", target_connection, target_database);
    let base_url = base_url.to_string();
    tokio::spawn(async move {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        let payload = json!({
            "text": format!("Approval requested by {}", user_email),
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": format!("*Tool:* {}\n*Database:* {}\n*User:* {}", tool_name, target, user_email)
                    }
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": { "type": "plain_text", "text": "Review" },
                            "url": format!("{}/approvals", base_url)
                        }
                    ]
                }
            ]
        });
        for url in urls {
            let resp = client.post(&url).json(&payload).send().await;
            if let Err(e) = resp {
                tracing::warn!("Webhook to {} failed: {}", url, e);
            }
        }
    });
}

// ============================================================================
// REST Handlers
// ============================================================================

/// GET /api/lane/approvals — list pending approvals
pub async fn list_approvals_handler(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Response {
    let (email, is_admin) = match authenticate(&headers, &state).await {
        AuthResult::SessionAccess { email, is_admin } => (email, is_admin),
        AuthResult::FullAccess => ("admin".to_string(), true),
        _ => return (axum::http::StatusCode::UNAUTHORIZED, json!({"error": "Unauthorized"}).to_string()).into_response(),
    };

    let registry = match &state.approval_registry {
        Some(r) => r,
        None => return Json(json!([])).into_response(),
    };

    let items = registry
        .list_pending(&email, is_admin, state.access_db.as_deref())
        .await;
    Json(json!(items)).into_response()
}

/// GET /api/lane/approvals/{id} — get full detail
pub async fn get_approval_handler(
    headers: HeaderMap,
    Path(id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Response {
    let (email, is_admin) = match authenticate(&headers, &state).await {
        AuthResult::SessionAccess { email, is_admin } => (email, is_admin),
        AuthResult::FullAccess => ("admin".to_string(), true),
        _ => return (axum::http::StatusCode::UNAUTHORIZED, json!({"error": "Unauthorized"}).to_string()).into_response(),
    };

    let registry = match &state.approval_registry {
        Some(r) => r,
        None => return (axum::http::StatusCode::NOT_FOUND, json!({"error": "Approval not found"}).to_string()).into_response(),
    };

    match registry
        .get_pending(&id, &email, is_admin, state.access_db.as_deref())
        .await
    {
        Some(detail) => Json(json!(detail)).into_response(),
        None => (axum::http::StatusCode::NOT_FOUND, json!({"error": "Approval not found"}).to_string()).into_response(),
    }
}

/// POST /api/lane/approvals/{id}/approve
pub async fn approve_handler(
    headers: HeaderMap,
    Path(id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Response {
    let (email, is_admin) = match authenticate(&headers, &state).await {
        AuthResult::SessionAccess { email, is_admin } => (email, is_admin),
        AuthResult::FullAccess => ("admin".to_string(), true),
        _ => return (axum::http::StatusCode::UNAUTHORIZED, json!({"error": "Unauthorized"}).to_string()).into_response(),
    };

    let registry = match &state.approval_registry {
        Some(r) => r,
        None => return (axum::http::StatusCode::NOT_FOUND, json!({"error": "Approval not found"}).to_string()).into_response(),
    };

    // Record in audit trail
    if let Some(ref access_db) = state.access_db {
        let _ = access_db.record_approval_decision(&id, "approved", None);
    }

    match registry
        .approve(&id, &email, is_admin, state.access_db.as_deref())
        .await
    {
        Ok(()) => Json(json!({"success": true, "approved_by": email})).into_response(),
        Err(e) => (axum::http::StatusCode::NOT_FOUND, json!({"error": e}).to_string()).into_response(),
    }
}

#[derive(Deserialize)]
pub struct RejectBody {
    pub reason: Option<String>,
}

/// POST /api/lane/approvals/{id}/reject
pub async fn reject_handler(
    headers: HeaderMap,
    Path(id): Path<String>,
    State(state): State<Arc<AppState>>,
    Json(body): Json<RejectBody>,
) -> Response {
    let (email, is_admin) = match authenticate(&headers, &state).await {
        AuthResult::SessionAccess { email, is_admin } => (email, is_admin),
        AuthResult::FullAccess => ("admin".to_string(), true),
        _ => return (axum::http::StatusCode::UNAUTHORIZED, json!({"error": "Unauthorized"}).to_string()).into_response(),
    };

    let registry = match &state.approval_registry {
        Some(r) => r,
        None => return (axum::http::StatusCode::NOT_FOUND, json!({"error": "Approval not found"}).to_string()).into_response(),
    };

    let reason = body.reason.unwrap_or_else(|| "Rejected by user".to_string());

    // Record in audit trail
    if let Some(ref access_db) = state.access_db {
        let _ = access_db.record_approval_decision(&id, "rejected", Some(&reason));
    }

    match registry
        .reject(&id, &email, is_admin, reason, state.access_db.as_deref())
        .await
    {
        Ok(()) => Json(json!({"success": true, "rejected_by": email})).into_response(),
        Err(e) => (axum::http::StatusCode::NOT_FOUND, json!({"error": e}).to_string()).into_response(),
    }
}

// ============================================================================
// SSE Endpoint
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct ApprovalEventsQuery {
    pub token: Option<String>,
}

/// GET /api/lane/approvals/events?token={session}
/// SSE endpoint for real-time approval notifications.
pub async fn approval_events_handler(
    headers: HeaderMap,
    Query(query): Query<ApprovalEventsQuery>,
    State(state): State<Arc<AppState>>,
) -> Response {
    // Auth: try query param token first (EventSource can't set headers), then headers
    let (email, is_admin) = if let Some(ref token) = query.token {
        if let Some(ref access_db) = state.access_db {
            match access_db.validate_session(token) {
                Ok(session) => (session.email.clone(), session.is_admin),
                Err(_) => {
                    return (
                        axum::http::StatusCode::UNAUTHORIZED,
                        json!({"error": "Invalid session"}).to_string(),
                    )
                        .into_response()
                }
            }
        } else {
            ("admin".to_string(), true)
        }
    } else {
        match authenticate(&headers, &state).await {
            AuthResult::SessionAccess { email, is_admin } => (email, is_admin),
            AuthResult::FullAccess => ("admin".to_string(), true),
            _ => {
                return (
                    axum::http::StatusCode::UNAUTHORIZED,
                    json!({"error": "Unauthorized"}).to_string(),
                )
                    .into_response()
            }
        }
    };

    let registry = match &state.approval_registry {
        Some(r) => r,
        None => {
            return (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                json!({"error": "Approvals not enabled"}).to_string(),
            )
                .into_response()
        }
    };

    let rx = registry.event_tx.subscribe();
    let access_db = state.access_db.clone();

    let stream = BroadcastStream::new(rx).filter_map(move |result| {
        let email = email.clone();
        let access_db = access_db.clone();
        match result {
            Ok(event) => {
                // Filter: only send events this user can see
                let visible = is_admin
                    || event.user_email == email
                    || access_db
                        .as_deref()
                        .map_or(false, |db| db.can_approve(&email, &event.user_email));
                if visible {
                    let data = serde_json::to_string(&event).unwrap_or_default();
                    Some(Ok::<_, Infallible>(
                        Event::default().event(&event.event_type).data(data),
                    ))
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    });

    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}
