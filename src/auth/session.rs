use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Redirect, Response},
    Json,
};
use base64::Engine;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::api::AppState;
use crate::auth::{authenticate, extract_session_token, extract_tailscale_identity, AuthProvider, AuthResult};

// ============================================================================
// Login rate limiter — tracks failed login attempts per IP
// ============================================================================

const MAX_FAILED_ATTEMPTS: usize = 5;
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(15 * 60); // 15 minutes

pub struct LoginRateLimiter {
    /// Map of IP address string -> timestamps of failed attempts
    failures: std::sync::Mutex<HashMap<String, Vec<Instant>>>,
}

impl LoginRateLimiter {
    pub fn new() -> Self {
        Self {
            failures: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Check if the given IP is allowed to attempt login.
    /// Prunes entries older than the rate limit window before checking.
    /// Returns true if allowed, false if rate-limited.
    pub fn check_rate_limit(&self, ip: &str) -> bool {
        let mut map = self.failures.lock().unwrap();
        let now = Instant::now();
        if let Some(attempts) = map.get_mut(ip) {
            attempts.retain(|t| now.duration_since(*t) < RATE_LIMIT_WINDOW);
            attempts.len() < MAX_FAILED_ATTEMPTS
        } else {
            true
        }
    }

    /// Record a failed login attempt for the given IP.
    pub fn record_failure(&self, ip: &str) {
        let mut map = self.failures.lock().unwrap();
        map.entry(ip.to_string())
            .or_insert_with(Vec::new)
            .push(Instant::now());
    }

    /// Remove all entries with no recent failures. Called from the hourly cleanup task.
    pub fn cleanup(&self) {
        let mut map = self.failures.lock().unwrap();
        let now = Instant::now();
        map.retain(|_, attempts| {
            attempts.retain(|t| now.duration_since(*t) < RATE_LIMIT_WINDOW);
            !attempts.is_empty()
        });
    }
}

// ============================================================================
// GET /api/auth/status — unauthenticated, returns setup/auth state
// ============================================================================

pub async fn auth_status_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let ts_auth = state.auth_providers.contains(&AuthProvider::Tailscale);
    let providers: Vec<&AuthProvider> = state.auth_providers.iter().collect();
    let auth_providers_json = serde_json::to_value(&providers).unwrap_or(json!([]));
    let smtp_configured = state.smtp_config.is_some();

    let access_db = match state.access_db.as_ref() {
        Some(db) => db,
        None => {
            return (
                StatusCode::OK,
                Json(json!({
                    "needs_setup": false,
                    "authenticated": false,
                    "user": null,
                    "tailscale_auth": ts_auth,
                    "auth_providers": auth_providers_json,
                    "smtp_configured": smtp_configured,
                })),
            )
                .into_response()
        }
    };

    let needs_setup = access_db.needs_setup().unwrap_or(false);

    if needs_setup {
        return (
            StatusCode::OK,
            Json(json!({
                "needs_setup": true,
                "authenticated": false,
                "user": null,
                "tailscale_auth": ts_auth,
                "auth_providers": auth_providers_json,
                "smtp_configured": smtp_configured,
            })),
        )
            .into_response();
    }

    // Check if caller is authenticated
    let auth = authenticate(&headers, &state).await;
    // Helper to build user response with optional team memberships
    let build_user_response = |email: &str, is_admin: bool| {
        let teams: serde_json::Value = access_db
            .get_user_teams(email)
            .map(|memberships| {
                serde_json::to_value(
                    memberships
                        .iter()
                        .map(|m| {
                            json!({
                                "id": m.team_id,
                                "name": m.team_name,
                                "role": m.role,
                            })
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap_or(json!([]))
            })
            .unwrap_or(json!([]));
        json!({
            "needs_setup": false,
            "authenticated": true,
            "user": { "email": email, "is_admin": is_admin, "teams": teams },
            "tailscale_auth": ts_auth,
            "auth_providers": auth_providers_json,
            "smtp_configured": smtp_configured,
        })
    };

    match auth {
        AuthResult::SessionAccess { email, is_admin } => {
            (StatusCode::OK, Json(build_user_response(&email, is_admin))).into_response()
        }
        AuthResult::FullAccess => (
            StatusCode::OK,
            Json(json!({
                "needs_setup": false,
                "authenticated": true,
                "user": { "email": "system", "is_admin": true, "teams": [] },
                "tailscale_auth": ts_auth,
                "auth_providers": auth_providers_json,
                "smtp_configured": smtp_configured,
            })),
        )
            .into_response(),
        AuthResult::TokenAccess { email, .. } => {
            let is_admin = access_db.is_admin(&email);
            (StatusCode::OK, Json(build_user_response(&email, is_admin))).into_response()
        }
        _ => (
            StatusCode::OK,
            Json(json!({
                "needs_setup": false,
                "authenticated": false,
                "user": null,
                "tailscale_auth": ts_auth,
                "auth_providers": auth_providers_json,
                "smtp_configured": smtp_configured,
            })),
        )
            .into_response(),
    }
}

// ============================================================================
// POST /api/auth/setup — first-run admin creation
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct SetupRequest {
    pub email: String,
    pub display_name: Option<String>,
    pub password: String,
    pub phone: Option<String>,
}

pub async fn setup_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<SetupRequest>,
) -> Response {
    let access_db = match state.access_db.as_ref() {
        Some(db) => db,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "Access control not initialized"})),
            )
                .into_response()
        }
    };

    // Guard: only works if no users exist
    match access_db.needs_setup() {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::CONFLICT,
                Json(json!({"error": "Setup already completed"})),
            )
                .into_response()
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e})),
            )
                .into_response()
        }
    }

    if body.password.len() < 8 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Password must be at least 8 characters"})),
        )
            .into_response();
    }

    // Create admin account
    if let Err(e) = access_db.setup_admin(
        &body.email,
        body.display_name.as_deref(),
        &body.password,
        body.phone.as_deref(),
    ) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e})),
        )
            .into_response();
    }

    // Return the system API key
    let api_key = state.api_key.read().await.clone();
    (
        StatusCode::OK,
        Json(json!({
            "success": true,
            "email": body.email,
            "api_key": api_key,
            "message": "Admin account created. Save this API key for programmatic access."
        })),
    )
        .into_response()
}

// ============================================================================
// POST /api/auth/login — email + password → session
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

pub async fn login_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<LoginRequest>,
) -> Response {
    let access_db = match state.access_db.as_ref() {
        Some(db) => db,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "Access control not initialized"})),
            )
                .into_response()
        }
    };

    // Extract client IP for rate limiting
    let client_ip = headers
        .get("x-forwarded-for")
        .or_else(|| headers.get("x-real-ip"))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .to_string();

    // Check rate limit before verifying password
    if !state.login_rate_limiter.check_rate_limit(&client_ip) {
        tracing::warn!("Login rate limit exceeded for IP: {}", client_ip);
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error": "Too many failed login attempts. Please try again later."})),
        )
            .into_response();
    }

    // Verify password
    match access_db.verify_password(&body.email, &body.password) {
        Ok(true) => {}
        Ok(false) => {
            state.login_rate_limiter.record_failure(&client_ip);
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "Invalid email or password"})),
            )
                .into_response()
        }
        Err(e) => {
            state.login_rate_limiter.record_failure(&client_ip);
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": e})),
            )
                .into_response()
        }
    }

    // Extract client info
    let ip = headers
        .get("x-forwarded-for")
        .or_else(|| headers.get("x-real-ip"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let user_agent = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Create session (24h)
    let token = match access_db.create_session(
        &body.email,
        ip.as_deref(),
        user_agent.as_deref(),
        24,
    ) {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Failed to create session: {}", e)})),
            )
                .into_response()
        }
    };

    let is_admin = access_db.is_admin(&body.email);

    // Set cookie + return token in body
    let cookie = format!(
        "session={}; HttpOnly; SameSite=Strict; Path=/; Max-Age=86400",
        token
    );

    (
        StatusCode::OK,
        [("set-cookie", cookie.as_str())],
        Json(json!({
            "success": true,
            "session_token": token,
            "email": body.email,
            "is_admin": is_admin,
        })),
    )
        .into_response()
}

// ============================================================================
// POST /api/auth/logout — destroy session
// ============================================================================

pub async fn logout_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if let Some(ref access_db) = state.access_db {
        if let Some(token) = extract_session_token(&headers) {
            let _ = access_db.delete_session(&token);
        }
    }

    let clear_cookie = "session=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0";

    (
        StatusCode::OK,
        [("set-cookie", clear_cookie)],
        Json(json!({"success": true})),
    )
        .into_response()
}

// ============================================================================
// POST /api/auth/password — change own password
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct ChangePasswordRequest {
    pub current_password: String,
    pub new_password: String,
}

pub async fn change_password_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<ChangePasswordRequest>,
) -> Response {
    let auth = authenticate(&headers, &state).await;
    let email = match auth {
        AuthResult::SessionAccess { ref email, .. } => email.clone(),
        AuthResult::TokenAccess { ref email, .. } => email.clone(),
        _ => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "Authentication required"})),
            )
                .into_response()
        }
    };

    let access_db = match state.access_db.as_ref() {
        Some(db) => db,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "Access control not initialized"})),
            )
                .into_response()
        }
    };

    // Verify current password
    match access_db.verify_password(&email, &body.current_password) {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "Current password is incorrect"})),
            )
                .into_response()
        }
        Err(e) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": e})),
            )
                .into_response()
        }
    }

    if body.new_password.len() < 8 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Password must be at least 8 characters"})),
        )
            .into_response();
    }

    match access_db.set_password(&email, &body.new_password) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({"success": true})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

// ============================================================================
// POST /api/auth/tailscale — Tailscale identity → session
// ============================================================================

pub async fn tailscale_login_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if !state.auth_providers.contains(&AuthProvider::Tailscale) {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "Tailscale auth is not enabled"})),
        )
            .into_response();
    }

    let (email, _name) = match extract_tailscale_identity(&headers) {
        Some(id) => id,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "Missing Tailscale identity headers"})),
            )
                .into_response()
        }
    };

    let access_db = match state.access_db.as_ref() {
        Some(db) => db,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "Access control not initialized"})),
            )
                .into_response()
        }
    };

    // Reject users not pre-created by an admin
    if !access_db.user_exists(&email) {
        tracing::warn!("Tailscale login denied for unknown user: {} — admin must create the user first", email);
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "Your account does not exist. Contact an administrator to be added."})),
        )
            .into_response();
    }

    // Extract client info
    let ip = headers
        .get("x-forwarded-for")
        .or_else(|| headers.get("x-real-ip"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let user_agent = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Create session (24h)
    let token = match access_db.create_session(
        &email,
        ip.as_deref(),
        user_agent.as_deref(),
        24,
    ) {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Failed to create session: {}", e)})),
            )
                .into_response()
        }
    };

    let is_admin = access_db.is_admin(&email);

    let cookie = format!(
        "session={}; HttpOnly; SameSite=Strict; Path=/; Max-Age=86400",
        token
    );

    (
        StatusCode::OK,
        [("set-cookie", cookie.as_str())],
        Json(json!({
            "success": true,
            "email": email,
            "is_admin": is_admin,
        })),
    )
        .into_response()
}

// ============================================================================
// GET /api/auth/oidc/{provider}/authorize — start OIDC flow
// ============================================================================

pub async fn oidc_authorize_handler(
    State(state): State<Arc<AppState>>,
    Path(provider_name): Path<String>,
) -> Response {
    let provider = match AuthProvider::from_str(&provider_name) {
        Some(p) if p.is_oidc() => p,
        _ => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": format!("Unknown OIDC provider: {}", provider_name)})),
            )
                .into_response()
        }
    };

    if !state.auth_providers.contains(&provider) {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("{} auth is not enabled", provider_name)})),
        )
            .into_response();
    }

    let oidc_config = match state.oidc_configs.get(&provider) {
        Some(c) => c,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "OIDC config not found"})),
            )
                .into_response()
        }
    };

    let base_url = match state.base_url.as_ref() {
        Some(u) => u.trim_end_matches('/'),
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "LANE_BASE_URL not configured"})),
            )
                .into_response()
        }
    };

    let access_db = match state.access_db.as_ref() {
        Some(db) => db,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "Access control not initialized"})),
            )
                .into_response()
        }
    };

    // Generate PKCE challenge + verifier
    let (pkce_challenge, pkce_verifier) = oauth2::PkceCodeChallenge::new_random_sha256();

    // Generate random state
    let state_param = oauth2::CsrfToken::new_random().secret().clone();

    let redirect_uri = format!("{}/api/auth/oidc/{}/callback", base_url, provider_name);

    // Store state + verifier in DB
    if let Err(e) = access_db.store_oauth_state(
        &state_param,
        &provider_name,
        pkce_verifier.secret(),
        &redirect_uri,
    ) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Failed to store OAuth state: {}", e)})),
        )
            .into_response();
    }

    // Build authorization URL
    let scopes = oidc_config
        .scopes
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<_>>()
        .join(" ");

    // Build authorization URL using url::Url for safe query param encoding
    let mut auth_url = url::Url::parse(&oidc_config.auth_url)
        .map_err(|e| format!("Invalid auth URL: {}", e))
        .unwrap();
    auth_url.query_pairs_mut()
        .append_pair("client_id", &oidc_config.client_id)
        .append_pair("redirect_uri", &redirect_uri)
        .append_pair("response_type", "code")
        .append_pair("scope", &scopes)
        .append_pair("state", &state_param)
        .append_pair("code_challenge", pkce_challenge.as_str())
        .append_pair("code_challenge_method", "S256");

    Redirect::temporary(auth_url.as_str()).into_response()
}

// ============================================================================
// GET /api/auth/oidc/{provider}/callback — handle OIDC redirect
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct OidcCallbackQuery {
    pub code: String,
    pub state: String,
}

pub async fn oidc_callback_handler(
    State(state): State<Arc<AppState>>,
    Path(provider_name): Path<String>,
    Query(query): Query<OidcCallbackQuery>,
    headers: HeaderMap,
) -> Response {
    let access_db = match state.access_db.as_ref() {
        Some(db) => db,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Access control not initialized".into_response(),
            )
                .into_response()
        }
    };

    // Validate and consume OAuth state
    let (stored_provider, pkce_verifier, redirect_uri) =
        match access_db.consume_oauth_state(&query.state) {
            Ok(v) => v,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": format!("Invalid or expired OAuth state: {}", e)})),
                )
                    .into_response()
            }
        };

    if stored_provider != provider_name {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Provider mismatch"})),
        )
            .into_response();
    }

    let provider = match AuthProvider::from_str(&provider_name) {
        Some(p) if p.is_oidc() => p,
        _ => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "Unknown provider"})),
            )
                .into_response()
        }
    };

    let oidc_config = match state.oidc_configs.get(&provider) {
        Some(c) => c,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "OIDC config not found"})),
            )
                .into_response()
        }
    };

    // Exchange code for tokens
    let client = reqwest::Client::new();
    let token_response = match client
        .post(&oidc_config.token_url)
        .header("Accept", "application/json")
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", &query.code),
            ("redirect_uri", &redirect_uri),
            ("client_id", &oidc_config.client_id),
            ("client_secret", &oidc_config.client_secret),
            ("code_verifier", &pkce_verifier),
        ])
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Token exchange failed: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "Token exchange failed"})),
            )
                .into_response();
        }
    };

    let token_body: serde_json::Value = match token_response.json().await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("Failed to parse token response: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "Failed to parse token response"})),
            )
                .into_response();
        }
    };

    if let Some(error) = token_body.get("error") {
        let error_desc = token_body.get("error_description").and_then(|v| v.as_str()).unwrap_or("");
        tracing::error!("OAuth token error: {} — {}", error, error_desc);
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Authentication failed with the identity provider"})),
        )
            .into_response();
    }

    let access_token = token_body
        .get("access_token")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Extract email based on provider
    let (email, _display_name) = match provider {
        AuthProvider::GitHub => {
            match extract_github_email(&client, &access_token).await {
                Ok((e, n)) => (e, n),
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": format!("Failed to get GitHub email: {}", e)})),
                    )
                        .into_response()
                }
            }
        }
        _ => {
            // Google / Microsoft: decode ID token
            let id_token = match token_body.get("id_token").and_then(|v| v.as_str()) {
                Some(t) => t,
                None => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "No id_token in response"})),
                    )
                        .into_response()
                }
            };
            match extract_email_from_id_token(id_token, &provider) {
                Ok((e, n)) => (e, n),
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": format!("Failed to extract email: {}", e)})),
                    )
                        .into_response()
                }
            }
        }
    };

    if email.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Could not determine email from OIDC provider"})),
        )
            .into_response();
    }

    // Reject users not pre-created by an admin
    if !access_db.user_exists(&email) {
        tracing::warn!("OIDC login denied for unknown user ({}): {} — admin must create the user first", provider_name, email);
        // Redirect to login with error message instead of returning JSON (this is a browser redirect flow)
        let redirect_url = "/login?error=account_not_found";
        return axum::response::Redirect::temporary(&redirect_url).into_response();
    }

    // Extract client info
    let ip = headers
        .get("x-forwarded-for")
        .or_else(|| headers.get("x-real-ip"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let user_agent = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Create session (24h)
    let session_token = match access_db.create_session(
        &email,
        ip.as_deref(),
        user_agent.as_deref(),
        24,
    ) {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Failed to create session: {}", e)})),
            )
                .into_response()
        }
    };

    // Set cookie with SameSite=Lax (required for cross-origin redirect from provider)
    let cookie = format!(
        "session={}; HttpOnly; SameSite=Lax; Path=/; Max-Age=86400",
        session_token
    );

    (
        StatusCode::TEMPORARY_REDIRECT,
        [
            ("set-cookie", cookie.as_str()),
            ("location", "/"),
        ],
        "",
    )
        .into_response()
}

/// Extract email from a JWT ID token (Google/Microsoft) without signature verification.
/// Safe because the token was received directly over TLS from the token endpoint.
fn extract_email_from_id_token(id_token: &str, provider: &AuthProvider) -> Result<(String, String), String> {
    let parts: Vec<&str> = id_token.split('.').collect();
    if parts.len() < 2 {
        return Err("Invalid ID token format".to_string());
    }

    // Decode the payload (second part)
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .map_err(|e| format!("Failed to decode ID token payload: {}", e))?;

    let claims: serde_json::Value =
        serde_json::from_slice(&payload).map_err(|e| format!("Failed to parse ID token: {}", e))?;

    // Reject unverified emails — prevents account takeover via unverified provider accounts
    let email_verified = claims
        .get("email_verified")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let email = claims
        .get("email")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    if !email.is_empty() && !email_verified {
        return Err("Email address is not verified by the identity provider".to_string());
    }

    // Microsoft fallback: preferred_username (typically a verified org email)
    let email = if email.is_empty() && matches!(provider, AuthProvider::Microsoft) {
        claims
            .get("preferred_username")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    } else {
        email
    };

    let name = claims
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    Ok((email, name))
}

/// Fetch primary verified email from GitHub API.
async fn extract_github_email(
    client: &reqwest::Client,
    access_token: &str,
) -> Result<(String, String), String> {
    // Get display name from /user
    let user_resp = client
        .get("https://api.github.com/user")
        .header("Authorization", format!("Bearer {}", access_token))
        .header("Accept", "application/json")
        .header("User-Agent", "lane")
        .send()
        .await
        .map_err(|e| format!("GitHub /user request failed: {}", e))?;

    let user_body: serde_json::Value = user_resp
        .json()
        .await
        .map_err(|e| format!("Failed to parse /user: {}", e))?;

    let display_name = user_body
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Get emails
    let resp = client
        .get("https://api.github.com/user/emails")
        .header("Authorization", format!("Bearer {}", access_token))
        .header("Accept", "application/json")
        .header("User-Agent", "lane")
        .send()
        .await
        .map_err(|e| format!("GitHub email request failed: {}", e))?;

    let emails: Vec<serde_json::Value> = resp
        .json()
        .await
        .map_err(|e| format!("Failed to parse GitHub emails: {}", e))?;

    // Find primary verified email
    for entry in &emails {
        let primary = entry.get("primary").and_then(|v| v.as_bool()).unwrap_or(false);
        let verified = entry.get("verified").and_then(|v| v.as_bool()).unwrap_or(false);
        if primary && verified {
            if let Some(email) = entry.get("email").and_then(|v| v.as_str()) {
                return Ok((email.to_string(), display_name));
            }
        }
    }

    // Fallback: any verified email
    for entry in &emails {
        let verified = entry.get("verified").and_then(|v| v.as_bool()).unwrap_or(false);
        if verified {
            if let Some(email) = entry.get("email").and_then(|v| v.as_str()) {
                return Ok((email.to_string(), display_name));
            }
        }
    }

    Err("No verified email found on GitHub account".to_string())
}

// ============================================================================
// POST /api/auth/email-code/send — request a login code via email
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct SendEmailCodeRequest {
    pub email: String,
}

pub async fn send_email_code_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<SendEmailCodeRequest>,
) -> Response {
    let smtp = match state.smtp_config.as_ref() {
        Some(s) => s,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "Email code login is not configured"})),
            )
                .into_response()
        }
    };

    let access_db = match state.access_db.as_ref() {
        Some(db) => db,
        None => {
            // Still return success to prevent enumeration
            return (StatusCode::OK, Json(json!({"success": true}))).into_response()
        }
    };

    let email = body.email.trim().to_lowercase();

    // Anti-enumeration: always return success regardless of what happens
    let success_response = || (StatusCode::OK, Json(json!({"success": true}))).into_response();

    // Check user exists (silently skip if not)
    if !access_db.user_exists(&email) {
        return success_response();
    }

    // Rate limit: max 5 codes per email per hour
    if access_db.count_recent_email_codes(&email) >= 5 {
        return success_response();
    }

    // Generate 6-digit code (zero-padded) using UUID v4's secure RNG
    let code = {
        let bytes = uuid::Uuid::new_v4().into_bytes();
        let n = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) % 1_000_000;
        format!("{:06}", n)
    };

    // Hash the code with SHA-256
    use sha2::{Sha256, Digest};
    let code_hash = hex::encode(Sha256::digest(code.as_bytes()));

    // Store in DB
    if let Err(e) = access_db.store_email_code(&email, &code_hash) {
        tracing::error!("Failed to store email code: {}", e);
        return success_response();
    }

    // Send via SMTP
    if let Err(e) = smtp.send_code(&email, &code).await {
        tracing::error!("Failed to send email code to {}: {}", email, e);
    }

    success_response()
}

// ============================================================================
// POST /api/auth/email-code/verify — verify a login code
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct VerifyEmailCodeRequest {
    pub email: String,
    pub code: String,
}

pub async fn verify_email_code_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<VerifyEmailCodeRequest>,
) -> Response {
    if state.smtp_config.is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "Email code login is not configured"})),
        )
            .into_response();
    }

    let access_db = match state.access_db.as_ref() {
        Some(db) => db,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "Access control not initialized"})),
            )
                .into_response()
        }
    };

    let email = body.email.trim().to_lowercase();
    let code = body.code.trim().to_string();

    // Hash the submitted code
    use sha2::{Sha256, Digest};
    let code_hash = hex::encode(Sha256::digest(code.as_bytes()));

    if !access_db.verify_email_code(&email, &code_hash) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Invalid or expired code"})),
        )
            .into_response();
    }

    // Extract client info
    let ip = headers
        .get("x-forwarded-for")
        .or_else(|| headers.get("x-real-ip"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let user_agent = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Create session (24h)
    let token = match access_db.create_session(
        &email,
        ip.as_deref(),
        user_agent.as_deref(),
        24,
    ) {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Failed to create session: {}", e)})),
            )
                .into_response()
        }
    };

    let is_admin = access_db.is_admin(&email);

    let cookie = format!(
        "session={}; HttpOnly; SameSite=Strict; Path=/; Max-Age=86400",
        token
    );

    (
        StatusCode::OK,
        [("set-cookie", cookie.as_str())],
        Json(json!({
            "success": true,
            "session_token": token,
            "email": email,
            "is_admin": is_admin,
        })),
    )
        .into_response()
}
