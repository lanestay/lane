#[cfg(feature = "mcp")]
pub mod tools;

#[cfg(feature = "mcp")]
mod inner {
    use std::collections::HashMap;
    use std::sync::Arc;

    use axum::body::Body;
    use axum::extract::{Path, State};
    use axum::http::HeaderMap;
    use axum::http::StatusCode;
    use axum::response::{IntoResponse, Response};
    use serde_json::json;
    use tokio::sync::RwLock;

    use rmcp::{
        ServerHandler,
        model::{ServerCapabilities, ServerInfo},
        tool_handler,
        transport::streamable_http_server::{
            StreamableHttpServerConfig, StreamableHttpService,
            session::local::LocalSessionManager,
        },
    };

    use crate::api::FileCache;
    use crate::api::approvals::ApprovalRegistry;
    use crate::auth::access_control::AccessControlDb;
    use crate::db::ConnectionRegistry;
    use super::tools::{BatchQueryMcp, UserContext};

    // ServerHandler implementation
    #[tool_handler]
    impl ServerHandler for BatchQueryMcp {
        fn get_info(&self) -> ServerInfo {
            ServerInfo {
                instructions: Some(
                    "lane MCP - Execute SQL queries, explore database schemas, and manage data. \
                     Tools: execute_sql_read (safe for auto-allow), execute_sql_write (requires confirmation), \
                     execute_sql_dry_run, list_tables, describe_table, list_databases, list_connections, get_api_help. \
                     Workspace tools (cross-database analytics): workspace_import_query, workspace_query, \
                     workspace_list_tables, workspace_clear, workspace_export_to_table. \
                     Storage tools (MinIO/S3): storage_list_buckets, storage_list_objects, storage_upload, \
                     storage_download_to_workspace, storage_get_url."
                        .into(),
                ),
                capabilities: ServerCapabilities::builder().enable_tools().build(),
                ..Default::default()
            }
        }
    }

    // ========================================================================
    // Per-token MCP auth
    // ========================================================================

    /// Shared state for the per-token MCP endpoint.
    pub struct McpTokenState {
        registry: Arc<ConnectionRegistry>,
        access_db: Arc<AccessControlDb>,
        approval_registry: Option<Arc<ApprovalRegistry>>,
        ct: tokio_util::sync::CancellationToken,
        downloads: FileCache,
        realtime_tx: tokio::sync::broadcast::Sender<crate::api::realtime::RealtimeEvent>,
        #[cfg(feature = "duckdb_backend")]
        workspace_db: Option<Arc<crate::db::duckdb_backend::DuckDbBackend>>,
        #[cfg(feature = "duckdb_backend")]
        workspace_dir: Option<std::path::PathBuf>,
        #[cfg(feature = "storage")]
        storage_registry: Option<Arc<crate::storage::StorageRegistry>>,
        search_db: Option<Arc<crate::search::db::SearchDb>>,
        graph_db: Option<Arc<crate::graph::GraphDb>>,
        services: RwLock<HashMap<String, StreamableHttpService<BatchQueryMcp>>>,
    }

    impl McpTokenState {
        pub fn new(
            registry: Arc<ConnectionRegistry>,
            access_db: Arc<AccessControlDb>,
            approval_registry: Option<Arc<ApprovalRegistry>>,
            ct: tokio_util::sync::CancellationToken,
            downloads: FileCache,
            realtime_tx: tokio::sync::broadcast::Sender<crate::api::realtime::RealtimeEvent>,
            #[cfg(feature = "duckdb_backend")] workspace_db: Option<Arc<crate::db::duckdb_backend::DuckDbBackend>>,
            #[cfg(feature = "duckdb_backend")] workspace_dir: Option<std::path::PathBuf>,
            #[cfg(feature = "storage")] storage_registry: Option<Arc<crate::storage::StorageRegistry>>,
            search_db: Option<Arc<crate::search::db::SearchDb>>,
            graph_db: Option<Arc<crate::graph::GraphDb>>,
        ) -> Self {
            Self {
                registry,
                access_db,
                approval_registry,
                ct,
                downloads,
                realtime_tx,
                #[cfg(feature = "duckdb_backend")]
                workspace_db,
                #[cfg(feature = "duckdb_backend")]
                workspace_dir,
                #[cfg(feature = "storage")]
                storage_registry,
                search_db,
                graph_db,
                services: RwLock::new(HashMap::new()),
            }
        }

        /// Get or create a per-token StreamableHttpService.
        fn create_service(
            &self,
            email: &str,
            token_prefix: &str,
            pii_mode: Option<String>,
        ) -> StreamableHttpService<BatchQueryMcp> {
            let registry = self.registry.clone();
            let access_db = self.access_db.clone();
            let approval_registry = self.approval_registry.clone();
            let downloads = self.downloads.clone();
            let realtime_tx = self.realtime_tx.clone();
            let email = email.to_string();
            let token_prefix = token_prefix.to_string();
            let ct = self.ct.clone();
            #[cfg(feature = "duckdb_backend")]
            let workspace_db = self.workspace_db.clone();
            #[cfg(feature = "duckdb_backend")]
            let workspace_dir = self.workspace_dir.clone();
            #[cfg(feature = "storage")]
            let storage_registry = self.storage_registry.clone();
            let search_db = self.search_db.clone();
            let graph_db = self.graph_db.clone();

            StreamableHttpService::new(
                move || {
                    let user_context = UserContext {
                        email: email.clone(),
                        token_prefix: token_prefix.clone(),
                        access_db: access_db.clone(),
                        pii_mode: pii_mode.clone(),
                    };
                    Ok(BatchQueryMcp::new(
                        registry.clone(),
                        Some(user_context),
                        approval_registry.clone(),
                        Some(downloads.clone()),
                        Some(realtime_tx.clone()),
                        #[cfg(feature = "duckdb_backend")]
                        workspace_db.clone(),
                        #[cfg(feature = "duckdb_backend")]
                        workspace_dir.clone(),
                        #[cfg(feature = "storage")]
                        storage_registry.clone(),
                        search_db.clone(),
                        graph_db.clone(),
                    ))
                },
                LocalSessionManager::default().into(),
                StreamableHttpServerConfig {
                    cancellation_token: ct.child_token(),
                    ..Default::default()
                },
            )
        }
    }

    /// Axum handler for `/mcp/token/{token}` and `/mcp/token/{token}/{*rest}`.
    pub async fn mcp_token_handler(
        Path(params): Path<HashMap<String, String>>,
        State(state): State<Arc<McpTokenState>>,
        req: axum::http::Request<Body>,
    ) -> Response {
        let token = match params.get("token") {
            Some(t) => t.clone(),
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    json!({"error": "Missing token"}).to_string(),
                )
                    .into_response();
            }
        };

        // Validate token on every request (catches mid-session revocation)
        let token_info = match state.access_db.validate_token(&token) {
            Ok(info) => info,
            Err(e) => {
                return (
                    StatusCode::UNAUTHORIZED,
                    json!({"error": e}).to_string(),
                )
                    .into_response();
            }
        };

        // Get or create per-token service
        // Always recreate if pii_mode may have changed (simple: just create on miss)
        let service = {
            let services = state.services.read().await;
            services.get(&token).cloned()
        };

        let service = match service {
            Some(s) => s,
            None => {
                let new_service = state.create_service(
                    &token_info.email,
                    &token_info.token_prefix,
                    token_info.pii_mode.clone(),
                );
                let mut services = state.services.write().await;
                // Double-check after acquiring write lock
                services
                    .entry(token.clone())
                    .or_insert(new_service)
                    .clone()
            }
        };

        // Rewrite URI to "/" — rmcp expects requests at the service root,
        // but Axum passes the full path (e.g. /mcp/token/{token}).
        let (mut parts, body) = req.into_parts();
        parts.uri = "/".parse().unwrap();
        let req = axum::http::Request::from_parts(parts, body);

        // Forward request to the MCP service
        service.handle(req).await.into_response()
    }

    /// Axum handler for `/mcp` with header-based auth (`x-lane-key`).
    ///
    /// Same as `mcp_token_handler` but reads the token from the request header
    /// instead of the URL path, keeping credentials out of server access logs.
    pub async fn mcp_header_auth_handler(
        headers: HeaderMap,
        State(state): State<Arc<McpTokenState>>,
        req: axum::http::Request<Body>,
    ) -> Response {
        let token = match headers
            .get("x-lane-key")
            .and_then(|v| v.to_str().ok())
        {
            Some(t) => t.to_string(),
            None => {
                return (
                    StatusCode::UNAUTHORIZED,
                    json!({"error": "Missing x-lane-key header"}).to_string(),
                )
                    .into_response();
            }
        };

        // Validate token on every request (catches mid-session revocation)
        let token_info = match state.access_db.validate_token(&token) {
            Ok(info) => info,
            Err(e) => {
                return (
                    StatusCode::UNAUTHORIZED,
                    json!({"error": e}).to_string(),
                )
                    .into_response();
            }
        };

        // Get or create per-token service
        let service = {
            let services = state.services.read().await;
            services.get(&token).cloned()
        };

        let service = match service {
            Some(s) => s,
            None => {
                let new_service = state.create_service(
                    &token_info.email,
                    &token_info.token_prefix,
                    token_info.pii_mode.clone(),
                );
                let mut services = state.services.write().await;
                services
                    .entry(token.clone())
                    .or_insert(new_service)
                    .clone()
            }
        };

        // Rewrite URI to "/" — rmcp expects requests at the service root
        let (mut parts, body) = req.into_parts();
        parts.uri = "/".parse().unwrap();
        let req = axum::http::Request::from_parts(parts, body);

        service.handle(req).await.into_response()
    }
}

#[cfg(feature = "mcp")]
pub use inner::{McpTokenState, mcp_header_auth_handler, mcp_token_handler};
