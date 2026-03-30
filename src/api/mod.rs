pub mod approvals;
pub mod connections;
pub mod endpoints;
pub mod errors;
pub mod graph;
pub mod handlers;
pub mod history;
pub mod import;
pub mod monitor;
pub mod realtime;
pub mod rest_api;
pub mod search;
#[cfg(feature = "duckdb_backend")]
pub mod workspace;
#[cfg(feature = "storage")]
pub mod storage;

use axum::{
    extract::DefaultBodyLimit,
    routing::{delete, get, post, put},
    Router,
};
#[cfg(feature = "webui")]
use axum::{
    http::{header, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use std::collections::{HashMap, HashSet};
use std::env;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::auth::access_control::AccessControlDb;
use crate::db::ConnectionRegistry;
use crate::search::db::SearchDb;

pub struct CachedFile {
    pub bytes: Vec<u8>,
    pub content_type: String,
    pub filename: String,
    pub created_at: std::time::Instant,
}

pub type FileCache = Arc<RwLock<HashMap<String, CachedFile>>>;

#[derive(Clone)]
pub struct AppState {
    pub api_key: Arc<RwLock<String>>,
    pub registry: Arc<ConnectionRegistry>,
    pub access_db: Option<Arc<AccessControlDb>>,
    pub approval_registry: Option<Arc<approvals::ApprovalRegistry>>,
    pub downloads: FileCache,
    pub realtime_tx: tokio::sync::broadcast::Sender<realtime::RealtimeEvent>,
    #[cfg(feature = "duckdb_backend")]
    pub workspace_db: Option<Arc<crate::db::duckdb_backend::DuckDbBackend>>,
    #[cfg(feature = "duckdb_backend")]
    pub workspace_dir: Option<std::path::PathBuf>,
    #[cfg(feature = "storage")]
    pub storage_registry: Arc<crate::storage::StorageRegistry>,
    pub search_db: Option<Arc<SearchDb>>,
    pub graph_db: Option<Arc<crate::graph::GraphDb>>,
    pub auth_providers: HashSet<crate::auth::AuthProvider>,
    pub oidc_configs: HashMap<crate::auth::AuthProvider, crate::auth::OidcProviderConfig>,
    pub base_url: Option<String>,
    pub smtp_config: Option<Arc<crate::auth::email_code::SmtpConfig>>,
    pub login_rate_limiter: Arc<crate::auth::session::LoginRateLimiter>,
}

/// Build the full REST API router with all routes
pub fn routes(state: Arc<AppState>) -> Router {
    let body_limit = env::var("LANE_BODY_LIMIT_MB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(10);

    let mut app = Router::new()
        // Core endpoints
        .route("/health", get(handlers::health_check))
        .route("/api/lane/help", get(handlers::help_handler))
        .route("/api/lane", post(handlers::query_handler))
        .route(
            "/api/lane/ai",
            post(handlers::query_ai_handler),
        )
        // Admin endpoints
        .route(
            "/api/lane/admin/check-user",
            get(crate::auth::admin::check_user_handler),
        )
        .route(
            "/api/lane/admin/tokens/generate",
            post(crate::auth::admin::generate_token_handler),
        )
        .route(
            "/api/lane/admin/tokens/revoke",
            post(crate::auth::admin::revoke_token_handler),
        )
        .route(
            "/api/lane/admin/tokens",
            get(crate::auth::admin::list_tokens_handler),
        )
        .route(
            "/api/lane/admin/users",
            post(crate::auth::admin::create_user_handler)
                .get(crate::auth::admin::list_users_handler),
        )
        .route(
            "/api/lane/admin/users/{email}",
            put(crate::auth::admin::update_user_handler)
                .delete(crate::auth::admin::delete_user_handler),
        )
        .route(
            "/api/lane/admin/permissions",
            post(crate::auth::admin::set_permissions_handler),
        )
        .route(
            "/api/lane/admin/audit",
            get(crate::auth::admin::audit_log_handler),
        )
        .route(
            "/api/lane/admin/inventory",
            get(crate::auth::admin::inventory_handler),
        )
        // Connection management endpoints (static routes first)
        .route(
            "/api/lane/admin/connections/test",
            post(connections::test_inline_connection_handler),
        )
        .route(
            "/api/lane/admin/connections",
            get(connections::list_admin_connections_handler)
                .post(connections::create_connection_handler),
        )
        .route(
            "/api/lane/admin/connections/{name}",
            put(connections::update_connection_handler)
                .delete(connections::delete_connection_handler),
        )
        .route(
            "/api/lane/admin/connections/{name}/test",
            post(connections::test_existing_connection_handler),
        )
        .route(
            "/api/lane/connections/status",
            get(connections::connections_status_handler),
        )
        .route(
            "/api/lane/connections/health",
            get(connections::connections_health_handler),
        )
        // Self-service token endpoints
        .route(
            "/api/lane/tokens",
            post(crate::auth::admin::self_generate_token_handler)
                .get(crate::auth::admin::self_list_tokens_handler),
        )
        .route(
            "/api/lane/tokens/{prefix}",
            delete(crate::auth::admin::self_revoke_token_handler),
        )
        // Token policy settings
        .route(
            "/api/lane/admin/settings/token-policy",
            get(crate::auth::admin::get_token_policy_handler)
                .put(crate::auth::admin::set_token_policy_handler),
        )
        // API key rotation
        .route(
            "/api/lane/admin/settings/rotate-api-key",
            post(crate::auth::admin::rotate_api_key_handler),
        )
        // Connection-level access control
        .route(
            "/api/lane/admin/connection-permissions",
            get(crate::auth::admin::get_connection_permissions_handler)
                .post(crate::auth::admin::set_connection_permissions_handler),
        )
        // Storage permissions
        .route(
            "/api/lane/admin/storage-permissions",
            get(crate::auth::admin::get_storage_permissions_handler)
                .post(crate::auth::admin::set_storage_permissions_handler),
        )
        .route(
            "/api/lane/admin/sa-storage-permissions",
            get(crate::auth::admin::get_sa_storage_permissions_handler)
                .post(crate::auth::admin::set_sa_storage_permissions_handler),
        )
        // Service accounts
        .route(
            "/api/lane/admin/service-accounts",
            get(crate::auth::admin::list_service_accounts_handler)
                .post(crate::auth::admin::create_service_account_handler),
        )
        .route(
            "/api/lane/admin/service-accounts/{name}",
            put(crate::auth::admin::update_service_account_handler)
                .delete(crate::auth::admin::delete_service_account_handler),
        )
        .route(
            "/api/lane/admin/service-accounts/{name}/rotate-key",
            post(crate::auth::admin::rotate_sa_key_handler),
        )
        .route(
            "/api/lane/admin/service-account-permissions",
            get(crate::auth::admin::get_sa_permissions_handler)
                .post(crate::auth::admin::set_sa_permissions_handler),
        )
        .route(
            "/api/lane/admin/service-account-connections",
            get(crate::auth::admin::get_sa_connections_handler)
                .post(crate::auth::admin::set_sa_connections_handler),
        )
        .route(
            "/api/lane/admin/service-account-endpoints",
            get(crate::auth::admin::get_sa_endpoint_permissions_handler)
                .post(crate::auth::admin::set_sa_endpoint_permissions_handler),
        )
        // PII Rules (static routes before parameterized)
        .route(
            "/api/lane/admin/pii/rules/test",
            post(crate::auth::admin::test_pii_rule_handler),
        )
        .route(
            "/api/lane/admin/pii/rules",
            get(crate::auth::admin::list_pii_rules_handler)
                .post(crate::auth::admin::create_pii_rule_handler),
        )
        .route(
            "/api/lane/admin/pii/rules/{id}",
            put(crate::auth::admin::update_pii_rule_handler)
                .delete(crate::auth::admin::delete_pii_rule_handler),
        )
        // PII Columns (static routes before parameterized)
        .route(
            "/api/lane/admin/pii/columns/discover",
            post(crate::auth::admin::discover_pii_columns_handler),
        )
        .route(
            "/api/lane/admin/pii/columns",
            get(crate::auth::admin::list_pii_columns_handler)
                .post(crate::auth::admin::set_pii_column_handler),
        )
        .route(
            "/api/lane/admin/pii/columns/{id}",
            delete(crate::auth::admin::remove_pii_column_handler),
        )
        // PII Settings
        .route(
            "/api/lane/admin/pii/settings",
            get(crate::auth::admin::get_pii_settings_handler)
                .put(crate::auth::admin::set_pii_settings_handler),
        )
        // Auth endpoints (session-based)
        .route(
            "/api/auth/status",
            get(crate::auth::session::auth_status_handler),
        )
        .route(
            "/api/auth/setup",
            post(crate::auth::session::setup_handler),
        )
        .route(
            "/api/auth/login",
            post(crate::auth::session::login_handler),
        )
        .route(
            "/api/auth/logout",
            post(crate::auth::session::logout_handler),
        )
        .route(
            "/api/auth/tailscale",
            post(crate::auth::session::tailscale_login_handler),
        )
        .route(
            "/api/auth/oidc/{provider}/authorize",
            get(crate::auth::session::oidc_authorize_handler),
        )
        .route(
            "/api/auth/oidc/{provider}/callback",
            get(crate::auth::session::oidc_callback_handler),
        )
        .route(
            "/api/auth/email-code/send",
            post(crate::auth::session::send_email_code_handler),
        )
        .route(
            "/api/auth/email-code/verify",
            post(crate::auth::session::verify_email_code_handler),
        )
        .route(
            "/api/auth/password",
            post(crate::auth::session::change_password_handler),
        )
        // Admin password reset
        .route(
            "/api/lane/admin/users/{email}/password",
            post(crate::auth::admin::admin_set_password_handler),
        )
        .route(
            "/api/lane/admin/users/{email}/sessions",
            delete(crate::auth::admin::purge_user_sessions_handler),
        )
        .route(
            "/api/lane/download/{id}",
            get(handlers::download_handler),
        )
        // Query history endpoints
        .route(
            "/api/lane/history",
            get(history::list_history_handler),
        )
        .route(
            "/api/lane/history/{id}/favorite",
            post(history::toggle_favorite_handler),
        )
        .route(
            "/api/lane/history/{id}",
            delete(history::delete_history_handler),
        )
        // Metadata endpoints (mirror MCP tools for web UI)
        .route(
            "/api/lane/connections",
            get(handlers::list_connections_handler),
        )
        .route(
            "/api/lane/databases",
            get(handlers::list_databases_handler),
        )
        .route(
            "/api/lane/schemas",
            get(handlers::list_schemas_handler),
        )
        .route(
            "/api/lane/tables",
            get(handlers::list_tables_handler),
        )
        .route(
            "/api/lane/describe",
            get(handlers::describe_table_handler),
        )
        .route(
            "/api/lane/views",
            get(handlers::list_views_handler),
        )
        .route(
            "/api/lane/routines",
            get(handlers::list_routines_handler),
        )
        .route(
            "/api/lane/object-definition",
            get(handlers::get_object_definition_handler),
        )
        .route(
            "/api/lane/triggers",
            get(handlers::list_triggers_handler),
        )
        .route(
            "/api/lane/trigger-definition",
            get(handlers::get_trigger_definition_handler),
        )
        .route(
            "/api/lane/related-objects",
            get(handlers::get_related_objects_handler),
        )
        .route(
            "/api/lane/rls-policies",
            get(handlers::list_rls_policies_handler),
        )
        .route(
            "/api/lane/rls-status",
            get(handlers::get_rls_status_handler),
        )
        .route(
            "/api/lane/rls-generate",
            post(handlers::generate_rls_sql_handler),
        )
        // Realtime SSE endpoints
        .route(
            "/api/lane/admin/realtime/enable",
            post(realtime::enable_realtime_handler),
        )
        .route(
            "/api/lane/admin/realtime/disable",
            post(realtime::disable_realtime_handler),
        )
        .route(
            "/api/lane/admin/realtime/tables",
            get(realtime::list_realtime_tables_handler),
        )
        .route(
            "/api/lane/admin/realtime/webhooks",
            post(realtime::create_webhook_handler)
                .get(realtime::list_webhooks_handler),
        )
        .route(
            "/api/lane/admin/realtime/webhooks/{id}",
            put(realtime::update_webhook_handler)
                .delete(realtime::delete_webhook_handler),
        )
        .route(
            "/api/lane/realtime/subscribe",
            get(realtime::subscribe_handler),
        )
        // Monitor endpoints
        .route(
            "/api/lane/monitor/queries",
            get(monitor::list_queries_handler),
        )
        .route(
            "/api/lane/monitor/kill",
            post(monitor::kill_query_handler),
        )
        // Teams & Projects (admin)
        .route(
            "/api/lane/admin/teams",
            get(crate::auth::admin::list_teams_handler)
                .post(crate::auth::admin::create_team_handler),
        )
        .route(
            "/api/lane/admin/teams/{id}",
            put(crate::auth::admin::update_team_handler)
                .delete(crate::auth::admin::delete_team_handler),
        )
        .route(
            "/api/lane/admin/teams/{id}/members",
            get(crate::auth::admin::list_team_members_handler)
                .post(crate::auth::admin::add_team_member_handler),
        )
        .route(
            "/api/lane/admin/teams/{team_id}/members/{email}",
            put(crate::auth::admin::set_team_member_role_handler)
                .delete(crate::auth::admin::remove_team_member_handler),
        )
        .route(
            "/api/lane/admin/teams/{id}/projects",
            get(crate::auth::admin::list_projects_handler)
                .post(crate::auth::admin::create_project_handler),
        )
        .route(
            "/api/lane/admin/projects/{id}",
            put(crate::auth::admin::update_project_handler)
                .delete(crate::auth::admin::delete_project_handler),
        )
        .route(
            "/api/lane/admin/projects/{id}/members",
            get(crate::auth::admin::list_project_members_handler)
                .post(crate::auth::admin::add_project_member_handler),
        )
        .route(
            "/api/lane/admin/projects/{project_id}/members/{email}",
            put(crate::auth::admin::set_project_member_role_handler)
                .delete(crate::auth::admin::remove_project_member_handler),
        )
        // Approval endpoints (static routes before parameterized)
        .route(
            "/api/lane/approvals",
            get(approvals::list_approvals_handler),
        )
        .route(
            "/api/lane/approvals/events",
            get(approvals::approval_events_handler),
        )
        .route(
            "/api/lane/approvals/{id}",
            get(approvals::get_approval_handler),
        )
        .route(
            "/api/lane/approvals/{id}/approve",
            post(approvals::approve_handler),
        )
        .route(
            "/api/lane/approvals/{id}/reject",
            post(approvals::reject_handler),
        );

    // Search endpoints
    app = app
        .route(
            "/api/lane/search",
            get(search::unified_search),
        )
        .route(
            "/api/lane/search/schema",
            get(search::search_schema),
        )
        .route(
            "/api/lane/search/queries",
            get(search::search_queries),
        )
        .route(
            "/api/lane/search/endpoints",
            get(search::search_endpoints),
        )
        .route(
            "/api/lane/admin/search/reindex",
            post(search::admin_reindex),
        )
        .route(
            "/api/lane/admin/search/stats",
            get(search::admin_stats),
        );

    // Graph metadata endpoints
    app = app
        .route(
            "/api/lane/admin/graph/nodes",
            get(graph::list_nodes_handler).post(graph::create_node_handler),
        )
        .route(
            "/api/lane/admin/graph/nodes/{id}",
            delete(graph::delete_node_handler),
        )
        .route(
            "/api/lane/admin/graph/edges",
            get(graph::list_edges_handler).post(graph::create_edge_handler),
        )
        .route(
            "/api/lane/admin/graph/edges/{id}",
            delete(graph::delete_edge_handler),
        )
        .route(
            "/api/lane/admin/graph/seed",
            post(graph::seed_handler),
        )
        .route(
            "/api/lane/graph/traverse",
            post(graph::traverse_handler),
        )
        .route(
            "/api/lane/graph/plan",
            post(graph::plan_handler),
        );

    // Named data endpoints (before REST data API nest to take priority)
    app = app
        .route(
            "/api/data/endpoints",
            get(endpoints::list_data_endpoints_handler),
        )
        .route(
            "/api/data/endpoints/{name}",
            get(endpoints::execute_data_endpoint_handler),
        )
        // Admin endpoint management (static routes before parameterized)
        .route(
            "/api/lane/admin/endpoints",
            get(endpoints::list_endpoints_handler)
                .post(endpoints::create_endpoint_handler),
        )
        .route(
            "/api/lane/admin/endpoints/{name}",
            put(endpoints::update_endpoint_handler)
                .delete(endpoints::delete_endpoint_handler),
        )
        .route(
            "/api/lane/admin/endpoints/{name}/permissions",
            get(endpoints::get_endpoint_permissions_handler)
                .put(endpoints::set_endpoint_permissions_handler),
        );

    // REST data API (PostgREST-style auto-generated endpoints)
    app = app.nest("/api/data", rest_api::rest_routes(state.clone()));

    // Import endpoints (50MB limit per route)
    {
        use axum::routing::post as post_route;
        let import_limit = DefaultBodyLimit::max(50 * 1024 * 1024);
        app = app
            .route(
                "/api/lane/import/preview",
                post_route(import::preview_handler).layer(import_limit.clone()),
            )
            .route(
                "/api/lane/import/execute",
                post_route(import::execute_handler).layer(import_limit),
            );
    }

    // Workspace endpoints (feature-gated, 100MB limit)
    #[cfg(feature = "duckdb_backend")]
    {
        let ws_limit = DefaultBodyLimit::max(100 * 1024 * 1024);
        app = app
            .route("/api/lane/workspace/upload", post(workspace::upload_handler).layer(ws_limit))
            .route("/api/lane/workspace/import-query", post(workspace::import_query_handler))
            .route("/api/lane/workspace/tables", get(workspace::list_tables_handler))
            .route("/api/lane/workspace/tables/{name}", delete(workspace::delete_table_handler))
            .route("/api/lane/workspace/clear", post(workspace::clear_handler))
            .route("/api/lane/workspace/query", post(workspace::query_handler))
            .route("/api/lane/workspace/files", get(workspace::list_files_handler));
    }

    // Storage endpoints (feature-gated, 100MB upload limit)
    #[cfg(feature = "storage")]
    {
        let storage_limit = DefaultBodyLimit::max(100 * 1024 * 1024);
        app = app
            .route("/api/lane/storage/connections", get(storage::list_storage_connections_handler))
            .route("/api/lane/storage/buckets", get(storage::list_buckets_handler).post(storage::create_bucket_handler))
            .route("/api/lane/storage/buckets/{name}", delete(storage::delete_bucket_handler))
            .route("/api/lane/storage/objects", get(storage::list_objects_handler).delete(storage::delete_object_handler))
            .route("/api/lane/storage/upload", post(storage::upload_object_handler).layer(storage_limit))
            .route("/api/lane/storage/download", get(storage::download_object_handler))
            .route("/api/lane/storage/metadata", get(storage::object_metadata_handler))
            .route("/api/lane/storage/preview", post(storage::preview_handler))
            .route("/api/lane/storage/export-query", post(storage::export_query_to_storage_handler))
            .route("/api/lane/storage/import-to-workspace", post(storage::import_to_workspace_handler))
            .route("/api/lane/storage/workspace-export", post(storage::workspace_export_to_storage_handler))
            .route(
                "/api/lane/admin/storage/column-links",
                get(crate::auth::admin::list_storage_column_links_handler)
                    .post(crate::auth::admin::set_storage_column_link_handler),
            )
            .route(
                "/api/lane/admin/storage/column-links/{id}",
                delete(crate::auth::admin::remove_storage_column_link_handler),
            )
            .route(
                "/api/lane/storage/column-links",
                get(crate::auth::admin::list_storage_column_links_public_handler),
            );
    }

    // Embedded Web UI (feature-gated)
    #[cfg(feature = "webui")]
    {
        app = app
            .route("/ui", get(ui_handler))
            .route("/ui/{*rest}", get(ui_handler))
            .route("/assets/{*rest}", get(ui_assets_handler))
            .fallback(get(ui_handler));
    }

    // CORS: permissive in dev mode (LANE_CORS=permissive), disabled in production
    let cors_mode = env::var("LANE_CORS").unwrap_or_default();
    if cors_mode == "permissive" {
        app = app.layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );
    }

    app.layer(DefaultBodyLimit::max(body_limit * 1024 * 1024))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

// ============================================================================
// Embedded Web UI (feature-gated)
// ============================================================================

#[cfg(feature = "webui")]
#[derive(rust_embed::Embed)]
#[folder = "ui/dist/"]
struct UiAssets;

#[cfg(feature = "webui")]
async fn ui_handler(uri: Uri) -> Response {
    let raw = uri.path();
    // Redirect /ui and /ui/ to / so the client-side router can match "/"
    if raw == "/ui" || raw == "/ui/" {
        return (
            StatusCode::TEMPORARY_REDIRECT,
            [(header::LOCATION, "/".to_string())],
            "",
        )
            .into_response();
    }
    let path = raw.trim_start_matches("/ui").trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    serve_embedded(path)
}

/// Serve /assets/* requests directly (Vite builds use absolute asset paths).
#[cfg(feature = "webui")]
async fn ui_assets_handler(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    serve_embedded(path)
}

#[cfg(feature = "webui")]
fn serve_embedded(path: &str) -> Response {
    match UiAssets::get(path) {
        Some(file) => {
            let mime = mime_guess::from_path(path)
                .first_or_octet_stream()
                .to_string();
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, mime)],
                file.data.to_vec(),
            )
                .into_response()
        }
        None => {
            // SPA fallback: return index.html for client-side routing
            match UiAssets::get("index.html") {
                Some(file) => (
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, "text/html".to_string())],
                    file.data.to_vec(),
                )
                    .into_response(),
                None => (StatusCode::NOT_FOUND, "UI not found").into_response(),
            }
        }
    }
}

/// Spawn a background task that evicts cached download files older than 5 minutes.
pub fn spawn_cleanup_task(cache: FileCache) {
    tokio::spawn(async move {
        let ttl = std::time::Duration::from_secs(300);
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            let mut map = cache.write().await;
            map.retain(|_, file| file.created_at.elapsed() < ttl);
        }
    });
}
