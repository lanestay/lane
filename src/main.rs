use anyhow::Result;
use std::{collections::{HashMap, HashSet}, env, path::PathBuf, sync::Arc};
use tokio::sync::RwLock;
use tracing_subscriber::{self, layer::SubscriberExt, util::SubscriberInitExt};

mod api;
mod auth;
mod config;
mod db;
mod export;
mod import;
mod mcp;
mod pii;
mod query;
mod rest;
mod search;
#[cfg(feature = "storage")]
mod storage;
use auth::access_control::{AccessControlDb, StoredConnection};
use config::{ConnectionConfig, NamedConnection};
use db::{ConnectionRegistry, ConnectionStatus};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,lane=debug".to_string().into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Initialize data directory and auth DB
    let data_dir = env::var("LANE_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./data"));

    std::fs::create_dir_all(&data_dir)
        .map_err(|e| anyhow::anyhow!("Failed to create data directory {:?}: {}", data_dir, e))?;

    let cipher_key_path = data_dir.join(".cipher_key");
    let cipher_key = if cipher_key_path.exists() {
        std::fs::read_to_string(&cipher_key_path)
            .map_err(|e| anyhow::anyhow!("Failed to read cipher key: {}", e))?
            .trim()
            .to_string()
    } else {
        // Generate a new cipher key
        let key = generate_random_hex(32);
        std::fs::write(&cipher_key_path, &key)
            .map_err(|e| anyhow::anyhow!("Failed to write cipher key: {}", e))?;
        // Set file permissions to 0600 (owner read/write only)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&cipher_key_path, std::fs::Permissions::from_mode(0o600))
                .map_err(|e| anyhow::anyhow!("Failed to set cipher key permissions: {}", e))?;
        }
        tracing::info!("Generated new cipher key at {:?}", cipher_key_path);
        key
    };

    let auth_db_path = data_dir.join("auth.db");
    let access_db = match AccessControlDb::new(
        auth_db_path.to_str().unwrap_or("data/auth.db"),
        &cipher_key,
    ) {
        Ok(db) => {
            tracing::info!("Auth database initialized at {:?}", auth_db_path);
            Arc::new(db)
        }
        Err(e) => {
            anyhow::bail!("Failed to initialize auth database: {}", e);
        }
    };

    // Seed connections from config into DB on first run, then load from DB
    let registry = build_registry_from_db(&access_db).await?;
    let registry = Arc::new(registry);

    // Generate system API key if not already stored
    let api_key = match access_db.get_config("system_api_key") {
        Ok(Some(key)) => key,
        _ => {
            let key = generate_random_hex(32);
            if let Err(e) = access_db.set_config("system_api_key", &key) {
                tracing::warn!("Failed to store system API key: {}", e);
            }
            tracing::info!("Generated new system API key");
            key
        }
    };

    // Check if setup is needed
    if access_db.needs_setup().unwrap_or(false) {
        tracing::info!("No users found. Visit the web UI to complete setup.");
    }

    // Initialize DuckDB workspace (optional, feature-gated)
    #[cfg(feature = "duckdb_backend")]
    let workspace_dir = data_dir.join("workspace");
    #[cfg(feature = "duckdb_backend")]
    let workspace_db = {
        let ws_dir = &workspace_dir;
        std::fs::create_dir_all(&ws_dir)
            .map_err(|e| anyhow::anyhow!("Failed to create workspace dir: {}", e))?;
        let ws_path = ws_dir.join("workspace.duckdb");
        let ws_config = config::DuckDbConnectionConfig {
            path: ws_path.to_string_lossy().to_string(),
            read_only: Some(false),
        };
        match db::duckdb_backend::DuckDbBackend::new(ws_config).await {
            Ok(backend) => {
                backend.execute_sql(
                    "CREATE TABLE IF NOT EXISTS __workspace_meta (
                        table_name VARCHAR PRIMARY KEY,
                        original_filename VARCHAR,
                        uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        row_count BIGINT,
                        column_count INTEGER
                    )"
                ).await?;
                tracing::info!("DuckDB workspace initialized at {:?}", ws_path);
                Some(Arc::new(backend))
            }
            Err(e) => {
                tracing::warn!("Failed to initialize DuckDB workspace: {}", e);
                None
            }
        }
    };

    // Build storage registry for MinIO/S3 connections
    #[cfg(feature = "storage")]
    let storage_registry = Arc::new(build_storage_registry_from_db(&access_db).await);

    // Initialize search database
    let search_db_path = data_dir.join("search.db");
    let search_db = match search::db::SearchDb::new(
        search_db_path.to_str().unwrap_or("data/search.db"),
        &cipher_key,
    ) {
        Ok(db) => {
            tracing::info!("Search database initialized at {:?}", search_db_path);
            Some(Arc::new(db))
        }
        Err(e) => {
            tracing::warn!("Failed to initialize search database: {}", e);
            None
        }
    };

    // Create file download cache
    let downloads: api::FileCache = Arc::new(RwLock::new(HashMap::new()));

    // Create realtime broadcast channel
    let (realtime_tx, _) = tokio::sync::broadcast::channel::<api::realtime::RealtimeEvent>(256);

    // Create approval registry for supervised mode
    let approval_registry = Arc::new(api::approvals::ApprovalRegistry::new());
    approval_registry.spawn_cleanup_task();

    // Parse auth providers from comma-separated env var
    let auth_providers: HashSet<auth::AuthProvider> = {
        let raw = env::var("LANE_AUTH").unwrap_or_else(|_| "email".to_string());
        let providers: HashSet<auth::AuthProvider> = raw
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| {
                auth::AuthProvider::from_str(s)
                    .unwrap_or_else(|| panic!("Unknown auth provider: '{}'. Valid: email, tailscale, google, microsoft, github", s))
            })
            .collect();
        if providers.is_empty() {
            panic!("LANE_AUTH must specify at least one provider");
        }
        providers
    };

    // Load OIDC configs for any OIDC providers
    let mut oidc_configs = HashMap::new();
    let base_url = env::var("LANE_BASE_URL").ok();
    let has_oidc = auth_providers.iter().any(|p| p.is_oidc());
    if has_oidc && base_url.is_none() {
        panic!("LANE_BASE_URL is required when OIDC providers are enabled");
    }
    if has_oidc {
        if let Some(ref url) = base_url {
            if !url.starts_with("https://") {
                tracing::warn!(
                    "LANE_BASE_URL uses HTTP, not HTTPS. OIDC tokens will be transmitted insecurely. \
                     Use HTTPS in production."
                );
            }
        }
    }
    for provider in &auth_providers {
        if !provider.is_oidc() {
            continue;
        }
        let prefix = provider.env_prefix();
        let client_id = env::var(format!("LANE_{}_CLIENT_ID", prefix))
            .unwrap_or_else(|_| panic!("LANE_{}_CLIENT_ID is required for {:?} auth", prefix, provider));
        let client_secret = env::var(format!("LANE_{}_CLIENT_SECRET", prefix))
            .unwrap_or_else(|_| panic!("LANE_{}_CLIENT_SECRET is required for {:?} auth", prefix, provider));
        let config = auth::OidcProviderConfig::for_provider(provider, client_id, client_secret)
            .expect("Failed to create OIDC config");
        oidc_configs.insert(provider.clone(), config);
    }

    if auth_providers.contains(&auth::AuthProvider::Tailscale) {
        tracing::info!("Tailscale authentication enabled");
        let other_providers: Vec<_> = auth_providers.iter().filter(|p| **p != auth::AuthProvider::Tailscale).collect();
        if !other_providers.is_empty() {
            tracing::info!(
                "Tailscale + {:?}: Tailscale auto-login will be tried first, other methods as fallback. \
                 For OIDC, set LANE_BASE_URL to your tailnet HTTPS address (e.g. https://machine.tailnet.ts.net).",
                other_providers
            );
        }
    }
    for p in &auth_providers {
        if p.is_oidc() {
            tracing::info!("{:?} OIDC authentication enabled", p);
        }
    }
    tracing::info!("Auth providers: {:?}", auth_providers);

    // Parse SMTP config for email code login
    let smtp_config = auth::email_code::SmtpConfig::from_env().map(|c| {
        tracing::info!("SMTP configured — email code login enabled");
        Arc::new(c)
    });

    // Create login rate limiter
    let login_rate_limiter = Arc::new(auth::session::LoginRateLimiter::new());

    // Create shared application state
    let state = Arc::new(api::AppState {
        api_key: Arc::new(RwLock::new(api_key)),
        registry: registry.clone(),
        access_db: Some(access_db.clone()),
        approval_registry: Some(approval_registry.clone()),
        downloads,
        realtime_tx: realtime_tx.clone(),
        #[cfg(feature = "duckdb_backend")]
        workspace_db,
        #[cfg(feature = "duckdb_backend")]
        workspace_dir: Some(workspace_dir),
        #[cfg(feature = "storage")]
        storage_registry,
        search_db: search_db.clone(),
        auth_providers: auth_providers.clone(),
        oidc_configs,
        base_url,
        smtp_config,
        login_rate_limiter: login_rate_limiter.clone(),
    });

    // Spawn background search indexer
    if let Some(ref sdb) = search_db {
        let search_clone = sdb.clone();
        let registry_clone = registry.clone();
        let access_db_clone = Some(access_db.clone());
        tokio::spawn(async move {
            // Small delay to let connections initialize
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            search::indexer::run_full_index(search_clone, registry_clone, access_db_clone).await;
        });
    }

    // Spawn background cleanup for expired download files
    api::spawn_cleanup_task(state.downloads.clone());

    // Spawn hourly session cleanup task
    {
        let db = access_db.clone();
        let rate_limiter = login_rate_limiter.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                match db.cleanup_expired_sessions() {
                    Ok(n) if n > 0 => tracing::debug!("Cleaned up {} expired sessions", n),
                    _ => {}
                }
                match db.cleanup_expired_oauth_states() {
                    Ok(n) if n > 0 => tracing::debug!("Cleaned up {} expired OAuth states", n),
                    _ => {}
                }
                match db.cleanup_expired_email_codes() {
                    Ok(n) if n > 0 => tracing::debug!("Cleaned up {} expired email codes", n),
                    _ => {}
                }
                rate_limiter.cleanup();
            }
        });
    }

    // Spawn health check task (every 30s)
    {
        let reg = registry.clone();
        let health_db = access_db.clone();
        tokio::spawn(async move {
            // Initial check after 5s
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            let mut check_count: u64 = 0;
            loop {
                let names = reg.connection_names();
                for name in &names {
                    if let Some(backend) = reg.get(name) {
                        match backend.health_check().await {
                            Ok(()) => {
                                reg.set_status(name, ConnectionStatus::Connected);
                                let _ = health_db.record_health_check(name, "connected", None);
                            }
                            Err(e) => {
                                let msg = format!("{:#}", e);
                                reg.set_status(name, ConnectionStatus::Error(msg.clone()));
                                let _ = health_db.record_health_check(name, "error", Some(&msg));
                            }
                        }
                    }
                }
                check_count += 1;
                // Prune old history every ~100 checks (~50 min)
                if check_count % 100 == 0 {
                    let _ = health_db.prune_health_history(48);
                }
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            }
        });
    }

    // HTTP server mode
    let port = env::var("PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(3401);

    let host = env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let bind_addr = format!("{}:{}", host, port);

    // In Docker, HOST=0.0.0.0 is required for container networking —
    // host-side exposure is controlled by docker-compose ports binding.
    let in_docker = std::path::Path::new("/.dockerenv").exists();
    if auth_providers.contains(&auth::AuthProvider::Tailscale) && host != "127.0.0.1" && !in_docker {
        tracing::warn!(
            "Tailscale auth enabled but listening on {}. Bind to 127.0.0.1 and use `tailscale serve` as the gateway.",
            bind_addr
        );
    }

    tracing::info!("Starting lane server on {}", bind_addr);

    run_http_server(state, registry, &bind_addr).await
}

/// Generate random hex string (n bytes = 2n hex chars).
fn generate_random_hex(num_bytes: usize) -> String {
    use std::io::Read;
    let mut buf = vec![0u8; num_bytes];
    let mut rng = std::fs::File::open("/dev/urandom").expect("Failed to open /dev/urandom");
    rng.read_exact(&mut buf).expect("Failed to read random bytes");
    hex::encode(buf)
}

/// Create a database backend from a NamedConnection.
pub(crate) async fn create_backend(
    conn: &NamedConnection,
) -> Result<Arc<dyn db::DatabaseBackend>> {
    match &conn.config {
        #[cfg(feature = "mssql")]
        ConnectionConfig::Mssql(cfg) => {
            let db_config = config::DbConfig::from(cfg.clone());
            let backend = db::mssql::MssqlBackend::new(db_config).await?;
            Ok(Arc::new(backend))
        }
        #[cfg(not(feature = "mssql"))]
        ConnectionConfig::Mssql(_) => {
            anyhow::bail!("MSSQL feature is not enabled");
        }
        #[cfg(feature = "postgres")]
        ConnectionConfig::Postgres(cfg) => {
            let backend = db::postgres::PostgresBackend::new(cfg.clone()).await?;
            Ok(Arc::new(backend))
        }
        #[cfg(not(feature = "postgres"))]
        ConnectionConfig::Postgres(_) => {
            anyhow::bail!("Postgres feature is not enabled");
        }
        #[cfg(feature = "duckdb_backend")]
        ConnectionConfig::DuckDb(cfg) => {
            let backend = db::duckdb_backend::DuckDbBackend::new(cfg.clone()).await?;
            Ok(Arc::new(backend))
        }
        #[cfg(feature = "storage")]
        ConnectionConfig::Minio(_) => {
            anyhow::bail!("MinIO/S3 connections are not database backends");
        }
        #[cfg(feature = "clickhouse_backend")]
        ConnectionConfig::ClickHouse(cfg) => {
            let backend = db::clickhouse_backend::ClickHouseBackend::new(cfg.clone()).await?;
            Ok(Arc::new(backend))
        }
        #[cfg(not(feature = "clickhouse_backend"))]
        ConnectionConfig::ClickHouse(_) => {
            anyhow::bail!("ClickHouse feature is not enabled. Compile with --features clickhouse_backend");
        }
    }
}

/// Build registry from DB. On first run, seeds connections from config file into DB.
async fn build_registry_from_db(access_db: &Arc<AccessControlDb>) -> Result<ConnectionRegistry> {
    // Seed from config file on first run
    if !access_db.connections_seeded() {
        let app_config = config::load_app_config()?;
        let default_name = app_config.default_connection.clone();

        if !app_config.connections.is_empty() {
            tracing::info!(
                "First run: seeding {} connection(s) from config into database",
                app_config.connections.len()
            );
            for nc in &app_config.connections {
                let mut stored = StoredConnection::from_named_connection(nc);
                if default_name.as_deref() == Some(&nc.name) {
                    stored.is_default = true;
                }
                if let Err(e) = access_db.create_connection(&stored) {
                    tracing::warn!("Failed to seed connection '{}': {}", nc.name, e);
                }
            }
        }
        if let Err(e) = access_db.mark_connections_seeded() {
            tracing::warn!("Failed to mark connections as seeded: {}", e);
        }
    }

    // Load all enabled connections from DB
    let stored = access_db
        .list_connections_db()
        .map_err(|e| anyhow::anyhow!("Failed to list connections from DB: {}", e))?;

    let default_name = stored
        .iter()
        .find(|c| c.is_default)
        .map(|c| c.name.clone())
        .or_else(|| stored.first().map(|c| c.name.clone()))
        .unwrap_or_else(|| "default".to_string());

    let registry = ConnectionRegistry::new(default_name);

    for sc in &stored {
        if !sc.is_enabled {
            tracing::info!("Skipping disabled connection '{}'", sc.name);
            continue;
        }
        if sc.conn_type == "minio" {
            tracing::info!("Skipping storage connection '{}' (not a database)", sc.name);
            continue;
        }
        let nc = sc.to_named_connection();
        tracing::info!("Initializing {} connection '{}'...", sc.conn_type, sc.name);
        match create_backend(&nc).await {
            Ok(backend) => {
                registry.register(sc.name.clone(), backend);
                tracing::info!("{} connection '{}' ready", sc.conn_type, sc.name);
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to initialize {} connection '{}': {:#}. Registering as error.",
                    sc.conn_type,
                    sc.name,
                    e
                );
                registry.set_status(&sc.name, ConnectionStatus::Error(format!("{:#}", e)));
            }
        }
    }

    Ok(registry)
}

/// Build storage registry from DB for MinIO/S3 connections.
#[cfg(feature = "storage")]
async fn build_storage_registry_from_db(
    access_db: &Arc<AccessControlDb>,
) -> storage::StorageRegistry {
    let registry = storage::StorageRegistry::new();

    let stored = match access_db.list_connections_db() {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("Failed to list connections for storage registry: {}", e);
            return registry;
        }
    };

    for sc in &stored {
        if sc.conn_type != "minio" || !sc.is_enabled {
            continue;
        }
        let nc = sc.to_named_connection();
        if let config::ConnectionConfig::Minio(ref cfg) = nc.config {
            match storage::StorageClient::new(cfg) {
                Ok(client) => {
                    registry
                        .register(sc.name.clone(), Arc::new(client))
                        .await;
                    tracing::info!("Storage connection '{}' registered", sc.name);
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to create storage client '{}': {:#}",
                        sc.name,
                        e
                    );
                }
            }
        }
    }

    registry
}

/// Start the HTTP server (REST API + MCP endpoints).
async fn run_http_server(
    state: Arc<api::AppState>,
    registry: Arc<ConnectionRegistry>,
    bind_addr: &str,
) -> Result<()> {
    // Build the REST API router
    let mut app = api::routes(state.clone());

    // Add MCP endpoints
    #[cfg(feature = "mcp")]
    {
        use axum::routing::any;

        let ct = tokio_util::sync::CancellationToken::new();

        // Per-token authenticated MCP endpoint (only when access control is configured)
        if let Some(ref access_db) = state.access_db {
            let mcp_token_state = Arc::new(mcp::McpTokenState::new(
                registry,
                access_db.clone(),
                state.approval_registry.clone(),
                ct,
                state.downloads.clone(),
                state.realtime_tx.clone(),
                #[cfg(feature = "duckdb_backend")]
                state.workspace_db.clone(),
                #[cfg(feature = "duckdb_backend")]
                state.workspace_dir.clone(),
                #[cfg(feature = "storage")]
                Some(state.storage_registry.clone()),
                state.search_db.clone(),
            ));
            app = app
                .route(
                    "/mcp",
                    any(mcp::mcp_header_auth_handler).with_state(mcp_token_state.clone()),
                )
                .route(
                    "/mcp/token/{token}",
                    any(mcp::mcp_token_handler).with_state(mcp_token_state.clone()),
                )
                .route(
                    "/mcp/token/{token}/{*rest}",
                    any(mcp::mcp_token_handler).with_state(mcp_token_state),
                );
            tracing::info!(
                "MCP endpoint available at http://{}/mcp (header auth) and http://{}/mcp/token/{{token}}",
                bind_addr, bind_addr
            );
        } else {
            tracing::warn!("No access control configured — MCP endpoint disabled. Set up auth to enable /mcp/token/{{token}}");
        }
    }

    // Start server
    let tcp_listener = tokio::net::TcpListener::bind(bind_addr).await?;
    tracing::info!("Server ready at http://{}", bind_addr);

    axum::serve(tcp_listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.unwrap();
            tracing::info!("Shutting down...");
        })
        .await?;

    Ok(())
}
