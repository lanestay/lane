use std::sync::Arc;

use crate::auth::access_control::AccessControlDb;
use crate::db::ConnectionRegistry;
use crate::search::db::SearchDb;

/// Full index of all schema objects across all connections.
pub async fn run_full_index(
    search_db: Arc<SearchDb>,
    registry: Arc<ConnectionRegistry>,
    access_db: Option<Arc<AccessControlDb>>,
) {
    tracing::info!("Search indexer: starting full index...");
    let mut schema_count: u64 = 0;

    let names = registry.connection_names();
    for conn_name in &names {
        let backend = match registry.get(conn_name) {
            Some(b) => b,
            None => continue,
        };

        search_db.clear_connection_schema(conn_name);

        // List databases
        let databases = match backend.list_databases().await {
            Ok(dbs) => dbs,
            Err(e) => {
                tracing::warn!("Search indexer: failed to list databases for '{}': {:#}", conn_name, e);
                continue;
            }
        };

        for db_row in &databases {
            let db_name = extract_string(db_row, &["name", "database_name", "DATABASE_NAME"])
                .unwrap_or_default();
            if db_name.is_empty() {
                continue;
            }

            // List schemas
            let schemas = match backend.list_schemas(&db_name).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::debug!("Search indexer: failed to list schemas for '{}.{}': {:#}", conn_name, db_name, e);
                    continue;
                }
            };

            for schema_row in &schemas {
                let schema_name = extract_string(schema_row, &["schema_name", "SCHEMA_NAME", "name"])
                    .unwrap_or_else(|| "dbo".to_string());

                // Index tables
                if let Ok(tables) = backend.list_tables(&db_name, &schema_name).await {
                    for table_row in &tables {
                        let table_name = extract_string(table_row, &["TABLE_NAME", "table_name", "name"])
                            .unwrap_or_default();
                        if table_name.is_empty() {
                            continue;
                        }

                        let columns = match backend.describe_table(&db_name, &table_name, &schema_name).await {
                            Ok(cols) => cols
                                .iter()
                                .filter_map(|c| extract_string(c, &["COLUMN_NAME", "column_name", "name"]))
                                .collect::<Vec<_>>(),
                            Err(_) => Vec::new(),
                        };

                        search_db.index_schema(
                            conn_name,
                            &db_name,
                            &schema_name,
                            &table_name,
                            "table",
                            &columns,
                        );
                        schema_count += 1;
                    }
                }

                // Index views
                if let Ok(views) = backend.list_views(&db_name, &schema_name).await {
                    for view_row in &views {
                        let view_name = extract_string(view_row, &["TABLE_NAME", "view_name", "name"])
                            .unwrap_or_default();
                        if view_name.is_empty() {
                            continue;
                        }

                        let columns = match backend.describe_table(&db_name, &view_name, &schema_name).await {
                            Ok(cols) => cols
                                .iter()
                                .filter_map(|c| extract_string(c, &["COLUMN_NAME", "column_name", "name"]))
                                .collect::<Vec<_>>(),
                            Err(_) => Vec::new(),
                        };

                        search_db.index_schema(
                            conn_name,
                            &db_name,
                            &schema_name,
                            &view_name,
                            "view",
                            &columns,
                        );
                        schema_count += 1;
                    }
                }
            }
        }
    }

    // Index existing endpoints from access_db
    let mut endpoint_count: u64 = 0;
    if let Some(ref adb) = access_db {
        search_db.clear_endpoints();
        if let Ok(endpoints) = adb.list_endpoints() {
            for ep in &endpoints {
                search_db.index_endpoint(
                    &ep.name,
                    &ep.connection_name,
                    &ep.database_name,
                    ep.description.as_deref().unwrap_or(""),
                    &ep.query,
                );
                endpoint_count += 1;
            }
        }
    }

    // Index existing query history from access_db
    let mut query_count: u64 = 0;
    if let Some(ref adb) = access_db {
        search_db.clear_queries();
        // Get all users and index their recent history
        if let Ok(users) = adb.list_users() {
            for user in &users {
                if let Ok(entries) = adb.list_query_history(&user.email, 200, 0, None, false) {
                    for entry in &entries {
                        search_db.index_query(
                            &entry.email,
                            entry.connection_name.as_deref().unwrap_or(""),
                            entry.database_name.as_deref().unwrap_or(""),
                            &entry.sql_text,
                        );
                        query_count += 1;
                    }
                }
            }
        }
    }

    tracing::info!(
        "Search indexer: complete — {} schema objects, {} queries, {} endpoints",
        schema_count,
        query_count,
        endpoint_count
    );
}

/// Extract a string value from a HashMap by trying multiple key names.
fn extract_string(
    row: &std::collections::HashMap<String, serde_json::Value>,
    keys: &[&str],
) -> Option<String> {
    for key in keys {
        if let Some(val) = row.get(*key) {
            if let Some(s) = val.as_str() {
                return Some(s.to_string());
            }
        }
    }
    None
}
