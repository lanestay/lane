use serde_json::{json, Value};
use crate::auth::access_control::AccessControlDb;
use crate::db::ConnectionRegistry;

/// Map SQL data types to OpenAPI types.
fn sql_type_to_openapi(sql_type: &str) -> Value {
    let t = sql_type.to_lowercase();
    if t.contains("int") {
        json!({"type": "integer"})
    } else if t.contains("decimal") || t.contains("numeric") || t.contains("float")
        || t.contains("real") || t.contains("double") || t.contains("money")
    {
        json!({"type": "number"})
    } else if t.contains("bool") || t.contains("bit") {
        json!({"type": "boolean"})
    } else if t.contains("date") && !t.contains("time") {
        json!({"type": "string", "format": "date"})
    } else if t.contains("datetime") || t.contains("timestamp") {
        json!({"type": "string", "format": "date-time"})
    } else if t.contains("time") {
        json!({"type": "string", "format": "time"})
    } else if t.contains("uuid") || t.contains("uniqueidentifier") {
        json!({"type": "string", "format": "uuid"})
    } else {
        json!({"type": "string"})
    }
}

/// Generate an OpenAPI 3.0 spec for all enabled REST tables.
pub async fn generate_openapi(
    registry: &ConnectionRegistry,
    access_db: &AccessControlDb,
) -> Value {
    let tables = match access_db.list_rest_tables() {
        Ok(t) => t,
        Err(_) => return empty_spec(),
    };

    let mut paths = serde_json::Map::new();
    let mut schemas = serde_json::Map::new();

    for table_entry in &tables {
        let db = match registry.resolve(Some(&table_entry.connection_name)) {
            Ok(db) => db,
            Err(_) => continue,
        };

        let columns = match db
            .describe_table(
                &table_entry.database_name,
                &table_entry.table_name,
                &table_entry.schema_name,
            )
            .await
        {
            Ok(c) => c,
            Err(_) => continue,
        };

        let base_path = format!(
            "/api/data/{}/{}/{}",
            table_entry.connection_name, table_entry.database_name, table_entry.table_name
        );

        // Build schema for this table
        let mut properties = serde_json::Map::new();
        let mut pk_col: Option<String> = None;

        for col in &columns {
            let col_name = col
                .get("COLUMN_NAME")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let data_type = col
                .get("DATA_TYPE")
                .and_then(|v| v.as_str())
                .unwrap_or("varchar");
            let is_nullable = col
                .get("IS_NULLABLE")
                .and_then(|v| v.as_str())
                .unwrap_or("YES");
            let is_pk = col
                .get("IS_PRIMARY_KEY")
                .and_then(|v| v.as_str())
                .unwrap_or("NO");

            let mut prop = sql_type_to_openapi(data_type);
            if is_nullable == "YES" {
                if let Some(obj) = prop.as_object_mut() {
                    obj.insert("nullable".to_string(), json!(true));
                }
            }
            properties.insert(col_name.to_string(), prop);

            if is_pk == "YES" && pk_col.is_none() {
                pk_col = Some(col_name.to_string());
            }
        }

        let schema_name = format!(
            "{}_{}_{}",
            table_entry.connection_name, table_entry.database_name, table_entry.table_name
        );
        schemas.insert(
            schema_name.clone(),
            json!({
                "type": "object",
                "properties": properties,
            }),
        );

        let schema_ref = format!("#/components/schemas/{}", schema_name);

        // List endpoint
        let list_path_item = json!({
            "get": {
                "summary": format!("List {} rows", table_entry.table_name),
                "tags": [format!("{}.{}", table_entry.connection_name, table_entry.database_name)],
                "parameters": [
                    {"name": "select", "in": "query", "schema": {"type": "string"}, "description": "Comma-separated column names"},
                    {"name": "order", "in": "query", "schema": {"type": "string"}, "description": "Order by columns (e.g. name.asc,age.desc)"},
                    {"name": "limit", "in": "query", "schema": {"type": "integer"}, "description": "Max rows to return"},
                    {"name": "offset", "in": "query", "schema": {"type": "integer"}, "description": "Rows to skip"},
                ],
                "responses": {
                    "200": {
                        "description": "List of rows",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "data": {"type": "array", "items": {"$ref": &schema_ref}},
                                        "total": {"type": "integer"},
                                        "limit": {"type": "integer"},
                                        "offset": {"type": "integer"},
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "post": {
                "summary": format!("Insert into {}", table_entry.table_name),
                "tags": [format!("{}.{}", table_entry.connection_name, table_entry.database_name)],
                "requestBody": {
                    "content": {
                        "application/json": {
                            "schema": {"$ref": &schema_ref}
                        }
                    }
                },
                "responses": {
                    "201": {
                        "description": "Created",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "data": {"$ref": &schema_ref}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
        paths.insert(base_path.clone(), list_path_item);

        // Single row endpoint (only if PK exists)
        if let Some(ref pk) = pk_col {
            let single_path = format!("{}/{{id}}", base_path);
            let single_path_item = json!({
                "get": {
                    "summary": format!("Get {} by {}", table_entry.table_name, pk),
                    "tags": [format!("{}.{}", table_entry.connection_name, table_entry.database_name)],
                    "parameters": [
                        {"name": "id", "in": "path", "required": true, "schema": {"type": "string"}, "description": format!("Primary key ({})", pk)}
                    ],
                    "responses": {
                        "200": {
                            "description": "Single row",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "data": {"$ref": &schema_ref}
                                        }
                                    }
                                }
                            }
                        },
                        "404": {"description": "Not found"}
                    }
                },
                "put": {
                    "summary": format!("Update {} by {}", table_entry.table_name, pk),
                    "tags": [format!("{}.{}", table_entry.connection_name, table_entry.database_name)],
                    "parameters": [
                        {"name": "id", "in": "path", "required": true, "schema": {"type": "string"}, "description": format!("Primary key ({})", pk)}
                    ],
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {"$ref": &schema_ref}
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Updated row",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "data": {"$ref": &schema_ref}
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "delete": {
                    "summary": format!("Delete {} by {}", table_entry.table_name, pk),
                    "tags": [format!("{}.{}", table_entry.connection_name, table_entry.database_name)],
                    "parameters": [
                        {"name": "id", "in": "path", "required": true, "schema": {"type": "string"}, "description": format!("Primary key ({})", pk)}
                    ],
                    "responses": {
                        "204": {"description": "Deleted"}
                    }
                }
            });
            paths.insert(single_path, single_path_item);
        }
    }

    json!({
        "openapi": "3.0.3",
        "info": {
            "title": "Lane Auto-Generated REST API",
            "version": "1.0.0",
            "description": "Auto-generated PostgREST-style endpoints for enabled database tables"
        },
        "paths": paths,
        "components": {
            "schemas": schemas,
            "securitySchemes": {
                "apiKey": {
                    "type": "apiKey",
                    "in": "header",
                    "name": "x-api-key"
                }
            }
        },
        "security": [{"apiKey": []}]
    })
}

fn empty_spec() -> Value {
    json!({
        "openapi": "3.0.3",
        "info": {
            "title": "Lane Auto-Generated REST API",
            "version": "1.0.0",
        },
        "paths": {},
        "components": {"schemas": {}}
    })
}
