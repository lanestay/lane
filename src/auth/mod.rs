pub mod access_control;
pub mod admin;
pub mod email_code;
pub mod session;

use axum::http::HeaderMap;

use crate::api::AppState;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthProvider {
    Email,
    Tailscale,
    Google,
    Microsoft,
    #[serde(rename = "github")]
    GitHub,
}

impl AuthProvider {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "email" => Some(Self::Email),
            "tailscale" => Some(Self::Tailscale),
            "google" => Some(Self::Google),
            "microsoft" => Some(Self::Microsoft),
            "github" => Some(Self::GitHub),
            _ => None,
        }
    }

    pub fn is_oidc(&self) -> bool {
        matches!(self, Self::Google | Self::Microsoft | Self::GitHub)
    }

    pub fn env_prefix(&self) -> &'static str {
        match self {
            Self::Google => "GOOGLE",
            Self::Microsoft => "MICROSOFT",
            Self::GitHub => "GITHUB",
            _ => "",
        }
    }
}

#[derive(Debug, Clone)]
pub struct OidcProviderConfig {
    pub client_id: String,
    pub client_secret: String,
    pub auth_url: String,
    pub token_url: String,
    pub scopes: Vec<String>,
}

impl OidcProviderConfig {
    pub fn for_provider(provider: &AuthProvider, client_id: String, client_secret: String) -> Option<Self> {
        match provider {
            AuthProvider::Google => Some(Self {
                client_id,
                client_secret,
                auth_url: "https://accounts.google.com/o/oauth2/v2/auth".to_string(),
                token_url: "https://oauth2.googleapis.com/token".to_string(),
                scopes: vec!["openid".to_string(), "email".to_string(), "profile".to_string()],
            }),
            AuthProvider::Microsoft => Some(Self {
                client_id,
                client_secret,
                auth_url: "https://login.microsoftonline.com/common/v2.0/authorize".to_string(),
                token_url: "https://login.microsoftonline.com/common/v2.0/token".to_string(),
                scopes: vec!["openid".to_string(), "email".to_string(), "profile".to_string()],
            }),
            AuthProvider::GitHub => Some(Self {
                client_id,
                client_secret,
                auth_url: "https://github.com/login/oauth/authorize".to_string(),
                token_url: "https://github.com/login/oauth/access_token".to_string(),
                scopes: vec!["user:email".to_string(), "read:user".to_string()],
            }),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum AuthResult {
    FullAccess,
    TokenAccess { email: String, pii_mode: Option<String> },
    SessionAccess { email: String, is_admin: bool },
    ServiceAccountAccess { account_name: String },
    Denied(String),
}

/// Extract Tailscale identity from headers set by `tailscale serve`.
pub fn extract_tailscale_identity(headers: &HeaderMap) -> Option<(String, String)> {
    let email = headers
        .get("tailscale-user-login")
        .and_then(|v| v.to_str().ok())?
        .to_string();
    let name = headers
        .get("tailscale-user-name")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    Some((email, name))
}

pub fn extract_api_key(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("x-api-key")
        .or_else(|| headers.get("x-lane-key"))
        .and_then(|v| v.to_str().ok())
}

/// Extract session token from Authorization header or cookie.
pub fn extract_session_token(headers: &HeaderMap) -> Option<String> {
    // Check Authorization: Bearer <token>
    if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        if let Some(token) = auth.strip_prefix("Bearer ") {
            return Some(token.to_string());
        }
    }

    // Check cookie: session=<token>
    if let Some(cookie) = headers.get("cookie").and_then(|v| v.to_str().ok()) {
        for part in cookie.split(';') {
            let part = part.trim();
            if let Some(token) = part.strip_prefix("session=") {
                return Some(token.to_string());
            }
        }
    }

    None
}

pub async fn authenticate(headers: &HeaderMap, state: &AppState) -> AuthResult {
    // 1. Check x-api-key header first
    if let Some(key) = extract_api_key(headers) {
        // Check system API key — full access
        if key == *state.api_key.read().await {
            return AuthResult::FullAccess;
        }

        // Check service account key
        if let Some(ref access_db) = state.access_db {
            if let Ok(sa) = access_db.validate_service_account_key(key) {
                return AuthResult::ServiceAccountAccess { account_name: sa.name };
            }
        }

        // Check per-user token in SQLite
        if let Some(ref access_db) = state.access_db {
            match access_db.validate_token(key) {
                Ok(info) => return AuthResult::TokenAccess { email: info.email, pii_mode: info.pii_mode },
                Err(_) => {} // fall through to session check
            }
        }
    }

    // 2. Check Tailscale identity headers (when enabled)
    if state.auth_providers.contains(&AuthProvider::Tailscale) {
        if let Some((email, _name)) = extract_tailscale_identity(headers) {
            if let Some(ref access_db) = state.access_db {
                // Reject users not pre-created by an admin
                if !access_db.user_exists(&email) {
                    tracing::warn!("Tailscale login denied for unknown user: {} — admin must create the user first", email);
                    return AuthResult::Denied("Account not found. Contact an administrator to be added.".to_string());
                }
                let is_admin = access_db.is_admin(&email);
                return AuthResult::SessionAccess { email, is_admin };
            }
        }
    }

    // 3. Check session token (Authorization: Bearer or cookie)
    if let Some(token) = extract_session_token(headers) {
        if let Some(ref access_db) = state.access_db {
            match access_db.validate_session(&token) {
                Ok(info) => {
                    return AuthResult::SessionAccess {
                        email: info.email,
                        is_admin: info.is_admin,
                    }
                }
                Err(_) => {} // invalid session, fall through to deny
            }
        }
    }

    AuthResult::Denied("Missing or invalid credentials".to_string())
}
