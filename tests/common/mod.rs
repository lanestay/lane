use reqwest::{Client, Response};
use serde_json::Value;
use std::env;

/// A test client that authenticates via session login.
#[allow(dead_code)]
pub struct SessionTestClient {
    client: Client,
    base_url: String,
    session_token: String,
}

#[allow(dead_code)]
impl SessionTestClient {
    pub async fn login(email: &str, password: &str) -> Self {
        let base_url =
            env::var("TEST_BASE_URL").unwrap_or_else(|_| "http://localhost:3401".to_string());
        let client = Client::new();
        let res = client
            .post(format!("{}/api/auth/login", base_url))
            .json(&serde_json::json!({"email": email, "password": password}))
            .send()
            .await
            .expect("login request failed");
        assert_eq!(res.status(), 200, "Login failed for {}", email);
        let body: Value = res.json().await.unwrap();
        let token = body["session_token"]
            .as_str()
            .or_else(|| body["token"].as_str())
            .expect("No token in login response")
            .to_string();
        Self {
            client,
            base_url,
            session_token: token,
        }
    }

    pub async fn login_admin() -> Self {
        let email = env::var("TEST_ADMIN_EMAIL").unwrap_or_else(|_| "admin@example.com".to_string());
        let password = env::var("TEST_ADMIN_PASSWORD").expect("TEST_ADMIN_PASSWORD must be set");
        Self::login(&email, &password).await
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    pub async fn get(&self, path: &str) -> Response {
        self.client
            .get(self.url(path))
            .header("Authorization", format!("Bearer {}", self.session_token))
            .send()
            .await
            .expect("request failed")
    }

    pub async fn get_query(&self, path: &str, query: &[(&str, &str)]) -> Response {
        self.client
            .get(self.url(path))
            .header("Authorization", format!("Bearer {}", self.session_token))
            .query(query)
            .send()
            .await
            .expect("request failed")
    }

    pub async fn get_no_auth(&self, path: &str) -> Response {
        self.client
            .get(self.url(path))
            .send()
            .await
            .expect("request failed")
    }

    pub async fn post_json(&self, path: &str, body: &Value) -> Response {
        self.client
            .post(self.url(path))
            .header("Authorization", format!("Bearer {}", self.session_token))
            .json(body)
            .send()
            .await
            .expect("request failed")
    }

    pub async fn put_json(&self, path: &str, body: &Value) -> Response {
        self.client
            .put(self.url(path))
            .header("Authorization", format!("Bearer {}", self.session_token))
            .json(body)
            .send()
            .await
            .expect("request failed")
    }

    pub async fn delete(&self, path: &str) -> Response {
        self.client
            .delete(self.url(path))
            .header("Authorization", format!("Bearer {}", self.session_token))
            .send()
            .await
            .expect("request failed")
    }

    pub async fn delete_json(&self, path: &str, body: &Value) -> Response {
        self.client
            .delete(self.url(path))
            .header("Authorization", format!("Bearer {}", self.session_token))
            .json(body)
            .send()
            .await
            .expect("request failed")
    }
}
