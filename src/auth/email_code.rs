use lettre::{
    message::header::ContentType, transport::smtp::authentication::Credentials, AsyncSmtpTransport,
    AsyncTransport, Message, Tokio1Executor,
};
use std::env;

#[derive(Debug, Clone)]
pub enum TlsMode {
    None,
    StartTls,
    Tls,
}

#[derive(Clone)]
pub struct SmtpConfig {
    pub from_address: String,
    transport: AsyncSmtpTransport<Tokio1Executor>,
}

impl SmtpConfig {
    /// Build from LANE_SMTP_* env vars. Returns None if SMTP_HOST is not set.
    pub fn from_env() -> Option<Self> {
        let host = env::var("LANE_SMTP_HOST").ok().filter(|s| !s.is_empty())?;
        let port: u16 = env::var("LANE_SMTP_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(587);
        let username = env::var("LANE_SMTP_USERNAME").ok().filter(|s| !s.is_empty());
        let password = env::var("LANE_SMTP_PASSWORD").ok().filter(|s| !s.is_empty());
        let from_address = env::var("LANE_SMTP_FROM")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "noreply@lane.local".to_string());
        let tls_mode = match env::var("LANE_SMTP_TLS")
            .unwrap_or_else(|_| "starttls".to_string())
            .to_lowercase()
            .as_str()
        {
            "none" => TlsMode::None,
            "tls" => TlsMode::Tls,
            _ => TlsMode::StartTls,
        };

        let mut builder = match tls_mode {
            TlsMode::None => AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&host),
            TlsMode::StartTls => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&host)
                .ok()?,
            TlsMode::Tls => AsyncSmtpTransport::<Tokio1Executor>::relay(&host).ok()?,
        };

        builder = builder.port(port);

        if let (Some(user), Some(pass)) = (username, password) {
            builder = builder.credentials(Credentials::new(user, pass));
        }

        let transport = builder.build();

        Some(Self {
            from_address,
            transport,
        })
    }

    /// Send a login code email.
    pub async fn send_code(&self, to_email: &str, code: &str) -> Result<(), String> {
        let email = Message::builder()
            .from(
                self.from_address
                    .parse()
                    .map_err(|e| format!("Invalid from address: {}", e))?,
            )
            .to(to_email
                .parse()
                .map_err(|e| format!("Invalid to address: {}", e))?)
            .subject("Your login code")
            .header(ContentType::TEXT_PLAIN)
            .body(format!(
                "Your login code is: {}\n\nThis code expires in 10 minutes.\n\nIf you did not request this, you can safely ignore this email.",
                code
            ))
            .map_err(|e| format!("Failed to build email: {}", e))?;

        self.transport
            .send(email)
            .await
            .map_err(|e| format!("SMTP send failed: {}", e))?;

        Ok(())
    }
}
