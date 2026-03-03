use axum::{
    extract::Request,
    http::{header::AUTHORIZATION, StatusCode},
    middleware::Next,
    response::Response,
};
use std::sync::OnceLock;
use tracing::warn;

struct AuthConfig {
    enabled: bool,
    access_key: String,
    secret_key: String,
    credential_prefix: OnceLock<String>,
}

impl AuthConfig {
    fn get_credential_prefix(&self) -> &str {
        self.credential_prefix
            .get_or_init(|| format!("Credential={}/", self.access_key))
    }
    
    fn get_bearer_token(&self) -> &str {
        self.secret_key.as_str()
    }
}

static AUTH_CONFIG: OnceLock<AuthConfig> = OnceLock::new();

fn get_auth_config() -> &'static AuthConfig {
    AUTH_CONFIG.get_or_init(|| AuthConfig {
        enabled: std::env::var("VAPORSTORE_AUTH")
            .unwrap_or_else(|_| "false".to_string())
            .to_lowercase()
            == "true",
        access_key: std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_default(),
        secret_key: std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default(),
        credential_prefix: OnceLock::new(),
    })
}

/// Constant-time string comparison to prevent timing attacks
pub fn test_constant_time_eq(a: &str, b: &str) -> bool {
    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();
    
    if a_bytes.len() != b_bytes.len() {
        return false;
    }
    
    let mut result = 0u8;
    for (x, y) in a_bytes.iter().zip(b_bytes.iter()) {
        result |= x ^ y;
    }
    result == 0
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    test_constant_time_eq(a, b)
}

pub async fn auth_middleware(req: Request, next: Next) -> Result<Response, StatusCode> {
    let config = get_auth_config();

    if !config.enabled {
        return Ok(next.run(req).await);
    }

    if config.access_key.is_empty() || config.secret_key.is_empty() {
        warn!("VAPORSTORE_AUTH is enabled but AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY is not set.");
        return Err(StatusCode::UNAUTHORIZED);
    }

    let auth_header = req.headers().get(AUTHORIZATION);
    if let Some(auth_value) = auth_header {
        if let Ok(auth_str) = auth_value.to_str() {
            // Check for AWS Signature V4
            if auth_str.contains(config.get_credential_prefix()) {
                return Ok(next.run(req).await);
            }
            
            // Check for Bearer token using constant-time comparison
            if let Some(token) = auth_str.strip_prefix("Bearer ") {
                if constant_time_eq(token, config.get_bearer_token()) {
                    return Ok(next.run(req).await);
                }
            }
        }
    }

    warn!("Unauthorized request: invalid or missing authentication.");
    Err(StatusCode::UNAUTHORIZED)
}
