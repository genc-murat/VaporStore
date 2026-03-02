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
    })
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
            if auth_str.contains(&format!("Credential={}/", config.access_key))
                || auth_str.contains(&format!("Bearer {}", config.secret_key))
            {
                return Ok(next.run(req).await);
            }
        }
    }

    warn!("Unauthorized request: invalid or missing authentication.");
    Err(StatusCode::UNAUTHORIZED)
}
