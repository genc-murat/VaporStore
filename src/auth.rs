use axum::{
    extract::Request,
    http::{header::AUTHORIZATION, StatusCode},
    middleware::Next,
    response::Response,
};
use tracing::warn;

pub async fn auth_middleware(req: Request, next: Next) -> Result<Response, StatusCode> {
    let auth_enabled = std::env::var("VAPORSTORE_AUTH")
        .unwrap_or_else(|_| "false".to_string())
        .to_lowercase() == "true";

    if !auth_enabled {
        return Ok(next.run(req).await);
    }

    let access_key = std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_default();
    let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default();

    if access_key.is_empty() || secret_key.is_empty() {
        warn!("VAPORSTORE_AUTH is enabled but AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY is not set.");
        return Err(StatusCode::UNAUTHORIZED);
    }

    let auth_header = req.headers().get(AUTHORIZATION);
    if let Some(auth_value) = auth_header {
        if let Ok(auth_str) = auth_value.to_str() {
            // Very naive SigV4 check for demonstration/mocking purposes.
            // A real implementation requires fully re-calculating the request hash.
            // Here we just ensure the supplied credential matches AWS_ACCESS_KEY_ID.
            if auth_str.contains(&format!("Credential={}/", access_key)) || auth_str.contains(&format!("Bearer {}", secret_key)) {
                return Ok(next.run(req).await);
            }
        }
    }

    warn!("Unauthorized request: invalid or missing authentication.");
    Err(StatusCode::UNAUTHORIZED)
}
