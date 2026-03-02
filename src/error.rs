use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

use crate::xml::{to_xml, S3Error};

#[derive(Debug)]
pub enum ApiError {
    NoSuchBucket(String),
    NoSuchKey(String),
    BucketAlreadyExists(String),
    EntityTooLarge,
    BucketNotEmpty(String),
    Internal(String),
}

impl ApiError {
    fn code(&self) -> &str {
        match self {
            Self::NoSuchBucket(_) => "NoSuchBucket",
            Self::NoSuchKey(_) => "NoSuchKey",
            Self::BucketAlreadyExists(_) => "BucketAlreadyExists",
            Self::EntityTooLarge => "EntityTooLarge",
            Self::BucketNotEmpty(_) => "BucketNotEmpty",
            Self::Internal(_) => "InternalError",
        }
    }

    fn message(&self) -> String {
        match self {
            Self::NoSuchBucket(b) => format!("The specified bucket '{}' does not exist.", b),
            Self::NoSuchKey(k) => format!("The specified key '{}' does not exist.", k),
            Self::BucketAlreadyExists(b) => format!("The bucket '{}' already exists.", b),
            Self::EntityTooLarge => "Your proposed upload exceeds the maximum allowed object size of 5MB.".to_string(),
            Self::BucketNotEmpty(b) => format!("The bucket '{}' is not empty.", b),
            Self::Internal(m) => m.clone(),
        }
    }

    fn resource(&self) -> String {
        match self {
            Self::NoSuchBucket(b) | Self::BucketAlreadyExists(b) | Self::BucketNotEmpty(b) => {
                format!("/{}", b)
            }
            Self::NoSuchKey(k) => k.clone(),
            _ => "/".to_string(),
        }
    }

    fn status(&self) -> StatusCode {
        match self {
            Self::NoSuchBucket(_) => StatusCode::NOT_FOUND,
            Self::NoSuchKey(_) => StatusCode::NOT_FOUND,
            Self::BucketAlreadyExists(_) => StatusCode::CONFLICT,
            Self::EntityTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::BucketNotEmpty(_) => StatusCode::CONFLICT,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let request_id = uuid::Uuid::new_v4().to_string();
        let err = S3Error {
            code: self.code().to_string(),
            message: self.message(),
            resource: self.resource(),
            request_id: request_id.clone(),
        };

        let body = to_xml(&err).unwrap_or_else(|_| "<Error/>".to_string());

        (
            self.status(),
            [
                ("Content-Type", "application/xml"),
                ("x-amz-request-id", request_id.as_str()),
            ],
            body,
        )
            .into_response()
    }
}

impl From<crate::storage::StoreError> for ApiError {
    fn from(e: crate::storage::StoreError) -> Self {
        use crate::storage::StoreError::*;
        match e {
            NoSuchBucket => ApiError::NoSuchBucket("unknown".to_string()),
            NoSuchKey => ApiError::NoSuchKey("unknown".to_string()),
            BucketAlreadyExists => ApiError::BucketAlreadyExists("unknown".to_string()),
            EntityTooLarge => ApiError::EntityTooLarge,
            BucketNotEmpty => ApiError::BucketNotEmpty("unknown".to_string()),
        }
    }
}
