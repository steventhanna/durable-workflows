use crate::DefinitionKey;
use crate::MAX_ERROR_REASON_BYTES;

#[derive(Debug, thiserror::Error)]
pub enum DurableError {
    #[error("database error: {0}")]
    Database(#[from] diesel::result::Error),

    #[error("database pool error: {0}")]
    Pool(#[from] diesel_async::pooled_connection::PoolError),

    #[error("database pool checkout error: {0}")]
    PoolCheckout(#[from] diesel_async::pooled_connection::bb8::RunError),

    #[error("durable migration failed: {0}")]
    Migration(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("durable payload serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("invalid {field}: {message}")]
    InvalidPayload {
        field: &'static str,
        message: String,
    },

    #[error("invalid durable definition: {0}")]
    InvalidDefinition(String),

    #[error("missing durable definition {kind} v{version}")]
    MissingDefinition { kind: String, version: i32 },

    #[error("missing current durable definition for {kind}")]
    MissingCurrentDefinition { kind: String },

    #[error("durable readiness is missing workflow definitions {workflows:?}, activity definitions {activities:?}, and activity topics {topics:?}")]
    MissingDefinitions {
        workflows: Vec<DefinitionKey>,
        activities: Vec<DefinitionKey>,
        topics: Vec<String>,
    },

    #[error("duplicate durable definition {kind} v{version}")]
    DuplicateDefinition { kind: String, version: i32 },

    #[error("invalid durable state: {0}")]
    InvalidState(String),

    #[error("durable write lost its lease fence")]
    FencedWrite,

    #[error("invalid durable identifier: {0}")]
    InvalidId(i64),

    #[error("durable resource not found: {resource} {identifier}")]
    NotFound {
        resource: &'static str,
        identifier: String,
    },

    #[error("durable operation conflicts with current state: {0}")]
    Conflict(String),

    #[error("invalid durable admin cursor: {0}")]
    InvalidCursor(String),

    #[error("{field} is {actual_bytes} bytes; maximum is {max_bytes} bytes")]
    PayloadTooLarge {
        field: &'static str,
        actual_bytes: usize,
        max_bytes: usize,
    },

    #[error(
        "stored result definition {actual_kind} v{actual_version} does not match expected {expected_kind} v{expected_version}"
    )]
    DefinitionMismatch {
        actual_kind: String,
        actual_version: i32,
        expected_kind: String,
        expected_version: i32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{category}: {message}")]
pub struct WorkflowError {
    pub category: String,
    pub message: String,
}

impl WorkflowError {
    pub fn new(category: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            category: truncate_utf8(category.into(), 64),
            message: truncate_utf8(message.into(), MAX_ERROR_REASON_BYTES),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ActivityError {
    #[error("retryable activity error {category}: {message}")]
    Retryable { category: String, message: String },

    #[error("permanent activity error {category}: {message}")]
    Permanent { category: String, message: String },
}

impl ActivityError {
    pub fn retryable(category: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Retryable {
            category: truncate_utf8(category.into(), 64),
            message: truncate_utf8(message.into(), MAX_ERROR_REASON_BYTES),
        }
    }

    pub fn permanent(category: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Permanent {
            category: truncate_utf8(category.into(), 64),
            message: truncate_utf8(message.into(), MAX_ERROR_REASON_BYTES),
        }
    }
}

pub(crate) fn ensure_size(
    field: &'static str,
    value: &str,
    max_bytes: usize,
) -> Result<(), DurableError> {
    let actual_bytes = value.len();
    if actual_bytes > max_bytes {
        return Err(DurableError::PayloadTooLarge {
            field,
            actual_bytes,
            max_bytes,
        });
    }
    Ok(())
}

pub(crate) fn truncate_utf8(mut value: String, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value;
    }
    let mut boundary = max_bytes;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value.truncate(boundary);
    value
}
