use crate::DefinitionKey;
use crate::MAX_ERROR_REASON_BYTES;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
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

impl DurableError {
    /// Whether the database aborted the transaction for a reason that a retry
    /// of the whole transaction can clear: a deadlock, a serialization failure
    /// or a lock wait timeout. A cancel that cascades to owned children locks
    /// parent before child, so it can deadlock with a child that finishes at
    /// the same moment (INVARIANTS §2.8); retry it when this holds.
    pub fn is_transient(&self) -> bool {
        crate::dialect::is_transient_error(self)
    }
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
#[non_exhaustive]
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

/// Kani proofs (`cargo kani`; CLAUDE.md, "Bounded model checking").
#[cfg(kani)]
mod verification {
    use super::truncate_utf8;

    /// Bytes in the symbolic input: room for a 4-byte char that starts at
    /// any of the first 4 offsets, so every cut position inside a char of
    /// every width is covered.
    const MAX_BYTES: usize = 8;

    /// For any UTF-8 text of up to `MAX_BYTES` bytes and any byte limit,
    /// the result is a prefix of the text that fits the limit and no longer
    /// char-boundary prefix fits: every offset between its end and the
    /// limit is inside a char. The unwind bound 10 covers the UTF-8 check
    /// and the byte copies and comparisons over at most 8 bytes (the
    /// boundary search steps back at most 3 bytes, since a char is at most
    /// 4 bytes).
    #[kani::proof]
    #[kani::unwind(10)]
    fn truncate_utf8_keeps_the_longest_fitting_prefix() {
        let bytes: [u8; MAX_BYTES] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= MAX_BYTES);
        let Ok(text) = std::str::from_utf8(&bytes[..len]) else {
            return;
        };
        let max_bytes: usize = kani::any();

        let truncated = truncate_utf8(text.to_owned(), max_bytes);

        assert!(text.as_bytes().starts_with(truncated.as_bytes()));
        if text.len() <= max_bytes {
            assert!(truncated.len() == text.len());
        } else {
            assert!(truncated.len() <= max_bytes);
            let longer: usize = kani::any();
            kani::assume(truncated.len() < longer && longer <= max_bytes);
            assert!(!text.is_char_boundary(longer));
        }
    }
}
