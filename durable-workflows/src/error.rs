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
        self.transience() == Some(Transience::RolledBack)
    }

    /// Whether the error is expected to clear without a code or config
    /// change, and how; `None` for every other error. The one classifier of
    /// database errors: the coordinator and dispatcher skip `RolledBack`
    /// (G1), the supervisor restarts a task that failed with either kind
    /// without spending its restart budget (§2.1).
    pub(crate) fn transience(&self) -> Option<Transience> {
        match self {
            Self::Database(error) => query_error_transience(error),
            Self::Pool(error) => pool_error_transience(error),
            Self::PoolCheckout(diesel_async::pooled_connection::bb8::RunError::TimedOut) => {
                Some(Transience::Unavailable)
            }
            Self::PoolCheckout(diesel_async::pooled_connection::bb8::RunError::User(error)) => {
                pool_error_transience(error)
            }
            Self::Migration(_)
            | Self::Serialization(_)
            | Self::InvalidPayload { .. }
            | Self::InvalidDefinition(_)
            | Self::MissingDefinition { .. }
            | Self::MissingCurrentDefinition { .. }
            | Self::MissingDefinitions { .. }
            | Self::DuplicateDefinition { .. }
            | Self::InvalidState(_)
            | Self::FencedWrite
            | Self::InvalidId(_)
            | Self::NotFound { .. }
            | Self::Conflict(_)
            | Self::InvalidCursor(_)
            | Self::PayloadTooLarge { .. }
            | Self::DefinitionMismatch { .. } => None,
        }
    }
}

/// Why a database error is expected to clear by itself
/// ([`DurableError::transience`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Transience {
    /// The database rolled the transaction back (deadlock, serialization
    /// failure, lock wait timeout); retrying the whole transaction is safe.
    RolledBack,
    /// The database could not be reached or is not serving: a refused,
    /// reset or closed connection, a pool checkout that timed out, a server
    /// that is starting, stopping or failing over (Vitess "primary is not
    /// serving", a read-only former primary). Whether a statement in flight
    /// committed is unknown, so this is not a reason to retry a transaction.
    Unavailable,
}

fn query_error_transience(error: &diesel::result::Error) -> Option<Transience> {
    match error {
        diesel::result::Error::DatabaseError(kind, info) => {
            crate::dialect::database_error_transience(kind, info.message())
        }
        // The connection failed during COMMIT and the rollback failed with
        // it: whether the transaction committed is unknown.
        diesel::result::Error::RollbackErrorOnCommit { commit_error, .. } => {
            query_error_transience(commit_error).map(|_| Transience::Unavailable)
        }
        _ => None,
    }
}

fn pool_error_transience(error: &diesel_async::pooled_connection::PoolError) -> Option<Transience> {
    use diesel::result::ConnectionError;
    use diesel_async::pooled_connection::PoolError;

    match error {
        // A failed health check of a pooled connection.
        PoolError::QueryError(error) => query_error_transience(error),
        // MySQL: an I/O or driver error while connecting (refused, reset,
        // closed during the handshake).
        PoolError::ConnectionError(ConnectionError::BadConnection(_)) => {
            Some(Transience::Unavailable)
        }
        // Postgres reports every connect failure here, MySQL a server error
        // during the handshake: classify the underlying error, so a refused
        // connection is transient and a rejected password is not.
        PoolError::ConnectionError(ConnectionError::CouldntSetupConfiguration(error)) => {
            query_error_transience(error).map(|_| Transience::Unavailable)
        }
        PoolError::ConnectionError(_) => None,
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

#[cfg(test)]
mod tests {
    use diesel::result::{ConnectionError, DatabaseErrorKind, Error as QueryError};
    use diesel_async::pooled_connection::{bb8::RunError, PoolError};

    use super::*;

    fn database(kind: DatabaseErrorKind, message: &str) -> QueryError {
        QueryError::DatabaseError(kind, Box::new(message.to_string()))
    }

    #[test]
    fn transience_separates_rollbacks_from_an_unavailable_database() {
        let rolled_back = DurableError::Database(database(
            DatabaseErrorKind::SerializationFailure,
            "could not serialize access",
        ));
        assert_eq!(rolled_back.transience(), Some(Transience::RolledBack));
        assert!(rolled_back.is_transient());

        let unavailable = [
            DurableError::PoolCheckout(RunError::TimedOut),
            DurableError::Database(database(DatabaseErrorKind::ClosedConnection, "closed")),
            DurableError::Pool(PoolError::ConnectionError(ConnectionError::BadConnection(
                "Input/output error: Connection refused (os error 111)".to_string(),
            ))),
            DurableError::PoolCheckout(RunError::User(PoolError::QueryError(database(
                DatabaseErrorKind::ClosedConnection,
                "closed",
            )))),
            // A refused connect on Postgres.
            DurableError::Pool(PoolError::ConnectionError(
                ConnectionError::CouldntSetupConfiguration(database(
                    DatabaseErrorKind::ClosedConnection,
                    "closed",
                )),
            )),
            DurableError::Database(QueryError::RollbackErrorOnCommit {
                rollback_error: Box::new(database(DatabaseErrorKind::ClosedConnection, "closed")),
                commit_error: Box::new(database(DatabaseErrorKind::ClosedConnection, "closed")),
            }),
        ];
        for error in unavailable {
            assert_eq!(error.transience(), Some(Transience::Unavailable), "{error}");
            assert!(
                !error.is_transient(),
                "a lost connection is not a safe transaction retry: {error}"
            );
        }
    }

    #[test]
    fn other_errors_have_no_transience() {
        for error in [
            DurableError::InvalidState("bug".to_string()),
            DurableError::FencedWrite,
            DurableError::Database(QueryError::NotFound),
            DurableError::Database(database(DatabaseErrorKind::UniqueViolation, "duplicate")),
            DurableError::Pool(PoolError::ConnectionError(
                ConnectionError::InvalidConnectionUrl("nope".to_string()),
            )),
            DurableError::Pool(PoolError::ConnectionError(
                ConnectionError::CouldntSetupConfiguration(database(
                    DatabaseErrorKind::Unknown,
                    "password authentication failed for user \"durable\"",
                )),
            )),
        ] {
            assert_eq!(error.transience(), None, "{error}");
        }
    }
}
