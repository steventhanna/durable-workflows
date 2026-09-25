use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde::{Deserialize, Serialize};

use crate::DurableError;

const CURSOR_VERSION: u8 = 1;
const MAX_CURSOR_BYTES: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorPosition {
    pub timestamp: i64,
    pub tie_breaker: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorPayload {
    version: u8,
    scope: String,
    timestamp: i64,
    tie_breaker: String,
}

pub fn encode_cursor(scope: &str, position: &CursorPosition) -> Result<String, DurableError> {
    validate_scope(scope)?;
    validate_tie_breaker(&position.tie_breaker)?;
    let json = serde_json::to_vec(&CursorPayload {
        version: CURSOR_VERSION,
        scope: scope.to_string(),
        timestamp: position.timestamp,
        tie_breaker: position.tie_breaker.clone(),
    })?;
    Ok(URL_SAFE_NO_PAD.encode(json))
}

pub fn decode_cursor(scope: &str, cursor: &str) -> Result<CursorPosition, DurableError> {
    validate_scope(scope)?;
    if cursor.is_empty() || cursor.len() > MAX_CURSOR_BYTES {
        return Err(invalid_cursor("cursor length is outside the allowed range"));
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| invalid_cursor("cursor is not valid URL-safe base64"))?;
    let payload: CursorPayload = serde_json::from_slice(&bytes)
        .map_err(|_| invalid_cursor("cursor payload is malformed"))?;
    if payload.version != CURSOR_VERSION {
        return Err(invalid_cursor("cursor version is not supported"));
    }
    if payload.scope != scope {
        return Err(invalid_cursor("cursor belongs to a different collection"));
    }
    validate_tie_breaker(&payload.tie_breaker)?;
    Ok(CursorPosition {
        timestamp: payload.timestamp,
        tie_breaker: payload.tie_breaker,
    })
}

pub(super) fn scoped_cursor_scope(collection: &str, identity: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in identity.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{collection}:{hash:016x}")
}

fn validate_scope(scope: &str) -> Result<(), DurableError> {
    if scope.is_empty() || scope.len() > 64 || !scope.is_ascii() {
        return Err(invalid_cursor("cursor scope is invalid"));
    }
    Ok(())
}

fn validate_tie_breaker(tie_breaker: &str) -> Result<(), DurableError> {
    if tie_breaker.is_empty() || tie_breaker.len() > 191 {
        return Err(invalid_cursor("cursor tie breaker is invalid"));
    }
    Ok(())
}

fn invalid_cursor(message: &str) -> DurableError {
    DurableError::InvalidCursor(message.to_string())
}
