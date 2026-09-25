//! Property tests for size limits, error truncation, and topic key validation
//! (pure, no database).

use durable_workflows::{
    ActivityError, ActivityResult, ActivityTopic, ChildResult, DurableError, TopicRegistry,
    WorkflowError, MAX_ERROR_REASON_BYTES, MAX_OUTPUT_BYTES,
};
use proptest::prelude::*;

const MAX_CATEGORY_BYTES: usize = 64;

/// Checks that `truncated` is the longest char-boundary prefix of `original`
/// that fits in `max_bytes`.
fn assert_maximal_prefix(
    original: &str,
    truncated: &str,
    max_bytes: usize,
) -> Result<(), TestCaseError> {
    prop_assert!(
        truncated.len() <= max_bytes,
        "{} > {}",
        truncated.len(),
        max_bytes
    );
    prop_assert!(original.starts_with(truncated));
    if original.len() <= max_bytes {
        prop_assert_eq!(truncated, original);
    } else {
        let next = original[truncated.len()..]
            .chars()
            .next()
            .expect("a cut char remains");
        prop_assert!(
            truncated.len() + next.len_utf8() > max_bytes,
            "truncation dropped a char that still fits"
        );
    }
    Ok(())
}

/// Unicode strings with a length near the given byte limit.
fn text_near(limit: usize) -> impl Strategy<Value = String> {
    let chars = prop_oneof![
        any::<char>(),
        Just('a'),
        Just('é'),
        Just('€'),
        Just('😀'),
        Just('\u{0301}'),
    ];
    proptest::collection::vec(chars, 0..=(limit + 8)).prop_map(|chars| chars.into_iter().collect())
}

#[derive(Clone, Copy)]
struct DynTopic {
    key: &'static str,
    max_concurrency: u32,
}

impl ActivityTopic for DynTopic {
    fn key(self) -> &'static str {
        self.key
    }

    fn max_concurrency(self) -> u32 {
        self.max_concurrency
    }
}

/// The documented grammar: `[a-z0-9][a-z0-9._-]*`.
fn matches_topic_grammar(key: &str) -> bool {
    let bytes = key.as_bytes();
    let head = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    let tail = |b: u8| head(b) || b == b'.' || b == b'_' || b == b'-';
    !bytes.is_empty() && head(bytes[0]) && bytes[1..].iter().all(|b| tail(*b))
}

fn topic_key_candidates() -> impl Strategy<Value = String> {
    prop_oneof![
        ".*",
        "[a-z0-9][a-z0-9._-]{0,40}",
        "[a-zA-Z0-9._\\- ]{0,20}",
        "[a-z0-9._-]{0,8}[\\x{80}-\\x{10FFFF}][a-z0-9._-]{0,8}",
        "[a-z0-9]{1,8}(\u{212A}|\u{0130}|\u{017F}|ß|é)",
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    #[test]
    fn workflow_error_truncates_on_char_boundary(
        category in text_near(MAX_CATEGORY_BYTES / 2),
        message in text_near(MAX_ERROR_REASON_BYTES / 4),
    ) {
        let error = WorkflowError::new(category.clone(), message.clone());
        assert_maximal_prefix(&category, &error.category, MAX_CATEGORY_BYTES)?;
        assert_maximal_prefix(&message, &error.message, MAX_ERROR_REASON_BYTES)?;
    }

    #[test]
    fn activity_error_truncates_on_char_boundary(
        category in text_near(MAX_CATEGORY_BYTES / 2),
        message in text_near(MAX_ERROR_REASON_BYTES / 4),
        retryable in any::<bool>(),
    ) {
        let error = if retryable {
            ActivityError::retryable(category.clone(), message.clone())
        } else {
            ActivityError::permanent(category.clone(), message.clone())
        };
        let (got_category, got_message) = match &error {
            ActivityError::Retryable { category, message } => (category, message),
            ActivityError::Permanent { category, message } => (category, message),
        };
        prop_assert_eq!(matches!(error, ActivityError::Retryable { .. }), retryable);
        assert_maximal_prefix(&category, got_category, MAX_CATEGORY_BYTES)?;
        assert_maximal_prefix(&message, got_message, MAX_ERROR_REASON_BYTES)?;
    }

    /// Output limits are inclusive byte limits, independent of char count.
    #[test]
    fn result_output_limit_is_inclusive_in_bytes(
        slack in 0_usize..8,
        over in any::<bool>(),
        fill in prop_oneof![Just('a'), Just('é'), Just('€'), Just('😀')],
    ) {
        let target = if over { MAX_OUTPUT_BYTES + 1 + slack } else { MAX_OUTPUT_BYTES - slack };
        let mut output = String::with_capacity(target + 4);
        while output.len() + fill.len_utf8() <= target {
            output.push(fill);
        }
        while output.len() < target {
            output.push('a');
        }
        prop_assert_eq!(output.len(), target);
        let activity = ActivityResult::new("kind", 1, output.clone());
        let child = ChildResult::new("kind", 1, output);
        prop_assert_eq!(activity.is_ok(), !over);
        prop_assert_eq!(child.is_ok(), !over);
        if over {
            let is_payload_too_large = matches!(
                activity,
                Err(DurableError::PayloadTooLarge { actual_bytes, max_bytes, .. })
                    if actual_bytes == target && max_bytes == MAX_OUTPUT_BYTES
            );
            prop_assert!(is_payload_too_large);
        }
    }

    #[test]
    fn topic_registry_accepts_iff_key_matches_grammar(
        key in topic_key_candidates(),
        max_concurrency in 1_u32..=64,
    ) {
        let leaked: &'static str = Box::leak(key.clone().into_boxed_str());
        let mut registry = TopicRegistry::new();
        let result = registry.register(DynTopic { key: leaked, max_concurrency });
        prop_assert_eq!(result.is_ok(), matches_topic_grammar(&key), "key {:?}", key);
        if result.is_ok() {
            prop_assert_eq!(registry.get(&key).map(|topic| topic.max_concurrency), Some(max_concurrency));
            // Re-registering the same limit is idempotent; a different limit conflicts.
            let same = registry.register(DynTopic { key: leaked, max_concurrency });
            prop_assert!(same.is_ok());
            let conflicting = registry.register(DynTopic { key: leaked, max_concurrency: max_concurrency + 1 });
            prop_assert!(conflicting.is_err());
            prop_assert_eq!(registry.len(), 1);
        } else {
            prop_assert!(matches!(result, Err(DurableError::InvalidDefinition(_))));
            prop_assert!(registry.is_empty());
        }
    }

    #[test]
    fn topic_registry_rejects_zero_concurrency(key in "[a-z0-9][a-z0-9._-]{0,20}") {
        let leaked: &'static str = Box::leak(key.into_boxed_str());
        let mut registry = TopicRegistry::new();
        let result = registry.register(DynTopic { key: leaked, max_concurrency: 0 });
        prop_assert!(result.is_err());
    }
}

#[test]
fn truncation_at_exact_limits() {
    let category = "é".repeat(MAX_CATEGORY_BYTES); // 128 bytes
    let message = "😀".repeat(MAX_ERROR_REASON_BYTES); // 8 KiB
    let error = WorkflowError::new(category, message);
    assert_eq!(error.category.len(), MAX_CATEGORY_BYTES);
    assert_eq!(error.message.len(), MAX_ERROR_REASON_BYTES);

    let odd = format!("a{}", "€".repeat(MAX_CATEGORY_BYTES));
    let error = WorkflowError::new(odd, "");
    assert_eq!(error.category.len(), 64);
}
