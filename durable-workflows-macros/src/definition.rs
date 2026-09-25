use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    parenthesized, parse::Parse, parse::ParseStream, DeriveInput, Error, Ident, LitInt, LitStr,
    Path, Result, Token,
};

enum BackoffMetadata {
    Fixed {
        delay_secs: u64,
    },
    Exponential {
        initial_secs: u64,
        max_secs: u64,
        jitter_percent: u8,
    },
}

impl Parse for BackoffMetadata {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let kind: Ident = input.parse()?;
        let content;
        parenthesized!(content in input);

        let mut delay_secs = None;
        let mut initial_secs = None;
        let mut max_secs = None;
        let mut jitter_percent = None;

        while !content.is_empty() {
            let field: Ident = content.parse()?;
            content.parse::<Token![=]>()?;
            let value: LitInt = content.parse()?;
            match field.to_string().as_str() {
                "delay_secs" if delay_secs.is_none() => {
                    delay_secs = Some(value.base10_parse::<u64>()?)
                }
                "initial_secs" if initial_secs.is_none() => {
                    initial_secs = Some(value.base10_parse::<u64>()?)
                }
                "max_secs" if max_secs.is_none() => max_secs = Some(value.base10_parse::<u64>()?),
                "jitter_percent" if jitter_percent.is_none() => {
                    jitter_percent = Some(value.base10_parse::<u8>()?)
                }
                "delay_secs" | "initial_secs" | "max_secs" | "jitter_percent" => {
                    return Err(Error::new_spanned(field, "duplicate backoff metadata"));
                }
                _ => return Err(Error::new_spanned(field, "unsupported backoff metadata")),
            }
            if content.peek(Token![,]) {
                content.parse::<Token![,]>()?;
            }
        }

        match kind.to_string().as_str() {
            "fixed" => {
                if initial_secs.is_some() || max_secs.is_some() || jitter_percent.is_some() {
                    return Err(Error::new_spanned(
                        kind,
                        "fixed backoff accepts only delay_secs",
                    ));
                }
                let delay_secs = delay_secs
                    .ok_or_else(|| Error::new_spanned(&kind, "fixed backoff needs delay_secs"))?;
                if delay_secs == 0 {
                    return Err(Error::new_spanned(
                        kind,
                        "fixed backoff delay_secs must be greater than zero",
                    ));
                }
                Ok(Self::Fixed { delay_secs })
            }
            "exponential" => {
                if delay_secs.is_some() {
                    return Err(Error::new_spanned(
                        kind,
                        "exponential backoff does not accept delay_secs",
                    ));
                }
                let initial_secs = initial_secs.ok_or_else(|| {
                    Error::new_spanned(&kind, "exponential backoff needs initial_secs")
                })?;
                let max_secs = max_secs.ok_or_else(|| {
                    Error::new_spanned(&kind, "exponential backoff needs max_secs")
                })?;
                let jitter_percent = jitter_percent.ok_or_else(|| {
                    Error::new_spanned(&kind, "exponential backoff needs jitter_percent")
                })?;
                if initial_secs == 0 {
                    return Err(Error::new_spanned(
                        &kind,
                        "exponential backoff initial_secs must be greater than zero",
                    ));
                }
                if max_secs < initial_secs {
                    return Err(Error::new_spanned(
                        &kind,
                        "exponential backoff max_secs must be at least initial_secs",
                    ));
                }
                if jitter_percent > 100 {
                    return Err(Error::new_spanned(
                        &kind,
                        "exponential backoff jitter_percent must be at most 100",
                    ));
                }
                Ok(Self::Exponential {
                    initial_secs,
                    max_secs,
                    jitter_percent,
                })
            }
            _ => Err(Error::new_spanned(
                kind,
                "backoff must be fixed(...) or exponential(...)",
            )),
        }
    }
}

struct WorkflowMetadata {
    kind: LitStr,
    version: i32,
}

struct ActivityMetadata {
    kind: LitStr,
    version: i32,
    topic: Path,
    max_attempts: u32,
    timeout_secs: u64,
    lease_secs: u64,
    backoff: BackoffMetadata,
}

struct ScheduleMetadata {
    key: LitStr,
    version: i32,
    cron: LitStr,
    timezone: LitStr,
    misfire: Ident,
    catch_up_limit: Option<u32>,
    overlap: Ident,
    misfire_grace_secs: u64,
}

fn required<T>(value: Option<T>, input: &DeriveInput, name: &str) -> Result<T> {
    value.ok_or_else(|| Error::new_spanned(input, format!("missing `{name}` metadata")))
}

pub(crate) fn is_lower_snake_case(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('_')
        && !value.ends_with('_')
        && !value.contains("__")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn parse_workflow(input: &DeriveInput) -> Result<WorkflowMetadata> {
    let mut kind = None;
    let mut version = None;
    let mut found_attribute = false;

    for attribute in &input.attrs {
        if !attribute.path().is_ident("workflow") {
            continue;
        }
        if found_attribute {
            return Err(Error::new_spanned(
                attribute,
                "duplicate `workflow` attribute",
            ));
        }
        found_attribute = true;
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("kind") {
                if kind.is_some() {
                    return Err(meta.error("duplicate `kind` metadata"));
                }
                kind = Some(meta.value()?.parse::<LitStr>()?);
                return Ok(());
            }
            if meta.path.is_ident("version") {
                if version.is_some() {
                    return Err(meta.error("duplicate `version` metadata"));
                }
                let literal = meta.value()?.parse::<LitInt>()?;
                version = Some(literal.base10_parse::<i32>()?);
                return Ok(());
            }
            Err(meta.error("unsupported workflow metadata"))
        })?;
    }

    if !found_attribute {
        return Err(Error::new_spanned(input, "missing `workflow` attribute"));
    }

    let kind = required(kind, input, "kind")?;
    if !is_lower_snake_case(&kind.value()) {
        return Err(Error::new_spanned(
            &kind,
            "workflow kind must be lower snake_case",
        ));
    }
    let version = required(version, input, "version")?;
    if version == 0 {
        return Err(Error::new_spanned(
            input,
            "workflow version must be greater than zero",
        ));
    }

    Ok(WorkflowMetadata { kind, version })
}

fn parse_activity(input: &DeriveInput) -> Result<ActivityMetadata> {
    let mut kind = None;
    let mut version = None;
    let mut topic = None;
    let mut max_attempts = None;
    let mut timeout_secs = None;
    let mut lease_secs = None;
    let mut backoff = None;
    let mut found_attribute = false;

    for attribute in &input.attrs {
        if !attribute.path().is_ident("activity") {
            continue;
        }
        if found_attribute {
            return Err(Error::new_spanned(
                attribute,
                "duplicate `activity` attribute",
            ));
        }
        found_attribute = true;
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("kind") {
                if kind.is_some() {
                    return Err(meta.error("duplicate `kind` metadata"));
                }
                kind = Some(meta.value()?.parse::<LitStr>()?);
                return Ok(());
            }
            if meta.path.is_ident("version") {
                if version.is_some() {
                    return Err(meta.error("duplicate `version` metadata"));
                }
                version = Some(meta.value()?.parse::<LitInt>()?.base10_parse::<i32>()?);
                return Ok(());
            }
            if meta.path.is_ident("topic") {
                if topic.is_some() {
                    return Err(meta.error("duplicate `topic` metadata"));
                }
                topic = Some(meta.value()?.parse::<Path>()?);
                return Ok(());
            }
            if meta.path.is_ident("max_attempts") {
                if max_attempts.is_some() {
                    return Err(meta.error("duplicate `max_attempts` metadata"));
                }
                max_attempts = Some(meta.value()?.parse::<LitInt>()?.base10_parse::<u32>()?);
                return Ok(());
            }
            if meta.path.is_ident("timeout_secs") {
                if timeout_secs.is_some() {
                    return Err(meta.error("duplicate `timeout_secs` metadata"));
                }
                timeout_secs = Some(meta.value()?.parse::<LitInt>()?.base10_parse::<u64>()?);
                return Ok(());
            }
            if meta.path.is_ident("lease_secs") {
                if lease_secs.is_some() {
                    return Err(meta.error("duplicate `lease_secs` metadata"));
                }
                lease_secs = Some(meta.value()?.parse::<LitInt>()?.base10_parse::<u64>()?);
                return Ok(());
            }
            if meta.path.is_ident("backoff") {
                if backoff.is_some() {
                    return Err(meta.error("duplicate `backoff` metadata"));
                }
                backoff = Some(meta.value()?.parse::<BackoffMetadata>()?);
                return Ok(());
            }
            Err(meta.error("unsupported activity metadata"))
        })?;
    }

    if !found_attribute {
        return Err(Error::new_spanned(input, "missing `activity` attribute"));
    }

    let kind = required(kind, input, "kind")?;
    if !is_lower_snake_case(&kind.value()) {
        return Err(Error::new_spanned(
            &kind,
            "activity kind must be lower snake_case",
        ));
    }
    let version = required(version, input, "version")?;
    if version == 0 {
        return Err(Error::new_spanned(
            input,
            "activity version must be greater than zero",
        ));
    }
    let topic = required(topic, input, "topic")?;
    if topic.segments.len() < 2 {
        return Err(Error::new_spanned(
            &topic,
            "activity topic must be a qualified enum variant such as Topics::ExternalApi",
        ));
    }
    let max_attempts = required(max_attempts, input, "max_attempts")?;
    if max_attempts == 0 {
        return Err(Error::new_spanned(
            input,
            "activity max_attempts must be greater than zero",
        ));
    }
    let timeout_secs = required(timeout_secs, input, "timeout_secs")?;
    if timeout_secs == 0 {
        return Err(Error::new_spanned(
            input,
            "activity timeout_secs must be greater than zero",
        ));
    }
    let lease_secs = required(lease_secs, input, "lease_secs")?;
    if lease_secs <= timeout_secs {
        return Err(Error::new_spanned(
            input,
            "activity lease_secs must be greater than timeout_secs",
        ));
    }
    let backoff = required(backoff, input, "backoff")?;

    Ok(ActivityMetadata {
        kind,
        version,
        topic,
        max_attempts,
        timeout_secs,
        lease_secs,
        backoff,
    })
}

fn parse_schedule(input: &DeriveInput) -> Result<ScheduleMetadata> {
    let mut key = None;
    let mut version = None;
    let mut cron = None;
    let mut timezone = None;
    let mut misfire = None;
    let mut catch_up_limit = None;
    let mut overlap = None;
    let mut misfire_grace_secs = None;
    let mut found_attribute = false;

    for attribute in &input.attrs {
        if !attribute.path().is_ident("schedule") {
            continue;
        }
        if found_attribute {
            return Err(Error::new_spanned(
                attribute,
                "duplicate `schedule` attribute",
            ));
        }
        found_attribute = true;
        attribute.parse_nested_meta(|meta| {
            macro_rules! assign {
                ($target:ident, $name:literal, $ty:ty) => {{
                    if $target.is_some() {
                        return Err(meta.error(concat!("duplicate `", $name, "` metadata")));
                    }
                    $target = Some(meta.value()?.parse::<$ty>()?);
                    return Ok(());
                }};
            }
            if meta.path.is_ident("key") {
                assign!(key, "key", LitStr);
            }
            if meta.path.is_ident("version") {
                if version.is_some() {
                    return Err(meta.error("duplicate `version` metadata"));
                }
                version = Some(meta.value()?.parse::<LitInt>()?.base10_parse::<i32>()?);
                return Ok(());
            }
            if meta.path.is_ident("cron") {
                assign!(cron, "cron", LitStr);
            }
            if meta.path.is_ident("timezone") {
                assign!(timezone, "timezone", LitStr);
            }
            if meta.path.is_ident("misfire") {
                assign!(misfire, "misfire", Ident);
            }
            if meta.path.is_ident("catch_up_limit") {
                if catch_up_limit.is_some() {
                    return Err(meta.error("duplicate `catch_up_limit` metadata"));
                }
                catch_up_limit = Some(meta.value()?.parse::<LitInt>()?.base10_parse::<u32>()?);
                return Ok(());
            }
            if meta.path.is_ident("overlap") {
                assign!(overlap, "overlap", Ident);
            }
            if meta.path.is_ident("misfire_grace_secs") {
                if misfire_grace_secs.is_some() {
                    return Err(meta.error("duplicate `misfire_grace_secs` metadata"));
                }
                misfire_grace_secs = Some(meta.value()?.parse::<LitInt>()?.base10_parse::<u64>()?);
                return Ok(());
            }
            Err(meta.error("unsupported schedule metadata"))
        })?;
    }

    if !found_attribute {
        return Err(Error::new_spanned(input, "missing `schedule` attribute"));
    }
    let key = required(key, input, "key")?;
    if !is_lower_snake_case(&key.value()) {
        return Err(Error::new_spanned(
            &key,
            "schedule key must be lower snake_case",
        ));
    }
    let version = required(version, input, "version")?;
    if version <= 0 {
        return Err(Error::new_spanned(
            input,
            "schedule version must be greater than zero",
        ));
    }
    let cron = required(cron, input, "cron")?;
    let timezone = required(timezone, input, "timezone")?;
    let misfire = required(misfire, input, "misfire")?;
    match misfire.to_string().as_str() {
        "Skip" | "RunLatest" if catch_up_limit.is_none() => {}
        "Skip" | "RunLatest" => {
            return Err(Error::new_spanned(
                &misfire,
                "catch_up_limit is only valid with CatchUp",
            ));
        }
        "CatchUp" if matches!(catch_up_limit, Some(1..=100)) => {}
        "CatchUp" => {
            return Err(Error::new_spanned(
                &misfire,
                "CatchUp requires catch_up_limit from 1 through 100",
            ));
        }
        _ => {
            return Err(Error::new_spanned(
                &misfire,
                "misfire must be Skip, RunLatest, or CatchUp",
            ));
        }
    }
    let overlap = required(overlap, input, "overlap")?;
    if !matches!(
        overlap.to_string().as_str(),
        "Allow" | "SkipIfActive" | "QueueOne"
    ) {
        return Err(Error::new_spanned(
            &overlap,
            "overlap must be Allow, SkipIfActive, or QueueOne",
        ));
    }
    let misfire_grace_secs = required(misfire_grace_secs, input, "misfire_grace_secs")?;
    if misfire_grace_secs == 0 {
        return Err(Error::new_spanned(
            input,
            "misfire_grace_secs must be greater than zero",
        ));
    }

    Ok(ScheduleMetadata {
        key,
        version,
        cron,
        timezone,
        misfire,
        catch_up_limit,
        overlap,
        misfire_grace_secs,
    })
}

pub fn derive_workflow(input: DeriveInput) -> TokenStream {
    match parse_workflow(&input) {
        Ok(metadata) => {
            let name = &input.ident;
            let kind = metadata.kind;
            let version = metadata.version;
            quote! {
                impl ::durable_workflows::DurableWorkflow for #name {
                    const KIND: &'static str = #kind;
                    const VERSION: i32 = #version;
                }
            }
        }
        Err(error) => error.into_compile_error(),
    }
}

pub fn derive_activity(input: DeriveInput) -> TokenStream {
    match parse_activity(&input) {
        Ok(metadata) => {
            let name = &input.ident;
            let kind = metadata.kind;
            let version = metadata.version;
            let topic = metadata.topic;
            let mut topic_type = topic.clone();
            topic_type.segments.pop();
            topic_type.segments.pop_punct();
            let max_attempts = metadata.max_attempts;
            let timeout_secs = metadata.timeout_secs;
            let lease_secs = metadata.lease_secs;
            let retry_policy = match metadata.backoff {
                BackoffMetadata::Fixed { delay_secs } => quote! {
                    ::durable_workflows::RetryPolicy::from_validated(
                        ::durable_workflows::BackoffPolicy::Fixed { delay_secs: #delay_secs }
                    )
                },
                BackoffMetadata::Exponential {
                    initial_secs,
                    max_secs,
                    jitter_percent,
                } => quote! {
                    ::durable_workflows::RetryPolicy::from_validated(
                        ::durable_workflows::BackoffPolicy::Exponential {
                            initial_secs: #initial_secs,
                            max_secs: #max_secs,
                            jitter_percent: #jitter_percent,
                        }
                    )
                },
            };
            quote! {
                impl ::durable_workflows::DurableActivity for #name {
                    type Topic = #topic_type;

                    const KIND: &'static str = #kind;
                    const VERSION: i32 = #version;
                    const MAX_ATTEMPTS: u32 = #max_attempts;
                    const TIMEOUT: ::std::time::Duration = ::std::time::Duration::from_secs(#timeout_secs);
                    const LEASE_DURATION: ::std::time::Duration = ::std::time::Duration::from_secs(#lease_secs);

                    fn topic() -> Self::Topic {
                        #topic
                    }

                    fn retry_policy() -> ::durable_workflows::RetryPolicy {
                        #retry_policy
                    }
                }
            }
        }
        Err(error) => error.into_compile_error(),
    }
}

pub fn derive_schedule(input: DeriveInput) -> TokenStream {
    match parse_schedule(&input) {
        Ok(metadata) => {
            let name = &input.ident;
            let key = metadata.key;
            let version = metadata.version;
            let cron = metadata.cron;
            let timezone = metadata.timezone;
            let overlap = metadata.overlap;
            let misfire_grace_secs = metadata.misfire_grace_secs;
            let misfire = if metadata.misfire == "CatchUp" {
                let max_occurrences = metadata
                    .catch_up_limit
                    .expect("validated CatchUp metadata has a limit");
                quote! {
                    ::durable_workflows::MisfirePolicy::CatchUp { max_occurrences: #max_occurrences }
                }
            } else {
                let variant = metadata.misfire;
                quote! { ::durable_workflows::MisfirePolicy::#variant }
            };
            quote! {
                impl ::durable_workflows::DurableSchedule for #name {
                    const KEY: &'static str = #key;
                    const VERSION: i32 = #version;
                    const CRON: &'static str = #cron;
                    const TIMEZONE: &'static str = #timezone;
                    const MISFIRE: ::durable_workflows::MisfirePolicy = #misfire;
                    const OVERLAP: ::durable_workflows::OverlapPolicy =
                        ::durable_workflows::OverlapPolicy::#overlap;
                    const MISFIRE_GRACE: ::std::time::Duration =
                        ::std::time::Duration::from_secs(#misfire_grace_secs);
                }
            }
        }
        Err(error) => error.into_compile_error(),
    }
}

#[cfg(test)]
mod tests {
    use syn::parse_quote;

    use super::{is_lower_snake_case, parse_activity, parse_schedule};

    #[test]
    fn durable_kinds_are_lower_snake_case() {
        assert!(is_lower_snake_case("submit_fax_v2"));
        assert!(!is_lower_snake_case("SubmitFax"));
        assert!(!is_lower_snake_case("submit__fax"));
        assert!(!is_lower_snake_case("_submit_fax"));
    }

    #[test]
    fn invalid_exponential_backoff_is_rejected_during_expansion() {
        let input = parse_quote! {
            #[activity(
                kind = "submit_fax",
                version = 1,
                topic = Topics::Fax,
                max_attempts = 3,
                timeout_secs = 30,
                lease_secs = 60,
                backoff = exponential(
                    initial_secs = 30,
                    max_secs = 5,
                    jitter_percent = 20
                )
            )]
            struct SubmitFax;
        };

        let error = match parse_activity(&input) {
            Ok(_) => panic!("the maximum cannot precede the initial delay"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("max_secs must be at least initial_secs"));
    }

    #[test]
    fn duplicate_metadata_is_rejected_during_expansion() {
        let input = parse_quote! {
            #[activity(
                kind = "submit_fax",
                kind = "another_fax",
                version = 1,
                topic = Topics::Fax,
                max_attempts = 3,
                timeout_secs = 30,
                lease_secs = 60,
                backoff = fixed(delay_secs = 5)
            )]
            struct SubmitFax;
        };

        let error = match parse_activity(&input) {
            Ok(_) => panic!("metadata may appear only once"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("duplicate `kind` metadata"));
    }

    #[test]
    fn catch_up_requires_a_bounded_limit() {
        let input = parse_quote! {
            #[schedule(
                key = "daily",
                version = 1,
                cron = "0 0 8 * * *",
                timezone = "UTC",
                misfire = CatchUp,
                catch_up_limit = 101,
                overlap = Allow,
                misfire_grace_secs = 60,
            )]
            struct Daily;
        };
        let error = match parse_schedule(&input) {
            Ok(_) => panic!("unbounded catch-up must fail expansion"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("1 through 100"));
    }
}
