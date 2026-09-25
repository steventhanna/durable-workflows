mod definition;
mod flow;

use proc_macro::TokenStream;
use syn::parse_macro_input;

#[proc_macro_derive(DurableWorkflow, attributes(workflow))]
pub fn derive_durable_workflow(input: TokenStream) -> TokenStream {
    definition::derive_workflow(parse_macro_input!(input)).into()
}

#[proc_macro_derive(DurableActivity, attributes(activity))]
pub fn derive_durable_activity(input: TokenStream) -> TokenStream {
    definition::derive_activity(parse_macro_input!(input)).into()
}

#[proc_macro_derive(DurableSchedule, attributes(schedule))]
pub fn derive_durable_schedule(input: TokenStream) -> TokenStream {
    definition::derive_schedule(parse_macro_input!(input)).into()
}

/// Turns an `async fn` into a durable flow: an input struct named after the
/// function in PascalCase plus `DurableWorkflow` and `DurableFlow`
/// implementations that replay the body over the persisted step journal.
#[proc_macro_attribute]
pub fn durable_flow(metadata: TokenStream, item: TokenStream) -> TokenStream {
    let metadata = parse_macro_input!(metadata as flow::FlowMetadata);
    let function = parse_macro_input!(item as syn::ItemFn);
    flow::expand_flow(metadata, function).into()
}
