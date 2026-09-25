use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse::Parse, parse::ParseStream, Error, FnArg, GenericArgument, Ident, ItemFn, LitInt, LitStr,
    Pat, PathArguments, Result, ReturnType, Token, Type,
};

use crate::definition::is_lower_snake_case;

pub(crate) struct FlowMetadata {
    kind: LitStr,
    version: i32,
}

impl Parse for FlowMetadata {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut kind = None;
        let mut version = None;
        while !input.is_empty() {
            let field: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            match field.to_string().as_str() {
                "kind" if kind.is_none() => kind = Some(input.parse::<LitStr>()?),
                "version" if version.is_none() => {
                    version = Some(input.parse::<LitInt>()?.base10_parse::<i32>()?)
                }
                "kind" | "version" => {
                    return Err(Error::new_spanned(field, "duplicate flow metadata"))
                }
                _ => return Err(Error::new_spanned(field, "unsupported flow metadata")),
            }
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }
        let kind = kind.ok_or_else(|| Error::new(input.span(), "missing `kind` metadata"))?;
        if !is_lower_snake_case(&kind.value()) {
            return Err(Error::new_spanned(
                &kind,
                "flow kind must be lower snake_case",
            ));
        }
        let version =
            version.ok_or_else(|| Error::new(input.span(), "missing `version` metadata"))?;
        if version <= 0 {
            return Err(Error::new_spanned(
                &kind,
                "flow version must be greater than zero",
            ));
        }
        Ok(Self { kind, version })
    }
}

pub(crate) fn expand_flow(metadata: FlowMetadata, function: ItemFn) -> TokenStream {
    match try_expand_flow(metadata, function) {
        Ok(tokens) => tokens,
        Err(error) => error.to_compile_error(),
    }
}

fn try_expand_flow(metadata: FlowMetadata, function: ItemFn) -> Result<TokenStream> {
    if function.sig.asyncness.is_none() {
        return Err(Error::new_spanned(
            &function.sig,
            "a durable flow must be an `async fn`",
        ));
    }
    if !function.sig.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &function.sig.generics,
            "a durable flow cannot have generic parameters",
        ));
    }

    let mut inputs = function.sig.inputs.iter();
    let Some(FnArg::Typed(ctx_arg)) = inputs.next() else {
        return Err(Error::new_spanned(
            &function.sig,
            "a durable flow's first parameter must be `ctx: &mut WfCtx<'_, Context>`",
        ));
    };
    let context_type = wfctx_context_type(&ctx_arg.ty).ok_or_else(|| {
        Error::new_spanned(
            &ctx_arg.ty,
            "a durable flow's first parameter must be `ctx: &mut WfCtx<'_, Context>`",
        )
    })?;
    let ctx_pat = &ctx_arg.pat;
    let ctx_type = &ctx_arg.ty;

    let mut field_names = Vec::new();
    let mut field_definitions = Vec::new();
    for input in inputs {
        let FnArg::Typed(argument) = input else {
            return Err(Error::new_spanned(input, "unsupported flow parameter"));
        };
        let Pat::Ident(pattern) = argument.pat.as_ref() else {
            return Err(Error::new_spanned(
                &argument.pat,
                "flow parameters must be plain identifiers",
            ));
        };
        let name = &pattern.ident;
        let ty = &argument.ty;
        let field_attrs = &argument.attrs;
        field_names.push(name.clone());
        field_definitions.push(quote! {
            #(#field_attrs)*
            pub #name: #ty
        });
    }

    let output_type = result_output_type(&function.sig.output).ok_or_else(|| {
        Error::new_spanned(
            &function.sig.output,
            "a durable flow must return `Result<Output, WfError>`",
        )
    })?;

    let kind = &metadata.kind;
    let version = metadata.version;
    let struct_name = format_ident!("{}", pascal_case(&function.sig.ident.to_string()));
    let visibility = &function.vis;
    let attributes = &function.attrs;
    let body = &function.block;
    let return_type = &function.sig.output;

    let bindings = if field_names.is_empty() {
        quote! {}
    } else {
        quote! {
            let Self { #(#field_names),* } = ::core::clone::Clone::clone(self);
        }
    };

    Ok(quote! {
        #(#attributes)*
        #[derive(Debug, Clone, durable_workflows::serde::Serialize, durable_workflows::serde::Deserialize)]
        #[serde(crate = "durable_workflows::serde")]
        #visibility struct #struct_name {
            #(#field_definitions),*
        }

        impl durable_workflows::DurableWorkflow for #struct_name {
            const KIND: &'static str = #kind;
            const VERSION: i32 = #version;
        }

        #[durable_workflows::async_trait::async_trait]
        impl durable_workflows::DurableFlow for #struct_name {
            type Context = #context_type;
            type Output = #output_type;

            async fn run(&self, #ctx_pat: #ctx_type) #return_type {
                #bindings
                #body
            }
        }
    })
}

fn wfctx_context_type(ty: &Type) -> Option<&Type> {
    let Type::Reference(reference) = ty else {
        return None;
    };
    reference.mutability?;
    let Type::Path(path) = reference.elem.as_ref() else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "WfCtx" {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    arguments.args.iter().find_map(|argument| match argument {
        GenericArgument::Type(context) => Some(context),
        _ => None,
    })
}

fn result_output_type(output: &ReturnType) -> Option<&Type> {
    let ReturnType::Type(_, ty) = output else {
        return None;
    };
    let Type::Path(path) = ty.as_ref() else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "Result" {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    arguments.args.iter().find_map(|argument| match argument {
        GenericArgument::Type(success) => Some(success),
        _ => None,
    })
}

fn pascal_case(value: &str) -> String {
    value
        .split('_')
        .filter(|segment| !segment.is_empty())
        .map(|segment| {
            let mut characters = segment.chars();
            match characters.next() {
                Some(first) => first.to_ascii_uppercase().to_string() + characters.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{expand_flow, pascal_case, FlowMetadata};

    #[test]
    fn pascal_case_converts_snake_case_function_names() {
        assert_eq!(pascal_case("lab_order_release_v2"), "LabOrderReleaseV2");
        assert_eq!(pascal_case("release_lab"), "ReleaseLab");
        assert_eq!(pascal_case("single"), "Single");
    }

    #[test]
    fn copies_parameter_attributes_onto_generated_fields() {
        let metadata: FlowMetadata =
            syn::parse_str(r#"kind = "example_workflow", version = 1"#).expect("flow metadata");
        let function: syn::ItemFn = syn::parse_quote! {
            async fn example_workflow(
                ctx: &mut WfCtx<'_, ()>,
                operation_key: String,
                #[serde(default = "default_refund_payment")]
                refund_payment: bool,
            ) -> Result<(), WfError> {
                let _ = (ctx, operation_key, refund_payment);
                Ok(())
            }
        };
        let rendered = expand_flow(metadata, function).to_string();
        assert!(
            rendered.contains("default_refund_payment"),
            "generated struct should preserve parameter serde defaults: {rendered}"
        );
        assert!(rendered.contains("refund_payment"));
    }
}
