use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{parse_macro_input, Data, DataEnum, DeriveInput, Error, Fields, Ident, LitStr};

#[proc_macro_derive(Message)]
pub fn message_macro_derive(input: TokenStream) -> TokenStream {
    let derive_input = parse_macro_input!(input as DeriveInput);
    match derive_input.data {
        Data::Enum(data_enum) => impl_labeled_variants(derive_input.ident, data_enum),
        _ => derive_error!("LabeledVariants can only be applied to enums!"),
    }
    todo!()
}
