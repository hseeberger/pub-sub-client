use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Error, Ident};

#[proc_macro_derive(PublishedMessage)]
pub fn message_macro_derive(input: TokenStream) -> TokenStream {
    let derive_input = parse_macro_input!(input as DeriveInput);

    match derive_input.data {
        Data::Union(_) => no_union(),
        _ => impl_published_message(derive_input.ident),
    }
}

fn no_union() -> TokenStream {
    let e = Error::new(
        Span::call_site(),
        "pub-sub-client and Serde do not support derive for unions",
    );
    let e = e.to_compile_error();
    quote!(#e).into()
}

fn impl_published_message(name: Ident) -> TokenStream {
    quote!(impl PublishedMessage for #name {}).into()
}
