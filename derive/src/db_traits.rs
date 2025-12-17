use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use quote::{quote, ToTokens};
use syn::{spanned::Spanned, Attribute, Data, DataStruct, DeriveInput, Generics, LitStr};

use std::collections::HashSet;

#[derive(Debug)]
struct BinaryValueStruct {
    ident: Ident,
    attrs: BinaryValueAttrs,
}

impl BinaryValueStruct {
    fn new(input: &DeriveInput) -> syn::Result<Self> {
        let attrs = BinaryValueAttrs::new(&input.attrs)?;
        Ok(Self {
            ident: input.ident.clone(),
            attrs,
        })
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
enum Codec {
    #[default]
    Bincode,
}

impl Codec {
    fn from_string(value: &LitStr) -> syn::Result<Self> {
        let parsed = value.value();
        #[allow(clippy::single_match_else)] // better for forward compatibility
        match parsed.as_str() {
            "bincode" => Ok(Codec::Bincode),
            _ => {
                let msg = format!("Unknown codec ({parsed}). Use `bincode`");
                Err(syn::Error::new(value.span(), msg))
            }
        }
    }
}

#[derive(Debug, Default)]
struct BinaryValueAttrs {
    codec: Codec,
}

impl BinaryValueAttrs {
    fn new(attrs: &[Attribute]) -> syn::Result<Self> {
        let attrs = attrs.iter().filter(|attr| attr.path().is_ident("binary_value"));
        let mut codec = Codec::default();
        for attr in attrs {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("codec") {
                    let val: LitStr = meta.value()?.parse()?;
                    codec = Codec::from_string(&val)?;
                    Ok(())
                } else {
                    Err(meta.error("Unsupported attribute"))
                }
            })?;
        }
        Ok(Self { codec })
    }
}

impl BinaryValueStruct {
    fn implement_binary_value_from_bincode(&self) -> proc_macro2::TokenStream {
        let name = &self.ident;

        quote! {
            impl ::matterdb::BinaryValue for #name {
                fn to_bytes(&self) -> std::vec::Vec<u8> {
                    ::bincode::encode_to_vec(self, ::bincode::config::standard()).expect(
                        concat!("Failed to serialize `BinaryValue` for ", stringify!(#name))
                    )
                }

                fn from_bytes(
                    value: std::borrow::Cow<[u8]>,
                ) -> std::result::Result<Self, matterdb::_reexports::Error> {
                    let (parsed, read_bytes) = ::bincode::decode_from_slice(
                        value.as_ref(),
                        ::bincode::config::standard(),
                    )?;
                    if read_bytes < value.len() {
                        return Err(matterdb::_reexports::Error::msg(
                            format!("Extra bytes after parsing value: read {read_bytes}, total {} bytes", value.len()),
                        ));
                    }
                    Ok(parsed)
                }
            }
        }
    }

    fn implement_binary_value(&self) -> impl ToTokens {
        match self.attrs.codec {
            Codec::Bincode => self.implement_binary_value_from_bincode(),
        }
    }
}

impl ToTokens for BinaryValueStruct {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let mod_name = Ident::new(
            &format!("binary_value_impl_{}", self.ident),
            Span::call_site(),
        );

        let binary_value = self.implement_binary_value();
        let expanded = quote! {
            mod #mod_name {
                use super::*;
                #binary_value
            }
        };

        tokens.extend(expanded);
    }
}

pub(crate) fn impl_binary_value(input: TokenStream) -> TokenStream {
    let input: DeriveInput = syn::parse(input).unwrap();
    let db_object = BinaryValueStruct::new(&input)
        .unwrap_or_else(|e| panic!("BinaryValue: {e}"));
    let tokens = quote! { #db_object };
    tokens.into()
}

/// Checks that an ASCII character is allowed in the `IndexAddress` component.
pub(crate) fn is_allowed_component_char(c: u8) -> bool {
    matches!(c, b'0'..=b'9' | b'A'..=b'Z' | b'a'..=b'z' | b'-' | b'_')
}

fn validate_address_component(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("Name shouldn't be empty".to_owned());
    }
    if !name
        .as_bytes()
        .iter()
        .copied()
        .all(is_allowed_component_char)
    {
        return Err(format!(
            "Name `{name}` contains invalid chars (allowed: `A-Z`, `a-z`, `0-9`, `_` and `-`)"
        ));
    }
    Ok(())
}

struct FromAccess {
    ident: Ident,
    access_ident: Ident,
    fields: Vec<AccessField>,
    generics: Generics,
    attrs: FromAccessAttrs,
}

#[derive(Debug, Default)]
struct FromAccessAttrs {
    transparent: bool,
}

impl FromAccessAttrs {
    fn new(attrs: &[Attribute]) -> syn::Result<Self> {
        let attrs = attrs.iter().filter(|attr| attr.path().is_ident("from_access"));
        let mut transparent = false;
        for attr in attrs {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("transparent") {
                    transparent = true;
                    Ok(())
                } else {
                    Err(meta.error("Unsupported attribute"))
                }
            })?;
        }
        Ok(Self { transparent })
    }
}

#[derive(Debug, Default)]
struct FromAccessFieldAttrs {
    rename: Option<String>,
    flatten: bool,
}

impl FromAccessFieldAttrs {
    fn new(attrs: &[Attribute]) -> syn::Result<Self> {
        let attrs = attrs.iter().filter(|attr| attr.path().is_ident("from_access"));

        let mut rename = None;
        let mut flatten = false;
        for attr in attrs {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("rename") {
                    let val: LitStr = meta.value()?.parse()?;
                    rename = Some(val.value());
                    Ok(())
                } else if meta.path.is_ident("flatten") {
                    flatten = true;
                    Ok(())
                } else {
                    Err(meta.error("Unsupported attribute"))
                }
            })?;
        }
        Ok(Self { rename, flatten })
    }
}

impl FromAccess {
    fn extract_access_ident(generics: &Generics) -> syn::Result<&Ident> {
        use syn::{TraitBound, TypeParamBound};

        for type_param in generics.type_params() {
            if type_param
                .attrs
                .iter()
                .any(|attr| attr.path().is_ident("from_access"))
            {
                return Ok(&type_param.ident);
            }
        }

        for type_param in generics.type_params() {
            for bound in &type_param.bounds {
                if let TypeParamBound::Trait(TraitBound { path, .. }) = bound {
                    if path.is_ident("Access") {
                        return Ok(&type_param.ident);
                    }
                }
            }
        }

        // No type params with the overt attribute or `T: Access` constraint.
        let mut params = generics.type_params();
        let type_param = params.next().ok_or_else(|| {
            syn::Error::new(generics.span(), "`FromAccess` struct should be generic over `Access` type")
        })?;
        if params.next().is_some() {
            let msg = "Cannot find type param implementing `Access` trait. \
                       You may mark it explicitly with `#[from_access]`";
            let e = syn::Error::new(generics.span(), msg);
            Err(e)
        } else {
            // If there is a single type param, we hope it's the correct one.
            Ok(&type_param.ident)
        }
    }
}

impl FromAccess {
    fn from_derive_input(input: &DeriveInput) -> syn::Result<Self> {
        let attrs = FromAccessAttrs::new(&input.attrs)?;
        match &input.data {
            Data::Struct(DataStruct { fields, .. }) => {
                let this = Self {
                    ident: input.ident.clone(),
                    access_ident: Self::extract_access_ident(&input.generics)?.clone(),
                    generics: input.generics.clone(),
                    fields: fields.iter().map(AccessField::new).collect::<syn::Result<_>>()?,
                    attrs,
                };

                if this.attrs.transparent {
                    if this.fields.len() != 1 {
                        let e = syn::Error::new(
                            input.ident.span(),
                            "Transparent struct must contain a single field",
                        );
                        return Err(e);
                    }
                } else {
                    let mut field_names = HashSet::new();

                    for field in &this.fields {
                        if let Some(ref name) = field.name_suffix {
                            validate_address_component(name).map_err(|msg| {
                                syn::Error::new(field.span, msg)
                            })?;
                            if !field_names.insert(name) {
                                let e = "Duplicate field name";
                                return Err(syn::Error::new(field.span, e));
                            }
                        } else if !field.flatten {
                            let msg = if this.fields.len() == 1 {
                                "Unnamed fields necessitate #[from_access(rename = ...)]. \
                                 To use a wrapper, add #[from_access(transparent)] to the struct"
                            } else {
                                "Unnamed fields necessitate #[from_access(rename = ...)]"
                            };
                            return Err(syn::Error::new(field.span, msg));
                        }
                    }
                }
                Ok(this)
            }
            _ => Err(syn::Error::new_spanned(
                input,
                "`FromAccess` can be only implemented for structs",
            )),
        }
    }
}

#[derive(Debug)]
struct AccessField {
    span: Span,
    ident: Option<Ident>,
    name_suffix: Option<String>,
    flatten: bool,
}

impl AccessField {
    fn new(field: &syn::Field) -> syn::Result<Self> {
        let ident = field.ident.clone();
        let attrs = FromAccessFieldAttrs::new(&field.attrs)?;
        let name_suffix = attrs
            .rename
            .or_else(|| ident.as_ref().map(ToString::to_string));
        Ok(Self {
            ident,
            name_suffix,
            span: field.span(),
            flatten: attrs.flatten,
        })
    }
}

impl AccessField {
    fn ident(&self, field_index: usize) -> impl ToTokens {
        if let Some(ref ident) = self.ident {
            quote!(#ident)
        } else {
            let field_index = syn::Index::from(field_index);
            quote!(#field_index)
        }
    }

    fn constructor(&self, field_index: usize) -> impl ToTokens {
        let from_access = quote!(matterdb::access::FromAccess);
        let ident = self.ident(field_index);
        if self.flatten {
            quote!(#ident: #from_access::from_access(access.clone(), addr.clone())?)
        } else {
            let name = self.name_suffix.as_ref().unwrap();
            quote!(#ident: #from_access::from_access(access.clone(), addr.clone().append_name(#name))?)
        }
    }

    fn root_constructor(&self, field_index: usize) -> impl ToTokens {
        let from_access = quote!(matterdb::access::FromAccess);
        let ident = self.ident(field_index);
        if self.flatten {
            quote!(#ident: #from_access::from_root(access.clone())?)
        } else {
            let name = &self.name_suffix;
            quote!(#ident: #from_access::from_access(access.clone(), #name.into())?)
        }
    }
}

impl FromAccess {
    fn access_fn(&self) -> impl ToTokens {
        let fn_impl = if self.attrs.transparent {
            let from_access = quote!(matterdb::access::FromAccess);
            let ident = self.fields[0].ident(0);
            quote!(Ok(Self { #ident: #from_access::from_access(access, addr)? }))
        } else {
            let field_constructors = self
                .fields
                .iter()
                .enumerate()
                .map(|(i, field)| field.constructor(i));
            quote!(Ok(Self { #(#field_constructors,)* }))
        };

        let access_ident = &self.access_ident;
        quote! {
            fn from_access(
                access: #access_ident,
                addr: matterdb::IndexAddress,
            ) -> Result<Self, matterdb::access::AccessError> {
                #fn_impl
            }
        }
    }

    fn root_fn(&self) -> impl ToTokens {
        let fn_impl = if self.attrs.transparent {
            let from_access = quote!(matterdb::access::FromAccess);
            let ident = self.fields[0].ident(0);
            quote!(Ok(Self { #ident: #from_access::from_root(access)? }))
        } else {
            let field_constructors = self
                .fields
                .iter()
                .enumerate()
                .map(|(i, field)| field.root_constructor(i));
            quote!(Ok(Self { #(#field_constructors,)* }))
        };

        let access_ident = &self.access_ident;
        quote! {
            fn from_root(
                access: #access_ident,
            ) -> Result<Self, matterdb::access::AccessError> {
                #fn_impl
            }
        }
    }
}

impl ToTokens for FromAccess {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let name = &self.ident;
        let tr = quote!(matterdb::access::FromAccess);
        let access_ident = &self.access_ident;
        let (impl_generics, ty_generics, where_clause) = self.generics.split_for_impl();

        let from_access_fn = self.access_fn();
        let from_root_fn = self.root_fn();

        let expanded = quote! {
            impl #impl_generics #tr<#access_ident> for #name #ty_generics #where_clause {
                #from_access_fn
                #from_root_fn
            }
        };
        tokens.extend(expanded);
    }
}

pub(crate) fn impl_from_access(input: TokenStream) -> TokenStream {
    let input: DeriveInput = syn::parse(input).unwrap();
    let from_access = match FromAccess::from_derive_input(&input) {
        Ok(access) => access,
        Err(e) => return e.into_compile_error().into(),
    };
    let tokens = quote!(#from_access);
    tokens.into()
}
