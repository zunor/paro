//! BLOB scalar functions

pub mod create_sort_key;
#[cfg(test)]
mod tests;

pub use create_sort_key::{
    encode_sort_key, encode_sort_key_into, get_create_sort_key_function, OrderModifiers,
};
