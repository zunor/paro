// Copyright 2024-2026 Zunor
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

macro_rules! try_dispatch {
    ($input:expr_2021, $return_if_ok:literal, $($pat:pat => $body:expr_2021),+ $(,)?) => {{
        if let Some(token_0) = $input.tokens.first() {
            use TokenKind::*;

            if let Some(result) = match token_0.kind {
                $($pat => Some($body),)+
                _ => None,
            } {
                if !$return_if_ok || result.is_ok() {
                    return result;
                }
            }
        }
    }};
}

pub(crate) mod common;
pub(crate) mod entry;
pub(crate) mod error;
pub(crate) mod error_suggestion;
pub(crate) mod expr;
pub(crate) mod graph_pattern;
pub(crate) mod input;
pub(crate) mod query;
// Script grammar is currently validated through the internal-testing facade.
#[allow(dead_code)]
pub(crate) mod script;
pub(crate) mod shared;
pub(crate) mod statement;
pub(crate) mod token;

pub(crate) use error::Backtrace;
pub(crate) use error::Error;
pub(crate) use error::ErrorKind;
pub(crate) use input::default_ident_quote;
pub(crate) use input::Input;
