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

pub(crate) mod acl;
pub(crate) mod comment;
pub(crate) mod copy;
pub(crate) mod data_mask;
pub(crate) mod ddl;
pub(crate) mod dispatch;
pub(crate) mod dml;
pub(crate) mod dynamic_table;
pub(crate) mod explain;
pub(crate) mod helpers;
pub(crate) mod sequence;
pub(crate) mod session;
pub(crate) mod show;
pub(crate) mod stage;
pub(crate) mod stream;
pub(crate) mod transaction;
pub(crate) mod utility;

pub(crate) use dispatch::*;
