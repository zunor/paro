// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL protocol-compatible network server.

mod client_connection;
mod command_line;
mod connection_control;
mod protocol;
mod server;

pub use command_line::CommandLineArgs;
pub use server::{Server, ServerShutdownReport};
