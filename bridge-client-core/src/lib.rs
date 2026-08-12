//! Reusable, presentation-free async client for the opcda-bridge gateway's
//! gRPC API.
//!
//! This crate is the typed connect/read/write/browse/list-servers surface
//! extracted from `opcda-bridge-client`'s `commands.rs`: no `clap`, no
//! `tabled`, no `serde_json`/`toml` — just [`Client`], the parameters its
//! methods take, and the plain result types they return ([`BrowseNode`],
//! [`TagValue`], [`WriteResult`], [`Value`]). `opcda-bridge-client` depends
//! on this crate and adds only CLI parsing and table/JSON rendering on top
//! of it; any other async Rust program that needs typed OPC DA
//! reads/writes/browses without shelling out to the CLI binary and parsing
//! its output can depend on this crate directly instead.
//!
//! ```no_run
//! # async fn example() -> bridge_client_core::Result<()> {
//! let mut client = bridge_client_core::Client::connect("localhost:7600").await?;
//! let servers = client.list_servers().await?;
//! let values = client
//!     .read(servers[0].clone(), vec!["Some.Tag".into()])
//!     .await?;
//! # let _ = values;
//! # Ok(())
//! # }
//! ```

mod client;
mod error;
mod types;

#[cfg(test)]
mod test_support;

pub use client::Client;
pub use error::{Error, Result};
pub use types::{BrowseNode, TagValue, Value, WriteResult, parse_value};
