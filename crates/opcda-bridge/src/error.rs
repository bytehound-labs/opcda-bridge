//! The error type returned by every [`crate::Client`] method.

/// Errors that can occur while talking to an opcda-bridge gateway.
///
/// The transport variants use `#[error(transparent)]`: `Display` and `source()`
/// forward straight through to the wrapped `tonic` error with no added
/// prefix or extra chain link. This matters because `opcda-bridge-client`'s
/// CLI commands convert this type into an `anyhow::Error` with a bare `?`
/// (the same way they converted a raw `tonic::Status`/
/// `tonic::transport::Error` before this crate existed); transparency keeps
/// that conversion rendering byte-for-byte the same error text the CLI
/// printed before this crate existed, which is part of this crate's
/// contract with its CLI consumer and is pinned down by
/// `test_error_rpc_anyhow_debug_matches_bare_status` and
/// `client::tests::test_connect_failure_anyhow_debug_matches_bare_transport_error`.
///
/// A dedicated `thiserror` enum (rather than reusing `anyhow::Error` here
/// the way `opcda-bridge-client`'s own commands do) still lets a downstream
/// consumer that does *not* want an `anyhow` dependency match on
/// [`Error::Connect`] / [`Error::Rpc`] directly. [`Error::Protocol`] reports
/// malformed or internally inconsistent responses from an incompatible gateway.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to establish the gRPC channel to the gateway (e.g. connection
    /// refused, DNS failure, invalid address).
    #[error(transparent)]
    Connect(#[from] tonic::transport::Error),
    /// The gateway returned a gRPC error for a `GetCapabilities`/`Browse`/
    /// `CloseBrowseSession`/`Search`/`ListServers`/`Read`/`Write` call, or
    /// for a search response-stream item.
    #[error(transparent)]
    Rpc(#[from] tonic::Status),
    /// The connected gateway predates a required RPC.
    #[error(
        "gateway does not support {operation}; upgrade the gateway and client to compatible protocol versions"
    )]
    IncompatibleGateway { operation: &'static str },
    /// The gateway returned a response that violates the negotiated protocol.
    #[error("protocol error: {0}")]
    Protocol(String),
}

/// A `Result` alias using [`Error`], mirroring the ergonomics of
/// `anyhow::Result` (`opcda_bridge::Result<T>`) for a crate that
/// intentionally does not depend on `anyhow` itself.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_rpc_display_is_transparent() {
        let status = tonic::Status::internal("boom");
        let expected = status.to_string();
        let err: Error = status.into();
        assert_eq!(err.to_string(), expected);
    }

    #[test]
    fn test_error_rpc_matches_variant() {
        let status = tonic::Status::not_found("missing");
        let err: Error = status.into();
        assert!(matches!(err, Error::Rpc(_)));
    }

    #[test]
    fn test_error_rpc_debug_matches_variant_and_status() {
        let status = tonic::Status::internal("boom");
        let err: Error = status.clone().into();
        assert_eq!(format!("{err:?}"), format!("Rpc({status:?})"));
    }

    #[test]
    fn test_error_rpc_anyhow_debug_matches_bare_status() {
        // `opcda-bridge-client`'s commands convert this crate's `Error` into
        // `anyhow::Error` via a bare `?`; this must render identically to
        // today's direct `tonic::Status` -> `anyhow::Error` conversion, or
        // the CLI's printed error text would silently change.
        let status = tonic::Status::internal("boom");
        let bare = anyhow::Error::from(status.clone());
        let wrapped = anyhow::Error::from(Error::from(status));
        assert_eq!(format!("{bare:?}"), format!("{wrapped:?}"));
        assert_eq!(bare.to_string(), wrapped.to_string());
    }

    #[test]
    fn test_protocol_error_is_actionable() {
        let err = Error::Protocol("unknown browse node kind".into());
        assert_eq!(err.to_string(), "protocol error: unknown browse node kind");
    }

    #[test]
    fn test_incompatible_gateway_error_is_actionable() {
        let err = Error::IncompatibleGateway {
            operation: "paged browse",
        };
        assert!(err.to_string().contains("upgrade the gateway and client"));
    }

    // `Error::Connect`'s transparency (both the plain and anyhow-wrapped
    // rendering) is exercised in `client::tests`, since a real
    // `tonic::transport::Error` can only be produced by an actual failed
    // connection attempt, not constructed directly.
}
