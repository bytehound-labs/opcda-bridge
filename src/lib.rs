pub const DEFAULT_BRIDGE_PORT: u16 = 7600;

#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    #[error("config error: {0}")]
    Config(String),
    #[error("connection error: {0}")]
    Connection(String),
    #[error("server error: {0}")]
    Server(String),
    #[error("tag error: {0}")]
    Tag(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type BridgeResult<T> = Result<T, BridgeError>;

/// Resolve the bridge port from the environment or default.
pub fn resolve_port() -> u16 {
    std::env::var("OPC_BRIDGE_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_BRIDGE_PORT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_bridge_port() {
        assert_eq!(DEFAULT_BRIDGE_PORT, 7600);
    }

    #[test]
    fn test_bridge_error_config_display() {
        let err = BridgeError::Config("bad config".into());
        assert_eq!(err.to_string(), "config error: bad config");
    }

    #[test]
    fn test_bridge_error_connection_display() {
        let err = BridgeError::Connection("timeout".into());
        assert_eq!(err.to_string(), "connection error: timeout");
    }

    #[test]
    fn test_bridge_error_server_display() {
        let err = BridgeError::Server("500".into());
        assert_eq!(err.to_string(), "server error: 500");
    }

    #[test]
    fn test_bridge_error_tag_display() {
        let err = BridgeError::Tag("not found".into());
        assert_eq!(err.to_string(), "tag error: not found");
    }

    #[test]
    fn test_bridge_error_io_display() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err = BridgeError::from(io_err);
        assert!(err.to_string().contains("io error"));
        assert!(err.to_string().contains("file missing"));
    }

    #[test]
    fn test_bridge_error_io_from() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let err: BridgeError = io_err.into();
        assert!(matches!(err, BridgeError::Io(_)));
    }

    #[test]
    fn test_bridge_error_debug() {
        let err = BridgeError::Config("msg".into());
        let debug_str = format!("{err:?}");
        assert!(debug_str.contains("Config"));
        assert!(debug_str.contains("msg"));
    }

    #[test]
    fn test_bridge_result_ok() {
        let result: BridgeResult<i32> = Ok(42);
        assert!(matches!(result, Ok(42)));
    }

    #[test]
    fn test_bridge_result_err() {
        let result: BridgeResult<i32> = Err(BridgeError::Tag("missing".into()));
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_port_default() {
        unsafe { std::env::remove_var("OPC_BRIDGE_PORT") };
        let port = resolve_port();
        assert_eq!(port, DEFAULT_BRIDGE_PORT);
    }

    #[test]
    fn test_resolve_port_from_env() {
        unsafe { std::env::set_var("OPC_BRIDGE_PORT", "9999") };
        let port = resolve_port();
        assert_eq!(port, 9999);
        unsafe { std::env::remove_var("OPC_BRIDGE_PORT") };
    }

    #[test]
    fn test_resolve_port_invalid_env() {
        unsafe { std::env::set_var("OPC_BRIDGE_PORT", "not_a_number") };
        let port = resolve_port();
        assert_eq!(port, DEFAULT_BRIDGE_PORT);
        unsafe { std::env::remove_var("OPC_BRIDGE_PORT") };
    }
}
