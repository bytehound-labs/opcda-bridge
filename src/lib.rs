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
