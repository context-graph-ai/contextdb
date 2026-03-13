#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("NATS error: {0}")]
    Nats(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("engine error: {0}")]
    Engine(String),
    #[error("serialization error: {0}")]
    Serde(String),
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),
    #[error("chunk reassembly error: {0}")]
    ChunkError(String),
}
