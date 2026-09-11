#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("transport error: {0}")]
    Transport(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("engine error: {0}")]
    Engine(String),
    #[error("serialization error: {0}")]
    Serde(String),
    #[error(
        "protocol version mismatch: received {received}, supported range {oldest_supported} through {newest_supported} — upgrade the older node to a contextdb release inside this sync window"
    )]
    ProtocolVersionMismatch {
        received: u8,
        oldest_supported: u8,
        newest_supported: u8,
    },
    #[error("chunk reassembly error: {0}")]
    ChunkError(String),
}
