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
        "protocol version mismatch: received {received}, supported {supported} — upgrade both ends to the same contextdb release so they speak the same sync protocol"
    )]
    ProtocolVersionMismatch { received: u8, supported: u8 },
    #[error("chunk reassembly error: {0}")]
    ChunkError(String),
}
