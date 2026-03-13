use crate::protocol::ChunkMessage;

pub const CHUNK_SIZE: usize = 512 * 1024;

pub fn needs_chunking(data: &[u8]) -> bool {
    data.len() > CHUNK_SIZE
}

pub fn chunk(data: &[u8]) -> Vec<ChunkMessage> {
    let chunk_id = uuid::Uuid::new_v4();
    let total = data.len().div_ceil(CHUNK_SIZE);
    data.chunks(CHUNK_SIZE)
        .enumerate()
        .map(|(i, slice)| ChunkMessage {
            chunk_id,
            sequence: i as u32,
            total_chunks: total as u32,
            payload: slice.to_vec(),
        })
        .collect()
}

pub fn reassemble(chunks: &mut [ChunkMessage]) -> Vec<u8> {
    chunks.sort_by_key(|c| c.sequence);
    let mut bytes = Vec::new();
    for chunk in chunks {
        bytes.extend_from_slice(&chunk.payload);
    }
    bytes
}
