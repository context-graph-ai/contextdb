//! Scalar-refusal adapters over custody's shared finite canonical walkers.
use super::DecodeError;
use crate::custody::canonical as shared;
use crate::custody::{authority::Birth, records::*};
pub(super) use shared::FiniteEncoder as Encoder;

fn size_failure() -> DecodeError {
    DecodeError::Malformed("canonical size overflow or changed")
}
pub(super) fn count(
    walk: impl FnOnce(&mut Encoder<'_>) -> Result<(), DecodeError>,
) -> Result<usize, DecodeError> {
    shared::finite_len_with_error(walk, size_failure)
}
pub(super) fn emit(
    bytes: &mut [u8],
    walk: impl FnOnce(&mut Encoder<'_>) -> Result<(), DecodeError>,
) -> Result<(), DecodeError> {
    shared::finite_emit_with_error(bytes, walk, size_failure)
}
pub(super) fn hash(
    walk: impl FnOnce(&mut Encoder<'_>) -> Result<(), DecodeError>,
) -> Result<Digest, DecodeError> {
    shared::finite_hash_with_error(walk, size_failure)
}
pub(super) fn node_bytes(node: &str) -> Result<[u8; 32], DecodeError> {
    shared::node_bytes_with_error(node, || DecodeError::Malformed("invalid fabric node"))
}
pub(super) fn certificate(
    e: &mut Encoder<'_>,
    ns: &Namespace,
    state: &StateRecord,
    birth: &Birth,
    retirement: &Digest,
) -> Result<(), DecodeError> {
    // encode_packet validates the node before entering a core-error walker.
    crate::custody::authority::certificate_bytes_finite(
        e,
        1,
        &ns.tenant,
        &ns.hub_node,
        state,
        birth,
        retirement,
    )
    .map_err(|_| DecodeError::Malformed("invalid canonical certificate"))
}
pub(super) fn terminal(e: &mut Encoder<'_>, record: &TerminalRecord) -> Result<(), DecodeError> {
    // Both namespace nodes were scalar-validated by encode_packet.
    record
        .bytes_finite(e)
        .map_err(|_| DecodeError::Malformed("invalid canonical terminal"))
}
pub(super) fn diagnostic(e: &mut Encoder<'_>, block: &DiagnosticBlock) -> Result<(), DecodeError> {
    block
        .bytes_finite(e)
        .map_err(|_| DecodeError::Malformed("invalid canonical diagnostics"))
}
