//! The journal record check T2 runs without decoding: every field of both
//! journal variants is parsed heap-free with exact consumption, so a record
//! that carries a valid checksum but a malformed body is refused exactly as
//! the full decoder refuses it.
use super::*;
