//! Allocation-free terminal serialization, borrowing the original producer.
//! The caller retains its actual connection or query custody across the write.
use super::{FrameViolation, LocalTransportError, MAX_FRAME_BYTES, invalid_channel_data,
    owner_disconnected};
use std::io::Write;

struct Count(usize);
impl Write for Count {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.checked_add(bytes.len()).ok_or(std::io::ErrorKind::InvalidData)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}
fn field<T: serde::Serialize + ?Sized>(value: &T, mut out: &mut dyn Write)
    -> Result<usize, LocalTransportError>
{
    bincode::serde::encode_into_std_write(value, &mut out, bincode::config::standard())
        .map_err(|_| invalid_channel_data())
}

fn write_frame(
    output: &mut impl Write,
    body: impl Fn(&mut dyn Write) -> Result<(), LocalTransportError>,
) -> Result<(), LocalTransportError> {
    let mut count = Count(0);
    body(&mut count)?;
    if count.0 > MAX_FRAME_BYTES {
        return Err(LocalTransportError::Frame(FrameViolation::LengthExceedsMaximum));
    }
    output.write_all(&(count.0 as u32).to_be_bytes()).map_err(|_| owner_disconnected())?;
    body(output).map_err(|_| owner_disconnected())
}

pub(crate) fn write_query_refusal(
    output: &mut impl Write, failure: &contextdb_core::read_contract::ReadFailure,
) -> Result<(), LocalTransportError> {
    // An enum's standard-bincode tag followed by its sole field is exactly
    // LocalResponse::Failure; the existing public DTO remains unchanged.
    write_frame(output, |out| { field(&(9_u32, failure), out)?; Ok(()) })
}

fn write_document(
    output: &mut impl Write,
    document: impl Fn(&mut dyn Write) -> Result<(), LocalTransportError>,
) -> Result<(), LocalTransportError> {
    let mut count = Count(0);
    document(&mut count)?;
    write_frame(output, |out| {
        // LocalResponse::EngineFailure { LocalEngineFailure { document } }.
        field(&(10_u32, count.0 as u64), out)?;
        document(out)
    })
}

pub(crate) fn write_query_error(
    output: &mut impl Write, error: &contextdb_core::Error,
) -> Result<(), LocalTransportError> {
    write_document(output, |mut out| {
        super::engine_answer::write_read_error(error, &mut out).map(|_| ())
    })
}

pub(crate) fn write_query_diagnostic(
    output: &mut impl Write, value: &impl std::fmt::Display,
) -> Result<(), LocalTransportError> {
    struct Text<'a>(&'a mut dyn Write);
    impl std::fmt::Write for Text<'_> {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            self.0.write_all(text.as_bytes()).map_err(|_| std::fmt::Error)
        }
    }
    let mut count = Count(0);
    std::fmt::write(&mut Text(&mut count), format_args!("{value}"))
        .map_err(|_| invalid_channel_data())?;
    write_document(output, |out| {
        field(&(super::engine_answer::TAG_OTHER, count.0 as u64), out)?;
        std::fmt::write(&mut Text(out), format_args!("{value}"))
            .map_err(|_| invalid_channel_data())
    })
}
