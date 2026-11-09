use super::*;

#[test]
fn native_storage_error_is_io_while_decode_error_is_malformed() {
    let native = RedbPersistence::storage_error(redb::Error::ValueTooLarge(9));
    let storage = read_storage_error(redb::StorageError::AdmissionRefused(Box::new(native)));
    assert_eq!(storage.kind(), LoadReadImageErrorKind::Io);
    let decode = read_image_error(Error::Other("invalid bincode record".into()));
    assert_eq!(
        decode.kind(),
        LoadReadImageErrorKind::Corrupt(ReadImageCorruptionCause::MalformedRecord)
    );
}
