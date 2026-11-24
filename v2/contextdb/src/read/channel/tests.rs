use super::*;
#[test]
fn a_refusal_is_read_even_when_the_owner_has_stopped_reading_requests() {
    let limits = ReadLimits::default();
    let budget = Budget::new(None).unwrap();
    for status in [Some(1), Some(3), None] {
        let (client, mut owner) = UnixStream::pair().unwrap();
        if let Some(status) = status {
            owner.write_all(&[status, 0, 0, 0, 0]).unwrap();
        }
        drop(owner); // All request writes now fail; any buffered status remains readable.
        let error = exchange(client, "opaque", b"request", &limits, &budget).unwrap_err();
        assert!(matches!(
            (status, error),
            (Some(1), Error::OwnerNotServing)
                | (Some(3), Error::OwnerLimitExceeded)
                | (None, Error::OwnerDisconnected)
        ));
    }
}
