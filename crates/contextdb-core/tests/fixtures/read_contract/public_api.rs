use contextdb_core::read_contract::{
    ChannelAddress, DatabaseIdentity, DeadlineClock, DeadlineWait, LocalUserIdentity,
    OwnerReadCancellation, OwnerRequestHandler, ProcessStartIdentity, ReadFailure,
    ReadFailureDetail, ReadFailureKind, ReaderBreadcrumb, ReaderBreadcrumbLocation,
    WriterRunNumber,
};
use contextdb_core::{Error, Result};

struct ManualClock;

impl DeadlineClock for ManualClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(std::future::ready(()))
    }
}

struct Handler;

impl OwnerRequestHandler for Handler {
    fn handle(
        &self,
        _namespace: &str,
        request: &[u8],
        _cancel: &OwnerReadCancellation,
    ) -> Result<Vec<u8>> {
        if request.is_empty() {
            Err(Error::Other("custom owner handler failed".to_owned()))
        } else {
            Ok(request.to_vec())
        }
    }
}

fn takes_database(_: DatabaseIdentity) {}
fn takes_run(_: WriterRunNumber) {}
fn takes_user(_: LocalUserIdentity) {}
fn takes_address(_: ChannelAddress) {}
fn accepts_cross_thread_clock<T: DeadlineClock + Send + Sync + 'static>(_: T) {}

fn main() {
    takes_database(DatabaseIdentity([1; 16]));
    takes_run(WriterRunNumber([2; 16]));
    takes_user(LocalUserIdentity(3));
    takes_address(ChannelAddress([4; 32]));
    let first_hash = ChannelAddress([5; 32]);
    let second_hash = ChannelAddress([6; 32]);
    let first_location = ReaderBreadcrumbLocation::for_database_path_hash(first_hash);
    let second_location = ReaderBreadcrumbLocation::for_database_path_hash(second_hash);
    assert_eq!(first_location.0, first_hash);
    assert_eq!(second_location.0, second_hash);
    assert_ne!(first_location, second_location);
    accepts_cross_thread_clock(ManualClock);
    let clock: &dyn DeadlineClock = &ManualClock;
    let _wait = clock.wait_until(0);

    let _breadcrumb = ReaderBreadcrumb {
        process_id: 6,
        process_name: "contextdb".to_owned(),
        process_start: ProcessStartIdentity(7),
    };
    let _failure = ReadFailure::new(
        ReadFailureKind::OwnerDisconnected,
        ReadFailureDetail::None,
    )
    .expect("construct an ordinary read failure");
    let handler = Handler;
    let cancellation = OwnerReadCancellation::new();
    let _general_error = handler.handle("diagnostics", b"", &cancellation);
}
