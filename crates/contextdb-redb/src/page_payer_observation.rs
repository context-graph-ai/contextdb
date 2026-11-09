//! Test-only scalar observations of the actual page payer; no funding or allocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Reserved,
    BeforeAllocation,
    Allocated,
    BeforeIo,
    Handoff,
    BeforeFree,
    AfterFree,
    BeforeRefund,
    AfterRefund,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Payer {
    Missing,
    Unscoped,
    Source,
    SourceCounted,
    Cache,
}
#[derive(Clone, Copy, Debug)]
pub struct Observation {
    pub phase: Phase,
    pub pointer: usize,
    pub bytes: usize,
    pub alignment: usize,
    pub payer: Payer,
    pub counter_identity: usize,
    pub counter_value: usize,
    pub occupancy: usize,
    pub fixed_occupancy: usize,
    pub reserve_identity: usize,
    pub fixed: bool,
}
thread_local! {
    static OBSERVER: std::cell::Cell<Option<fn(Observation)>> = const { std::cell::Cell::new(None) };
}
pub fn observe<T>(observer: fn(Observation), action: impl FnOnce() -> T) -> T {
    struct Restore(Option<fn(Observation)>);
    impl Drop for Restore {
        fn drop(&mut self) {
            OBSERVER.with(|slot| slot.set(self.0));
        }
    }
    let _restore = Restore(OBSERVER.with(|slot| slot.replace(Some(observer))));
    action()
}
pub(crate) fn emit(observation: Observation) {
    if let Ok(Some(observer)) = OBSERVER.try_with(std::cell::Cell::get) {
        observer(observation);
    }
}
