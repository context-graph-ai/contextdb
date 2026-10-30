//! Test-only correlation of native writes with the actual physical allocator.
use super::NativeIvfScopeRef;
use crate::persistence::vector_ivf::NativeIvfPhysicalDiskOwnerForTest;
use contextdb_core::{Error, Result};
use redb::allocation_observation::{AllocationObserverForTest, AllocationPlanForTest};
use std::{
    cell::RefCell,
    marker::PhantomData,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeIvfBuildWriteClassForTest {
    TrainingReservoir,
    BuildAccumulator,
    UnpublishedRun,
    PreparedReplacement,
}
#[derive(Clone, Copy, Debug)]
pub struct NativeIvfDiskBoundaryForTest {
    pub operation: u64,
    pub class: NativeIvfBuildWriteClassForTest,
    pub current_store_bytes: u64,
    pub minimum_growth_bytes: u64,
}
pub enum NativeIvfDiskEventForTest<'a> {
    Admission {
        scope: NativeIvfScopeRef<'a>,
        boundary: NativeIvfDiskBoundaryForTest,
        requested_bytes: u64,
        projected_store_bytes: u64,
        ceiling: Option<u64>,
        admitted: bool,
    },
    Written {
        physical_owner: usize,
        scope: NativeIvfScopeRef<'a>,
        operation: u64,
        class: NativeIvfBuildWriteClassForTest,
        bytes: u64,
    },
    Refused {
        physical_owner: usize,
        index: &'a contextdb_core::VectorIndexRef,
        tuple: &'a contextdb_core::VectorPartitionKey,
        error: &'a Error,
    },
}
pub type NativeIvfDiskObserverForTest =
    Arc<dyn for<'a> Fn(NativeIvfDiskEventForTest<'a>) + Send + Sync>;
type Chooser = Box<dyn FnOnce(&NativeIvfDiskBoundaryForTest) -> u64 + Send>;
struct Choice {
    owner: NativeIvfPhysicalDiskOwnerForTest,
    class: NativeIvfBuildWriteClassForTest,
    choose: Option<Chooser>,
    chosen: Option<u64>,
}
thread_local! {
    static OBSERVER: RefCell<Option<NativeIvfDiskObserverForTest>> = const {RefCell::new(None)};
    static WITHHELD: RefCell<Option<(NativeIvfPhysicalDiskOwnerForTest, NativeIvfBuildWriteClassForTest)>> = const {RefCell::new(None)};
    static CEILING: RefCell<Option<Arc<Mutex<Choice>>>> = const {RefCell::new(None)};
}
pub fn with_native_ivf_disk_observer_for_test<T>(
    observer: NativeIvfDiskObserverForTest,
    run: impl FnOnce() -> T,
) -> T {
    struct Restore(Option<NativeIvfDiskObserverForTest>);
    impl Drop for Restore {
        fn drop(&mut self) {
            OBSERVER.with(|slot| *slot.borrow_mut() = self.0.take());
        }
    }
    let _restore = Restore(OBSERVER.with(|slot| slot.replace(Some(observer))));
    run()
}
pub(crate) fn native_ivf_build_refused_for_test(
    physical_owner: usize,
    index: &contextdb_core::VectorIndexRef,
    tuple: &contextdb_core::VectorPartitionKey,
    error: &Error,
) {
    let sink = OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(sink) = sink {
        sink(NativeIvfDiskEventForTest::Refused {
            physical_owner,
            index,
            tuple,
            error,
        });
    }
}
pub struct NativeIvfReusableSpaceWithheldForTest {
    previous: Option<(
        NativeIvfPhysicalDiskOwnerForTest,
        NativeIvfBuildWriteClassForTest,
    )>,
    _thread: PhantomData<Rc<()>>,
}
impl Drop for NativeIvfReusableSpaceWithheldForTest {
    fn drop(&mut self) {
        WITHHELD.with(|slot| *slot.borrow_mut() = self.previous.take());
    }
}
pub fn withhold_native_ivf_reusable_space_for_test(
    owner: NativeIvfPhysicalDiskOwnerForTest,
    class: NativeIvfBuildWriteClassForTest,
) -> NativeIvfReusableSpaceWithheldForTest {
    NativeIvfReusableSpaceWithheldForTest {
        previous: WITHHELD.with(|slot| slot.replace(Some((owner, class)))),
        _thread: PhantomData,
    }
}
pub struct NativeIvfCeilingAtAdmissionForTest {
    state: Arc<Mutex<Choice>>,
    previous: Option<Arc<Mutex<Choice>>>,
    _thread: PhantomData<Rc<()>>,
}
impl NativeIvfCeilingAtAdmissionForTest {
    pub fn chosen(&self) -> Option<u64> {
        self.state.lock().expect("ceiling observation").chosen
    }
}
impl Drop for NativeIvfCeilingAtAdmissionForTest {
    fn drop(&mut self) {
        CEILING.with(|slot| *slot.borrow_mut() = self.previous.take());
    }
}
pub fn choose_native_ivf_ceiling_for_test(
    owner: NativeIvfPhysicalDiskOwnerForTest,
    class: NativeIvfBuildWriteClassForTest,
    choose: Chooser,
) -> NativeIvfCeilingAtAdmissionForTest {
    let state = Arc::new(Mutex::new(Choice {
        owner,
        class,
        choose: Some(choose),
        chosen: None,
    }));
    NativeIvfCeilingAtAdmissionForTest {
        previous: CEILING.with(|slot| slot.replace(Some(state.clone()))),
        state,
        _thread: PhantomData,
    }
}

struct AccessState {
    operation: u64,
    admitted: bool,
}
struct WriteObservation<'a> {
    owner: &'a NativeIvfPhysicalDiskOwnerForTest,
    scope: NativeIvfScopeRef<'a>,
    class: NativeIvfBuildWriteClassForTest,
    sink: Option<NativeIvfDiskObserverForTest>,
    state: Mutex<AccessState>,
}
static NEXT_ACCESS: AtomicU64 = AtomicU64::new(1);
impl WriteObservation<'_> {
    fn begin(&self, plan: AllocationPlanForTest) -> redb::Result {
        let operation = NEXT_ACCESS.fetch_add(1, Ordering::Relaxed);
        assert_ne!(operation, u64::MAX, "disk observation id exhausted");
        *self.state.lock().expect("disk observation") = AccessState {
            operation,
            admitted: false,
        };
        let choice = CEILING.with(|slot| slot.borrow().clone());
        if let Some(choice) = choice {
            let selected = {
                let mut choice = choice.lock().expect("ceiling observation");
                if choice.owner.identity() == self.owner.identity() && choice.class == self.class {
                    choice.choose.take().map(|choose| {
                        let ceiling = choose(&self.boundary(plan, operation));
                        choice.chosen = Some(ceiling);
                        ceiling
                    })
                } else {
                    None
                }
            };
            if let Some(ceiling) = selected {
                self.owner
                    .set_ceiling(Some(ceiling))
                    .map_err(redb::StorageError::Io)?;
            }
        }
        Ok(())
    }
    fn boundary(
        &self,
        plan: AllocationPlanForTest,
        operation: u64,
    ) -> NativeIvfDiskBoundaryForTest {
        NativeIvfDiskBoundaryForTest {
            operation,
            class: self.class,
            current_store_bytes: plan.current_store_bytes,
            minimum_growth_bytes: plan.minimum_growth_bytes,
        }
    }
    fn decision(&self, plan: AllocationPlanForTest, admitted: bool) -> redb::Result {
        let mut state = self.state.lock().expect("disk observation");
        if state.admitted {
            return Ok(());
        }
        if let Some(sink) = &self.sink {
            sink(NativeIvfDiskEventForTest::Admission {
                scope: self.scope,
                boundary: self.boundary(plan, state.operation),
                requested_bytes: plan.requested_bytes as u64,
                projected_store_bytes: if admitted {
                    self.owner.file_len().map_err(redb::StorageError::Io)?
                } else {
                    plan.current_store_bytes
                        .saturating_add(plan.minimum_growth_bytes)
                },
                ceiling: self.owner.ceiling(),
                admitted,
            });
        }
        state.admitted = admitted;
        Ok(())
    }
}
impl AllocationObserverForTest for WriteObservation<'_> {
    fn before_allocate(&self, plan: AllocationPlanForTest) -> redb::Result {
        self.begin(plan)
    }
    fn before_existing_write(&self, plan: AllocationPlanForTest) -> redb::Result {
        self.begin(plan)
    }
    fn allocated(&self, _plan: AllocationPlanForTest, _page: [u8; 8]) {}
    fn before_write(
        &self,
        plan: AllocationPlanForTest,
        _page: [u8; 8],
        _bytes: usize,
    ) -> redb::Result {
        self.decision(plan, true)
    }
    fn written(&self, _plan: AllocationPlanForTest, _page: [u8; 8], bytes: usize) {
        let state = self.state.lock().expect("disk observation");
        assert!(state.admitted, "a real mutable write follows its admission");
        if let Some(sink) = &self.sink {
            sink(NativeIvfDiskEventForTest::Written {
                physical_owner: self.owner.identity(),
                scope: self.scope,
                operation: state.operation,
                class: self.class,
                bytes: bytes as u64,
            });
        }
    }
    fn refused(&self, plan: AllocationPlanForTest, _error: &redb::StorageError) {
        self.decision(plan, false)
            .expect("read actual physical limit");
    }
}
pub(crate) fn observe_native_ivf_write_for_test<T>(
    owner: Option<&NativeIvfPhysicalDiskOwnerForTest>,
    scope: NativeIvfScopeRef<'_>,
    class: NativeIvfBuildWriteClassForTest,
    run: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let Some(owner) = owner else { return run() };
    let sink = OBSERVER.with(|slot| slot.borrow().clone());
    let withhold = WITHHELD.with(|slot| {
        slot.borrow()
            .as_ref()
            .is_some_and(|(selected, c)| selected.identity() == owner.identity() && *c == class)
    });
    if sink.is_none() && !withhold && !CEILING.with(|slot| slot.borrow().is_some()) {
        return run();
    }
    let observation = WriteObservation {
        owner,
        scope,
        class,
        sink,
        state: Mutex::new(AccessState {
            operation: 0,
            admitted: false,
        }),
    };
    redb::allocation_observation::with_borrowed_allocation_observer_for_test(
        &observation,
        withhold,
        run,
    )
}

pub(crate) fn native_ivf_write_class_for_test(
    build: Option<contextdb_vector::ivf::storage::BuildState>,
    kind: contextdb_vector::ivf::storage::PageKind,
) -> NativeIvfBuildWriteClassForTest {
    use NativeIvfBuildWriteClassForTest::*;
    use contextdb_vector::ivf::storage::{BuildPhase, PageKind};
    match kind {
        PageKind::Reservoir => TrainingReservoir,
        PageKind::Codebook | PageKind::Accumulators => BuildAccumulator,
        _ => match build.map(|b| b.phase) {
            Some(BuildPhase::Seed | BuildPhase::Train) => BuildAccumulator,
            Some(BuildPhase::Assign) if kind != PageKind::Catalog => UnpublishedRun,
            Some(BuildPhase::Assign | BuildPhase::Publish | BuildPhase::Ready) => {
                PreparedReplacement
            }
            Some(BuildPhase::Ingest) | None => TrainingReservoir,
        },
    }
}
