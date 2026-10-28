//! Passive service attribution; these records own no memory capability.
use std::cell::Cell;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone, Copy)]
struct Scope {
    id: u64,
    leases: usize,
    used: u64,
    controls: u64,
}
#[derive(Clone, Copy)]
struct Child {
    id: u64,
    scope: u64,
    counter: usize,
    held: u64,
    controls: u64,
    control_alive: bool,
    pending: bool,
}
struct Registry {
    next: u64,
    scopes: [Option<Scope>; 64],
    children: [Option<Child>; 4096],
}
static REGISTRY: Mutex<Registry> = Mutex::new(Registry {
    next: 0,
    scopes: [None; 64],
    children: [None; 4096],
});
static ACTIVE: AtomicUsize = AtomicUsize::new(0);
thread_local! { static CURRENT: Cell<Option<u64>> = const { Cell::new(None) }; }
impl Registry {
    fn next(&mut self) -> u64 {
        self.next = self
            .next
            .checked_add(1)
            .expect("service observation generation exhausted");
        self.next
    }
    fn scope(&mut self, id: u64) -> &mut Scope {
        self.scopes
            .iter_mut()
            .flatten()
            .find(|s| s.id == id)
            .expect("service observation generation lost")
    }
}
#[derive(Debug)]
pub(crate) struct ServiceObservation {
    id: u64,
}
impl ServiceObservation {
    pub(crate) fn new() -> Self {
        let mut r = REGISTRY.lock().unwrap_or_else(|p| p.into_inner());
        let slot = r
            .scopes
            .iter()
            .position(Option::is_none)
            .expect("service observation slots exhausted; report invalid");
        let id = r.next();
        r.scopes[slot] = Some(Scope {
            id,
            leases: 1,
            used: 0,
            controls: 0,
        });
        ACTIVE.fetch_add(1, Ordering::Relaxed);
        Self { id }
    }
    pub(crate) fn enter(&self) -> ServiceScope {
        ServiceScope {
            previous: CURRENT.with(|c| c.replace(Some(self.id))),
            _lease: self.clone(),
        }
    }
    pub(crate) fn snapshot(&self) -> (u64, u64) {
        let mut r = REGISTRY.lock().unwrap_or_else(|p| p.into_inner());
        let s = r.scope(self.id);
        (s.used, s.controls)
    }
}
impl Clone for ServiceObservation {
    fn clone(&self) -> Self {
        let mut r = REGISTRY.lock().unwrap_or_else(|p| p.into_inner());
        let s = r.scope(self.id);
        s.leases = s
            .leases
            .checked_add(1)
            .expect("service observation lease overflow");
        Self { id: self.id }
    }
}
impl Drop for ServiceObservation {
    fn drop(&mut self) {
        let mut r = REGISTRY.lock().unwrap_or_else(|p| p.into_inner());
        let i = r
            .scopes
            .iter()
            .position(|s| s.is_some_and(|s| s.id == self.id))
            .expect("service observation lease lost");
        let s = r.scopes[i].as_mut().unwrap();
        s.leases = s.leases.checked_sub(1).expect("service lease underflow");
        if s.leases == 0 {
            assert_eq!(
                (s.used, s.controls),
                (0, 0),
                "service observation cannot retire live charges"
            );
            r.scopes[i] = None;
            ACTIVE.fetch_sub(1, Ordering::Relaxed);
        }
    }
}
pub(crate) struct ServiceScope {
    previous: Option<u64>,
    _lease: ServiceObservation,
}
impl Drop for ServiceScope {
    fn drop(&mut self) {
        CURRENT.with(|c| c.set(self.previous));
    }
}
#[derive(Debug)]
pub(super) struct ChildControl {
    id: u64,
    _scope: ServiceObservation,
}
impl ChildControl {
    pub(super) fn prepare() -> Option<Self> {
        let scope = CURRENT.with(Cell::get)?;
        let mut r = REGISTRY.lock().unwrap_or_else(|p| p.into_inner());
        let i = r
            .children
            .iter()
            .position(Option::is_none)
            .expect("service child observation slots exhausted; report invalid");
        let id = r.next();
        let s = r.scope(scope);
        s.leases = s
            .leases
            .checked_add(1)
            .expect("service observation lease overflow");
        r.children[i] = Some(Child {
            id,
            scope,
            counter: 0,
            held: 0,
            controls: 0,
            control_alive: true,
            pending: false,
        });
        Some(Self {
            id,
            _scope: ServiceObservation { id: scope },
        })
    }
    pub(super) fn id(&self) -> u64 {
        self.id
    }
}
impl Drop for ChildControl {
    fn drop(&mut self) {
        let mut r = REGISTRY.lock().unwrap_or_else(|p| p.into_inner());
        let i = r
            .children
            .iter()
            .position(|c| c.is_some_and(|c| c.id == self.id))
            .expect("service child observation lost");
        let c = r.children[i].as_mut().unwrap();
        if !c.pending {
            assert_eq!(
                c.held, 0,
                "a service child physically freed with live payload credits"
            );
        }
        let (scope, controls, pending) = (c.scope, c.controls, c.pending);
        c.control_alive = false;
        let s = r.scope(scope);
        s.used = s
            .used
            .checked_sub(controls)
            .expect("service control refund underflow");
        s.controls = s
            .controls
            .checked_sub(controls)
            .expect("service native control underflow");
        if !pending {
            r.children[i] = None;
        }
    }
}
pub(super) fn controls(id: Option<u64>, bytes: usize) {
    if let Some(id) = id {
        let mut r = REGISTRY.lock().unwrap_or_else(|p| p.into_inner());
        let i = r
            .children
            .iter()
            .position(|c| c.is_some_and(|c| c.id == id))
            .expect("service constructor observation lost");
        let c = r.children[i].unwrap();
        assert_eq!(c.controls, 0);
        let s = r.scope(c.scope);
        let used = s
            .used
            .checked_add(bytes as u64)
            .expect("service usage overflow");
        let controls = s
            .controls
            .checked_add(bytes as u64)
            .expect("service controls overflow");
        s.used = used;
        s.controls = controls;
        r.children[i].as_mut().unwrap().controls = bytes as u64;
    }
}
struct PayloadRollback {
    id: Option<u64>,
    bytes: usize,
    armed: bool,
    _scope: Option<ServiceObservation>,
}
impl PayloadRollback {
    fn commit(&mut self) {
        if let Some(id) = self.id {
            let mut r = REGISTRY.lock().unwrap_or_else(|p| p.into_inner());
            let c = r
                .children
                .iter_mut()
                .flatten()
                .find(|c| c.id == id)
                .expect("service constructor observation lost");
            assert_ne!(
                c.counter, 0,
                "published service child owns its transferred credit"
            );
            c.pending = false;
        }
        self.armed = false;
    }
}
impl Drop for PayloadRollback {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(id) = self.id {
            let mut r = REGISTRY.lock().unwrap_or_else(|p| p.into_inner());
            let i = r
                .children
                .iter()
                .position(|c| c.is_some_and(|c| c.id == id))
                .expect("service constructor rollback lost");
            let c = r.children[i].as_mut().unwrap();
            assert!(c.pending);
            c.held = c
                .held
                .checked_sub(self.bytes as u64)
                .expect("service constructor payload underflow");
            c.pending = false;
            let (scope, alive) = (c.scope, c.control_alive);
            let s = r.scope(scope);
            s.used = s
                .used
                .checked_sub(self.bytes as u64)
                .expect("service constructor usage underflow");
            if !alive {
                assert_eq!(r.children[i].unwrap().held, 0);
                r.children[i] = None;
            }
        }
    }
}
/// Original constructor credit settles before its passive rollback mirror.
pub(super) struct PendingPayloadCredit {
    credit: Option<contextdb_core::read_memory::ReadCredit>,
    pending: PayloadRollback,
}
impl PendingPayloadCredit {
    pub(super) fn new(
        credit: contextdb_core::read_memory::ReadCredit,
        id: Option<u64>,
        bytes: usize,
    ) -> Self {
        let scope = id.map(|id| {
            let mut r = REGISTRY.lock().unwrap_or_else(|p| p.into_inner());
            let scope = r
                .children
                .iter()
                .flatten()
                .find(|c| c.id == id)
                .expect("service constructor observation lost")
                .scope;
            let s = r.scope(scope);
            s.leases = s.leases.checked_add(1).expect("service lease overflow");
            ServiceObservation { id: scope }
        });
        let mut value = Self {
            credit: Some(credit),
            pending: PayloadRollback {
                id,
                bytes,
                armed: false,
                _scope: scope,
            },
        };
        if let Some(id) = id {
            let mut r = REGISTRY.lock().unwrap_or_else(|p| p.into_inner());
            let i = r
                .children
                .iter()
                .position(|c| c.is_some_and(|c| c.id == id))
                .expect("service constructor observation lost");
            let c = r.children[i].unwrap();
            assert_eq!(c.held, 0);
            assert!(!c.pending);
            let s = r.scope(c.scope);
            s.used = s
                .used
                .checked_add(bytes as u64)
                .expect("service initial usage overflow");
            let c = r.children[i].as_mut().unwrap();
            c.held = bytes as u64;
            c.pending = true;
            value.pending.armed = true;
        }
        value
    }
    pub(super) fn transfer(mut self) {
        self.pending.commit();
        self.credit
            .take()
            .expect("original prepaid constructor credit")
            .transfer();
    }
}
pub(super) fn bind(id: Option<u64>, counter: &std::sync::atomic::AtomicU64) {
    if let Some(id) = id {
        let mut r = REGISTRY.lock().unwrap_or_else(|p| p.into_inner());
        let c = r
            .children
            .iter_mut()
            .flatten()
            .find(|c| c.id == id)
            .expect("service constructor observation lost");
        assert_eq!(c.held, counter.load(Ordering::SeqCst));
        c.counter = counter as *const _ as usize;
    }
}
fn change(counter: &std::sync::atomic::AtomicU64, bytes: usize, add: bool) {
    if ACTIVE.load(Ordering::Relaxed) == 0 {
        return;
    }
    let mut r = REGISTRY.lock().unwrap_or_else(|p| p.into_inner());
    let Some(c) = r
        .children
        .iter_mut()
        .flatten()
        .find(|c| c.counter == counter as *const _ as usize)
    else {
        return;
    };
    c.held = if add {
        c.held.checked_add(bytes as u64)
    } else {
        c.held.checked_sub(bytes as u64)
    }
    .expect("service child payload observation arithmetic");
    let scope = c.scope;
    let s = r.scope(scope);
    s.used = if add {
        s.used.checked_add(bytes as u64)
    } else {
        s.used.checked_sub(bytes as u64)
    }
    .expect("service payload observation arithmetic");
}
pub(super) fn admitted(counter: &std::sync::atomic::AtomicU64, bytes: usize) {
    change(counter, bytes, true);
}
pub(super) fn released(counter: &std::sync::atomic::AtomicU64, bytes: usize) {
    change(counter, bytes, false);
}

// Payload destruction precedes native deallocation. Stop matching its address
// now, but retain every scalar charge until the external final invoice drops.
pub(super) fn payload_dropped(counter: &std::sync::atomic::AtomicU64) {
    if ACTIVE.load(Ordering::Relaxed) == 0 {
        return;
    }
    let mut r = REGISTRY.lock().unwrap_or_else(|p| p.into_inner());
    if let Some(c) = r
        .children
        .iter_mut()
        .flatten()
        .find(|c| c.counter == counter as *const _ as usize)
    {
        c.counter = 0;
    }
}
