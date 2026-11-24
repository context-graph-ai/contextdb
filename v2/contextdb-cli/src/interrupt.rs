//! Ctrl-C cancels the running statement through its session, or wakes a piped run's input.
use contextdb::{Database, ReadSession};
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, Mutex, MutexGuard, mpsc::Sender};
pub enum Active {
    Reader(Arc<ReadSession>),
    Writer(Database),
    Input(Sender<std::io::Result<String>>),
}
static ACTIVE: Mutex<Option<Active>> = Mutex::new(None);
pub static FIRED: AtomicBool = AtomicBool::new(false);
fn active() -> MutexGuard<'static, Option<Active>> {
    ACTIVE.lock().unwrap_or_else(|e| e.into_inner())
}
pub fn install() -> Result<(), ctrlc::Error> {
    ctrlc::set_handler(fire)
}
pub fn fire() {
    FIRED.store(true, SeqCst);
    match &*active() {
        Some(Active::Reader(reader)) => reader.cancel(),
        Some(Active::Writer(db)) => db.cancel(),
        Some(Active::Input(wake)) => drop(wake.send(Err(std::io::ErrorKind::Interrupted.into()))),
        None => {}
    }
}
pub fn running<T>(now: Active, call: impl FnOnce() -> T) -> T {
    let before = active().replace(now);
    let answer = call();
    *active() = before;
    answer
}
pub fn reading<T>(reader: &Arc<ReadSession>, call: impl FnOnce() -> T) -> T {
    running(Active::Reader(reader.clone()), call)
}
/// Through a handle on the same store that lives only while the statement runs.
pub fn writing<T>(db: &Database, call: impl FnOnce() -> T) -> T {
    running(
        Active::Writer(db.scoped_with_constraints(None, None, None)),
        call,
    )
}
