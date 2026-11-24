use crate::output::Output;
use contextdb::{ReadProgress, ReadProgressObserver};
use std::sync::atomic::{AtomicU64, Ordering};
pub struct Progress {
    pub output: Output,
    pub threshold: u64,
    pub next: AtomicU64,
}
impl ReadProgressObserver for Progress {
    fn progress(&self, p: ReadProgress) {
        if p.active_ms >= self.next.load(Ordering::Relaxed) {
            self.next.store(
                p.active_ms.saturating_add(self.threshold),
                Ordering::Relaxed,
            );
            self.output.notice(
                "statement_progress",
                &format!(
                    "statement_progress: {} ms, {} rows, {} bytes, {} work",
                    p.active_ms, p.rows, p.bytes, p.work
                ),
            );
        }
    }
    fn completed(&self, _: ReadProgress) {
        self.next.store(self.threshold, Ordering::Relaxed);
    }
}
