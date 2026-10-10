// Counted standalone requesting reader for cross-crate native test fixtures.
#[allow(dead_code)] // Shared support is compiled in test targets that do not construct this fixture.
#[derive(Debug, Default)]
pub struct TestReadMemory {
    used: std::sync::atomic::AtomicUsize,
}
impl contextdb_core::read_memory::ReadMemoryBudget for TestReadMemory {
    fn try_reserve(
        &self,
        bytes: usize,
        _: &'static str,
        _: &'static str,
        _: &'static str,
    ) -> contextdb_core::Result<()> {
        self.used
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |used| used.checked_add(bytes),
            )
            .map_err(|_| contextdb_core::Error::Other("test reader charge overflow".into()))?;
        Ok(())
    }
    fn release(&self, bytes: usize) {
        self.used
            .fetch_sub(bytes, std::sync::atomic::Ordering::SeqCst);
    }
}
impl Drop for TestReadMemory {
    fn drop(&mut self) {
        assert_eq!(
            self.used.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "requesting test reader returns all credit at teardown"
        );
    }
}
