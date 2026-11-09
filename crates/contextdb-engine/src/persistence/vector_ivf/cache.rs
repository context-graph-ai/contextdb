//! One immutable-root descriptor window in the existing retained source holder.
//! Its fixed payload retains its original reader invoice through physical free.

#![expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
use super::*;
use contextdb_core::read_memory::{ReadCredit, ReadMemoryOwner, ReadShared};
use contextdb_vector::ivf::storage::{Manifest, Verdict};
use parking_lot::Mutex;

pub(crate) struct NativeIvfCache {
    #[cfg(feature = "test-seams")]
    observation_identity: u64,
    pub(super) descriptor: Mutex<Option<ReadShared<DescriptorWindow>>>,
    pub(super) retained: Mutex<Option<super::retained::RegistryOwner>>,
    pub(super) activation: Mutex<()>,
    // Only the persistence cache retains calibration. Root forks start empty,
    // so a capture cannot retain itself through its own source cache.
    pub(super) calibration: Mutex<Option<super::calibration::capture::Owner>>,
    pub(super) calibration_epoch: std::sync::atomic::AtomicU64,
}
pub(super) struct DescriptorWindow {
    pub key: [u8; 64],
    pub descriptor: [u8; descriptor::BYTES],
    pub manifest: Manifest,
    pub verified: Mutex<([u16; control::ROOTS], [Option<Verdict>; control::ROOTS])>,
}
impl NativeIvfCache {
    pub(super) fn get(&self, key: &[u8; 64]) -> Option<ReadShared<DescriptorWindow>> {
        self.descriptor
            .lock()
            .as_ref()
            .filter(|window| window.key == *key)
            .cloned()
    }
    pub(super) fn install(
        &self,
        key: [u8; 64],
        descriptor: [u8; descriptor::BYTES],
        manifest: Manifest,
        memory: ReadMemoryOwner,
    ) -> Result<ReadShared<DescriptorWindow>> {
        let mut slot = self.descriptor.lock();
        if let Some(window) = slot.as_ref().filter(|window| window.key == key) {
            return Ok(window.clone());
        }
        // One bounded window, with eviction before admission. No population map.
        *slot = None;
        let credit = ReadCredit::try_new(
            memory,
            ReadShared::<DescriptorWindow>::layout().size(),
            "vector_index",
            "clustered_descriptor_window",
            "Release retained source metadata or raise reader memory.",
        )?;
        let window = ReadShared::try_from_prepaid_with(credit, || {
            Ok(DescriptorWindow {
                key,
                descriptor,
                manifest,
                verified: Mutex::new(([0; control::ROOTS], [None; control::ROOTS])),
            })
        })?;
        *slot = Some(window.clone());
        Ok(window)
    }
}

impl Default for NativeIvfCache {
    fn default() -> Self {
        Self {
            descriptor: Mutex::new(None),
            retained: Mutex::new(None),
            activation: Mutex::new(()),
            calibration: Mutex::new(None),
            calibration_epoch: std::sync::atomic::AtomicU64::new(1),
            #[cfg(feature = "test-seams")]
            observation_identity: {
                static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
                NEXT.fetch_update(
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                    |value| value.checked_add(1),
                )
                .expect("native cache observation identity exhausted")
            },
        }
    }
}
#[cfg(feature = "test-seams")]
impl NativeIvfCache {
    pub(crate) fn observation_identity_for_test(&self) -> u64 {
        self.observation_identity
    }
}

impl NativeIvfCache {
    pub(super) fn cancel_calibration_where(
        &self,
        matches: impl FnOnce(&super::calibration::capture::Capture) -> bool,
    ) {
        let retired = {
            let mut active = self.calibration.lock();
            if active.as_ref().is_some_and(|capture| matches(capture)) {
                if let Some(capture) = active.as_ref() {
                    capture
                        .cancelled
                        .store(true, std::sync::atomic::Ordering::Release);
                }
                active.take()
            } else {
                None
            }
        };
        // Its whole capture, root and invoices free without cache locks.
        drop(retired);
    }
}
