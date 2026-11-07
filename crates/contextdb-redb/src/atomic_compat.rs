//! Atomic conditional updates supported by the minimum Rust version.
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

pub(crate) trait AtomicUpdate<T> {
    fn fetch_update_compat(
        &self,
        set: Ordering,
        fetch: Ordering,
        update: impl FnMut(T) -> Option<T>,
    ) -> Result<T, T>;
}
macro_rules! conditional_update {
    ($atomic:ty, $value:ty) => {
        impl AtomicUpdate<$value> for $atomic {
            #[inline]
            fn fetch_update_compat(
                &self,
                set: Ordering,
                fetch: Ordering,
                mut update: impl FnMut($value) -> Option<$value>,
            ) -> Result<$value, $value> {
                let mut previous = self.load(fetch);
                loop {
                    let next = update(previous).ok_or(previous)?;
                    match self.compare_exchange_weak(previous, next, set, fetch) {
                        Ok(value) => return Ok(value),
                        Err(value) => previous = value,
                    }
                }
            }
        }
    };
}
conditional_update!(AtomicUsize, usize);
conditional_update!(AtomicU64, u64);
