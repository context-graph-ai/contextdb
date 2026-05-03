//! benches/tier2_fk_revalidation_overhead.rs
#![allow(clippy::assertions_on_constants)]

use criterion::{Criterion, criterion_group, criterion_main};

fn bench(c: &mut Criterion) {
    c.bench_function("tier2_fk_revalidation_overhead", |b| {
        b.iter(|| {
            assert!(false, "not yet implemented");
        });
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
