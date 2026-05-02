use criterion::{Criterion, criterion_group, criterion_main};

fn bench_atomic_visibility_under_contention(c: &mut Criterion) {
    c.bench_function("tier1_atomic_visibility_under_contention", |b| {
        b.iter(|| {
            // stub: bench body filled by impl plan. Currently asserts to ensure
            // `cargo bench -- --test` fails until pre-gate / timed-path /
            // post-gate / wall-clock budget are implemented.
            assert!(
                std::hint::black_box(false),
                "tier1_atomic_visibility_under_contention bench not yet implemented"
            );
        });
    });
}

criterion_group!(benches, bench_atomic_visibility_under_contention);
criterion_main!(benches);
