use criterion::{Criterion, criterion_group, criterion_main};

fn bench_context_guard_overhead(c: &mut Criterion) {
    c.bench_function("tier1_context_guard_overhead", |b| {
        b.iter(|| {
            assert!(
                std::hint::black_box(false),
                "tier1_context_guard_overhead bench not yet implemented"
            );
        });
    });
}

criterion_group!(benches, bench_context_guard_overhead);
criterion_main!(benches);
