use criterion::{Criterion, criterion_group, criterion_main};

fn bench_scope_label_overhead(c: &mut Criterion) {
    c.bench_function("tier1_scope_label_overhead", |b| {
        b.iter(|| {
            assert!(
                std::hint::black_box(false),
                "tier1_scope_label_overhead bench not yet implemented"
            );
        });
    });
}

criterion_group!(benches, bench_scope_label_overhead);
criterion_main!(benches);
