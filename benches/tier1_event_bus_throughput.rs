use criterion::{Criterion, criterion_group, criterion_main};

fn bench_event_bus_throughput(c: &mut Criterion) {
    c.bench_function("tier1_event_bus_throughput", |b| {
        b.iter(|| {
            assert!(
                std::hint::black_box(false),
                "tier1_event_bus_throughput bench not yet implemented"
            );
        });
    });
}

criterion_group!(benches, bench_event_bus_throughput);
criterion_main!(benches);
