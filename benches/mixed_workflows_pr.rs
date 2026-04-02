#[path = "common/mod.rs"]
mod common;

use common::workloads::{
    run_flagship_recall_query, run_graph_relational_vector_query, setup_flagship_recall_db,
    setup_graph_relational_vector_db,
};
use criterion::{Criterion, criterion_group, criterion_main};

fn mixed_workflows_pr(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workflows_pr");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));
    group.warm_up_time(std::time::Duration::from_millis(500));

    let (db, root) = setup_graph_relational_vector_db();
    group.bench_function("graph_relational_vector", |b| {
        b.iter(|| run_graph_relational_vector_query(&db, root));
    });

    let flagship = setup_flagship_recall_db();
    group.bench_function("flagship_recall_query", |b| {
        b.iter(|| run_flagship_recall_query(&flagship));
    });

    group.finish();
}

criterion_group!(benches, mixed_workflows_pr);
criterion_main!(benches);
