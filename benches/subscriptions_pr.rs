#[path = "common/mod.rs"]
mod common;

use common::workloads::{drain_events, setup_subscription_db, subscription_insert_sql};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::sync::mpsc::Receiver;

fn subscription_fanout_pr(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscriptions_pr");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));
    group.warm_up_time(std::time::Duration::from_millis(500));

    for subscriber_count in [4usize, 8usize] {
        group.bench_with_input(
            BenchmarkId::new("fanout_single_commit", subscriber_count),
            &subscriber_count,
            |b, &subscriber_count| {
                b.iter_batched(
                    || {
                        let db = setup_subscription_db();
                        let receivers: Vec<Receiver<_>> =
                            (0..subscriber_count).map(|_| db.subscribe()).collect();
                        (db, receivers)
                    },
                    |(db, receivers)| {
                        db.execute(
                            &subscription_insert_sql(1),
                            &std::collections::HashMap::new(),
                        )
                        .unwrap();
                        for rx in &receivers {
                            rx.recv_timeout(std::time::Duration::from_secs(2))
                                .expect("subscriber should receive commit event");
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.bench_function("backpressure_small_capacity", |b| {
        b.iter_batched(
            || {
                let db = setup_subscription_db();
                let rx = db.subscribe_with_capacity(2);
                (db, rx)
            },
            |(db, rx)| {
                for id in 1..=10u128 {
                    db.execute(
                        &subscription_insert_sql(id),
                        &std::collections::HashMap::new(),
                    )
                    .unwrap();
                }
                let delivered = drain_events(&rx);
                assert!(
                    delivered <= 2,
                    "capacity-2 channel should not buffer more than 2"
                );
                let health = db.subscription_health();
                assert!(
                    health.events_dropped >= 8,
                    "expected drops under backpressure, got {}",
                    health.events_dropped
                );
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("concurrent_commit_delivery", |b| {
        b.iter_batched(
            || {
                let db = setup_subscription_db();
                let rx = db.subscribe();
                (db, rx)
            },
            |(db, rx)| {
                let db = std::sync::Arc::clone(&db);
                let db1 = std::sync::Arc::clone(&db);
                let db2 = std::sync::Arc::clone(&db);
                let h1 = std::thread::spawn(move || {
                    for id in 1..=10u128 {
                        db1.execute(
                            &subscription_insert_sql(id),
                            &std::collections::HashMap::new(),
                        )
                        .unwrap();
                    }
                });
                let h2 = std::thread::spawn(move || {
                    for id in 11..=20u128 {
                        db2.execute(
                            &subscription_insert_sql(id),
                            &std::collections::HashMap::new(),
                        )
                        .unwrap();
                    }
                });
                h1.join().unwrap();
                h2.join().unwrap();

                let mut total_rows = 0usize;
                while let Ok(event) = rx.recv_timeout(std::time::Duration::from_secs(2)) {
                    total_rows += event.row_count;
                    if total_rows >= 20 {
                        break;
                    }
                }
                assert_eq!(
                    total_rows, 20,
                    "subscriber should observe 20 committed rows"
                );
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, subscription_fanout_pr);
criterion_main!(benches);
