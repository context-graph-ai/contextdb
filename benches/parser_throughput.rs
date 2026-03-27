use criterion::{Criterion, criterion_group, criterion_main};

fn parse_simple_statements(c: &mut Criterion) {
    let mut group = c.benchmark_group("parser");

    group.bench_function("parse_insert_1000x", |b| {
        let stmts: Vec<String> = (0..1_000)
            .map(|i| {
                format!(
                    "INSERT INTO items (id, name, seq) VALUES ('00000000-0000-0000-0000-{i:012}', 'item-{i}', {i})"
                )
            })
            .collect();

        b.iter(|| {
            for stmt in &stmts {
                contextdb_parser::parse(stmt).unwrap();
            }
        });
    });

    group.bench_function("parse_select_with_joins_1000x", |b| {
        let stmt = "SELECT a.id, b.name FROM t1 a INNER JOIN t2 b ON a.id = b.ref_id WHERE a.status = 'active' ORDER BY a.id LIMIT 10";

        b.iter(|| {
            for _ in 0..1_000 {
                contextdb_parser::parse(stmt).unwrap();
            }
        });
    });

    group.bench_function("parse_cte_graph_vector_100x", |b| {
        let stmt = "\
            WITH similar AS (\
                SELECT id FROM observations \
                ORDER BY embedding <=> $vec \
                LIMIT 5\
            ), \
            reached AS (\
                SELECT b_id FROM GRAPH_TABLE(\
                    edges MATCH (a)-[:OBSERVED_ON]->(b) \
                    WHERE a.id IN (SELECT id FROM similar) \
                    COLUMNS (b.id AS b_id)\
                )\
            ) \
            SELECT d.id, d.description \
            FROM decisions d \
            INNER JOIN reached r ON d.id = r.b_id \
            WHERE d.status = 'active'";

        b.iter(|| {
            for _ in 0..100 {
                contextdb_parser::parse(stmt).unwrap();
            }
        });
    });

    group.bench_function("parse_create_table_100x", |b| {
        let stmt = "CREATE TABLE observations (id UUID PRIMARY KEY, entity_id UUID, summary TEXT, embedding VECTOR(384), observed_at INTEGER)";

        b.iter(|| {
            for _ in 0..100 {
                contextdb_parser::parse(stmt).unwrap();
            }
        });
    });

    group.finish();
}

criterion_group!(benches, parse_simple_statements);
criterion_main!(benches);
