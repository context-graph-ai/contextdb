//! Journey: vector-partition inspection uses the ordinary bounded read and cursor paths.
//!
//! This is deliberately a CLI/database journey: the fixture is written through
//! `contextdb --write`, inspection is read through a fresh CLI process, and no
//! test seam manufactures a page or cursor result.  The 501 partition rows are
//! the smallest deterministic fixture that crosses the shipped 500-row ordinary
//! result ceiling without relying on encoded-byte size.

mod read_cli_support;

use read_cli_support::*;

const SHIPPED_RESULT_ROWS: usize = 500;
const PARTITIONS: usize = SHIPPED_RESULT_ROWS + 1;
const VECTOR_PARTITION_COLUMNS: &[&str] = &[
    "table",
    "column",
    "partition_key",
    "live_rows",
    "retained_rows",
    "base_generation",
    "base_tx",
    "pending_inserts",
    "tombstones",
    "durable_vector_bytes",
    "charged_vector_bytes",
    "durable_index_bytes",
    "charged_index_bytes",
    "query_state",
    "availability_reason",
    "maintenance_state",
    "maintenance_reason",
    "maintenance_vectors_total",
    "maintenance_vectors_done",
    "maintenance_vectors_remaining",
    "maintenance_checkpoint_tx",
    "desired_hnsw_m",
    "desired_hnsw_ef_construction",
    "desired_hnsw_ef_search",
    "serving_hnsw_m",
    "serving_hnsw_ef_construction",
    "serving_hnsw_ef_search",
    "declared_consolidation_mode",
    "declared_consolidation_change_percent",
    "declared_consolidation_tombstone_percent",
    "effective_consolidation_mode",
    "effective_consolidation_change_percent",
    "effective_consolidation_tombstone_percent",
    "desired_policy_revision",
    "serving_policy_revision",
    "recovery_action",
];

fn partition_store() -> Store {
    let mut sql = String::from(
        "CREATE TABLE inspection_vectors (\
            id INTEGER PRIMARY KEY, \
            partition_id TEXT NOT NULL, \
            embedding VECTOR(3) PARTITION_KEY (partition_id) MAX_PARTITIONS 501, \
            secondary VECTOR(3)\
        );\n",
    );
    for id in 0..PARTITIONS {
        sql.push_str(&format!(
            "INSERT INTO inspection_vectors (id, partition_id, embedding) \
             VALUES ({id}, 'partition-{id:04}', [1.0, 0.0, 0.0]);\n"
        ));
    }
    store_with(&sql)
}

fn result(outcome: &Outcome, journey: &str) -> serde_json::Value {
    expect_document(&outcome.stdout_docs(), "result", journey)
}

fn cursor_pages(outcome: &Outcome) -> Vec<serde_json::Value> {
    documents_named(&outcome.stdout_docs(), "cursor")
        .into_iter()
        .filter(|page| page.get("rows").is_some())
        .collect()
}

fn partition_keys(document: &serde_json::Value) -> Vec<String> {
    rows_of(document)
        .into_iter()
        .map(|row| {
            row.get("partition_key")
                .and_then(|key| key.get("partition_id"))
                .and_then(|key| key.as_str())
                .unwrap_or_else(|| panic!("partition row keeps its JSON key: {row}"))
                .to_owned()
        })
        .collect()
}

fn expected_keys(start: usize, count: usize) -> Vec<String> {
    (start..start + count)
        .map(|id| format!("partition-{id:04}"))
        .collect()
}

fn assert_partition_schema(document: &serde_json::Value, journey: &str) {
    let columns = document
        .get("columns")
        .and_then(|columns| columns.as_array())
        .unwrap_or_else(|| panic!("{journey} returns columns: {document}"));
    let names: Vec<&str> = columns
        .iter()
        .filter_map(|column| column.as_str())
        .collect();
    assert_eq!(
        names, VECTOR_PARTITION_COLUMNS,
        "{journey} keeps the ordinary result schema"
    );
}

fn assert_partition_rows(document: &serde_json::Value, expected: &[String], journey: &str) {
    assert_partition_schema(document, journey);
    let rows = rows_of(document);
    assert_eq!(
        partition_keys(document),
        expected,
        "{journey} returns exact ordered keys"
    );
    for row in rows {
        assert_eq!(
            row.get("table").and_then(|value| value.as_str()),
            Some("inspection_vectors")
        );
        assert_eq!(
            row.get("column").and_then(|value| value.as_str()),
            Some("embedding")
        );
        assert_eq!(
            row.get("live_rows").and_then(|value| value.as_i64()),
            Some(1)
        );
        assert_eq!(
            row.get("retained_rows").and_then(|value| value.as_i64()),
            Some(0)
        );
    }
}

#[test]
fn show_vector_partitions_accepts_all_columns_targeted_and_optional_paging() {
    let store = partition_store();
    let args = [store.path_str(), "--json", "--read-result-rows", "600"];

    let all = run(&args, "SHOW VECTOR_PARTITIONS;\n");
    assert_eq!(
        all.code,
        Some(0),
        "the all-columns inspection reaches production.\n{}",
        all.describe()
    );
    let all_result = result(&all, "all-columns vector inspection");
    assert_partition_schema(&all_result, "all-columns vector inspection");
    assert_eq!(
        rows_of(&all_result).len(),
        PARTITIONS + 1,
        "all-columns inspection includes every partition and the unpartitioned column"
    );
    let all_rows = rows_of(&all_result);
    let all_partition_keys: Vec<String> = all_rows
        .iter()
        .filter(|row| row.get("column").and_then(|value| value.as_str()) == Some("embedding"))
        .map(|row| {
            row.get("partition_key")
                .and_then(|key| key.get("partition_id"))
                .and_then(|key| key.as_str())
                .unwrap_or_else(|| panic!("all-columns partition row keeps its JSON key: {row}"))
                .to_owned()
        })
        .collect();
    assert_eq!(
        all_partition_keys,
        expected_keys(0, PARTITIONS),
        "all-columns inspection keeps every partition exactly once in stable order"
    );
    let secondary = all_rows.last().expect("unpartitioned vector detail row");
    assert_eq!(
        secondary.get("column").and_then(|value| value.as_str()),
        Some("secondary"),
        "all-columns inspection retains the other declared vector column"
    );
    assert_eq!(secondary.get("partition_key"), Some(&serde_json::json!({})));

    let targeted = run(
        &args,
        "SHOW VECTOR_PARTITIONS FOR inspection_vectors.embedding;\n",
    );
    assert_eq!(
        targeted.code,
        Some(0),
        "the targeted inspection reaches production.\n{}",
        targeted.describe()
    );
    assert_partition_rows(
        &result(&targeted, "targeted vector inspection"),
        &expected_keys(0, PARTITIONS),
        "targeted vector inspection",
    );

    let limit_without_for = run(&args, "SHOW VECTOR_PARTITIONS LIMIT 2;\n");
    assert_eq!(
        limit_without_for.code,
        Some(0),
        "LIMIT is legal for all columns.\n{}",
        limit_without_for.describe()
    );
    assert_partition_rows(
        &result(&limit_without_for, "all-columns LIMIT"),
        &expected_keys(0, 2),
        "all-columns LIMIT defaults OFFSET to zero",
    );

    let limit_without_offset = run(
        &args,
        "SHOW VECTOR_PARTITIONS FOR inspection_vectors.embedding LIMIT 2;\n",
    );
    assert_eq!(
        limit_without_offset.code,
        Some(0),
        "LIMIT does not require OFFSET.\n{}",
        limit_without_offset.describe()
    );
    assert_partition_rows(
        &result(&limit_without_offset, "targeted LIMIT"),
        &expected_keys(0, 2),
        "targeted LIMIT defaults OFFSET to zero",
    );

    let explicit_offset = run(
        &args,
        "SHOW VECTOR_PARTITIONS FOR inspection_vectors.embedding LIMIT 2 OFFSET 2;\n",
    );
    assert_eq!(
        explicit_offset.code,
        Some(0),
        "the existing explicit paging form reaches production.\n{}",
        explicit_offset.describe()
    );
    assert_partition_rows(
        &result(&explicit_offset, "explicit OFFSET"),
        &expected_keys(2, 2),
        "explicit OFFSET selects the next stable rows",
    );
}

#[test]
fn oversized_vector_inspection_refuses_then_the_ordinary_cursor_pages_every_row() {
    let store = partition_store();
    let ordinary = run(&[store.path_str(), "--json"], "SHOW VECTOR_PARTITIONS;\n");
    assert_eq!(
        ordinary.code,
        Some(1),
        "the ordinary ceiling refuses rather than truncating inspection.\n{}",
        ordinary.describe()
    );
    assert!(
        ordinary.stdout.trim().is_empty(),
        "a refused inspection emits no partial rows.\n{}",
        ordinary.describe()
    );
    let error = ordinary
        .errors()
        .into_iter()
        .find(|error| detail_kind(error).as_deref() == Some("owner_limit_exceeded"))
        .unwrap_or_else(|| panic!("inspection refusal is typed.\n{}", ordinary.describe()));
    assert_eq!(
        error
            .get("detail")
            .and_then(|detail| detail.get("limit"))
            .and_then(|limit| limit.as_str()),
        Some("result_rows"),
        "the ordinary row ceiling refused the whole inspection: {error}"
    );
    let remedy = error
        .get("detail")
        .and_then(|detail| detail.get("remedy_command"))
        .and_then(|remedy| remedy.as_str())
        .unwrap_or_default();
    assert!(
        remedy.starts_with(".cursor open SHOW VECTOR_PARTITIONS"),
        "the typed recovery opens the ordinary cursor on the refused SHOW: {error}"
    );
    let human = run(&[store.path_str()], "SHOW VECTOR_PARTITIONS;\n");
    assert_eq!(
        human.code,
        Some(1),
        "human rendering keeps the same refusal.\n{}",
        human.describe()
    );
    assert!(
        human.stderr.contains(".cursor open SHOW VECTOR_PARTITIONS"),
        "human rendering prints the same executable cursor recovery.\n{}",
        human.describe()
    );

    let paged = run(
        &[store.path_str(), "--json", "--read-cursor-page-rows", "200"],
        ".cursor open SHOW VECTOR_PARTITIONS\n.cursor fetch\n.cursor fetch\n",
    );
    assert_eq!(
        paged.code,
        Some(0),
        "the ordinary cursor pages all-columns inspection.\n{}",
        paged.describe()
    );
    let pages = cursor_pages(&paged);
    assert_eq!(
        pages.len(),
        3,
        "three stable ordinary pages cover 502 rows.\n{}",
        paged.describe()
    );
    for page in &pages {
        assert_partition_schema(page, "cursor page");
    }
    let rows: Vec<serde_json::Value> = pages.iter().flat_map(rows_of).collect();
    assert_eq!(
        rows.len(),
        PARTITIONS + 1,
        "cursor paging omits no inspection row"
    );
    assert_eq!(
        rows.iter()
            .filter(|row| row.get("column").and_then(|value| value.as_str()) == Some("embedding"))
            .count(),
        PARTITIONS,
        "every partitioned row appears exactly once"
    );
    let paged_keys: Vec<String> = rows
        .iter()
        .filter(|row| row.get("column").and_then(|value| value.as_str()) == Some("embedding"))
        .map(|row| {
            row.get("partition_key")
                .and_then(|key| key.get("partition_id"))
                .and_then(|key| key.as_str())
                .unwrap_or_else(|| panic!("cursor row keeps its JSON partition key: {row}"))
                .to_owned()
        })
        .collect();
    assert_eq!(
        paged_keys,
        expected_keys(0, PARTITIONS),
        "all-columns cursor pages preserve the exact partition order"
    );

    let targeted = run(
        &[store.path_str(), "--json", "--read-cursor-page-rows", "250"],
        ".cursor open SHOW VECTOR_PARTITIONS FOR inspection_vectors.embedding\n.cursor fetch\n.cursor fetch\n",
    );
    assert_eq!(
        targeted.code,
        Some(0),
        "the ordinary cursor pages targeted inspection.\n{}",
        targeted.describe()
    );
    let targeted_pages = cursor_pages(&targeted);
    let targeted_rows: Vec<serde_json::Value> = targeted_pages.iter().flat_map(rows_of).collect();
    let targeted_document =
        serde_json::json!({"columns": VECTOR_PARTITION_COLUMNS, "rows": targeted_rows});
    assert_partition_rows(
        &targeted_document,
        &expected_keys(0, PARTITIONS),
        "targeted cursor pages",
    );
}

#[test]
fn owner_read_override_changes_the_ordinary_inspection_ceiling_without_a_vector_cap() {
    let store = partition_store();
    let _owner = live_owner_with(&store.path, &["--owner-read-result-rows", "600"], "");
    let outcome = run(
        &[store.path_str(), "--json", "--read-result-rows", "600"],
        "SHOW VECTOR_PARTITIONS;\n",
    );
    assert_eq!(
        outcome.code,
        Some(0),
        "the owner-read row override publishes the same ordinary inspection.\n{}",
        outcome.describe()
    );
    let document = result(&outcome, "owner-read vector inspection");
    assert_partition_schema(&document, "owner-read vector inspection");
    assert_eq!(
        rows_of(&document).len(),
        PARTITIONS + 1,
        "the owner override changes the ordinary ceiling, not a vector-specific one"
    );
}

#[test]
fn owner_read_byte_policy_applies_to_vector_inspection_as_an_ordinary_result() {
    let store = partition_store();
    let _owner = live_owner_with(
        &store.path,
        &[
            "--owner-read-result-bytes",
            "512",
            "--owner-read-cursor-page-bytes",
            "512",
        ],
        "",
    );
    let outcome = run(
        &[store.path_str(), "--json"],
        "SHOW VECTOR_PARTITIONS FOR inspection_vectors.embedding LIMIT 1 OFFSET 0;\n",
    );
    assert_eq!(
        outcome.code,
        Some(1),
        "the owner byte ceiling also bounds vector inspection.\n{}",
        outcome.describe()
    );
    assert!(
        outcome.stdout.trim().is_empty(),
        "the owner byte refusal publishes no partial inspection row"
    );
    let error = outcome
        .errors()
        .into_iter()
        .find(|error| detail_kind(error).as_deref() == Some("owner_limit_exceeded"))
        .unwrap_or_else(|| panic!("owner byte refusal is typed.\n{}", outcome.describe()));
    assert_eq!(
        error
            .get("detail")
            .and_then(|detail| detail.get("limit"))
            .and_then(|limit| limit.as_str()),
        Some("result_bytes"),
        "the owner byte policy uses the ordinary result-byte refusal: {error}"
    );
}

#[test]
fn controls_prove_the_existing_select_cursor_and_valid_show_form_reach_production() {
    let store = partition_store();
    let select = run(
        &[store.path_str(), "--json", "--read-cursor-page-rows", "1"],
        ".cursor open SELECT id FROM inspection_vectors ORDER BY id LIMIT 1\n",
    );
    assert_eq!(
        select.code,
        Some(0),
        "the existing SELECT cursor control reaches production.\n{}",
        select.describe()
    );
    assert_eq!(
        cursor_pages(&select).len(),
        1,
        "the SELECT control publishes its first ordinary page"
    );

    let show = run(
        &[store.path_str(), "--json", "--read-result-rows", "600"],
        "SHOW VECTOR_PARTITIONS FOR inspection_vectors.embedding LIMIT 1 OFFSET 0;\n",
    );
    assert_eq!(
        show.code,
        Some(0),
        "the currently valid FOR/LIMIT/OFFSET SHOW form reaches production.\n{}",
        show.describe()
    );
    assert_partition_rows(
        &result(&show, "existing SHOW control"),
        &expected_keys(0, 1),
        "existing SHOW control",
    );
}

#[test]
fn table_only_partition_target_is_refused_by_the_cli() {
    let store = partition_store();
    for statement in [
        "SHOW VECTOR_PARTITIONS FOR inspection_vectors;\n",
        ".cursor open SHOW VECTOR_PARTITIONS FOR inspection_vectors\n",
    ] {
        let outcome = run(&[store.path_str(), "--json"], statement);
        assert_ne!(outcome.code, Some(0), "{}", outcome.describe());
        assert!(outcome.stdout.trim().is_empty(), "{}", outcome.describe());
        assert!(!outcome.errors().is_empty(), "{}", outcome.describe());
    }
}
