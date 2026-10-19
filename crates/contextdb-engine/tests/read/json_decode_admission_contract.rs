//! File-route JSON values must be admitted as they are decoded, rather than
//! after an unbounded serde tree has already been built.

use contextdb_core::Error;
use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const DEFAULT_MEMORY: usize = ReadLimits::SHIPPED_MEMORY as usize;
const AUDITED_SERDE_JSON_VERSION: &str = "1.0.149";
const AUDITED_SERDE_JSON_FEATURES: &[&str] = &["default", "raw_value", "std"];

#[derive(Clone, Copy)]
enum Expected {
    Answers,
    MemoryRefusal,
}

#[derive(Clone, Copy)]
enum Drive {
    One,
    Sequential,
    Metadata,
    RawFunding,
    WriterReopen,
}

struct Row {
    name: &'static str,
    drive: Drive,
    expected: Expected,
    document: fn() -> String,
}

fn padded_document(bytes: usize) -> String {
    let prefix = r#"{"p":""#;
    let suffix = r#""}"#;
    format!(
        "{prefix}{}{suffix}",
        "x".repeat(bytes - prefix.len() - suffix.len())
    )
}

fn scalar_1_mib() -> String {
    format!("\"{}\"", "x".repeat(1_048_574))
}

fn document_64_kib() -> String {
    padded_document(65_536)
}

fn two_fields_64_kib() -> String {
    let field = "x".repeat(65_536);
    format!(r#"{{"left":"{field}","right":"{field}"}}"#)
}

fn zero_array(elements: usize) -> String {
    let mut json = String::with_capacity(elements.saturating_mul(2).saturating_add(1));
    json.push('[');
    for element in 0..elements {
        if element != 0 {
            json.push(',');
        }
        json.push('0');
    }
    json.push(']');
    json
}

fn fitting_array_peak() -> String {
    zero_array(131_073)
}

fn retained_fits_peak_exceeds() -> String {
    let mut json = String::with_capacity(312_147);
    json.push('[');
    for element in 0..131_073 {
        if element != 0 {
            json.push(',');
        }
        if element < 10_000 {
            json.push_str(r#"{"":0}"#);
        } else {
            json.push('0');
        }
    }
    json.push(']');
    json
}

fn growing_escaped_newlines() -> String {
    format!("\"{}\"", r"\n".repeat(200))
}

fn metadata_row() -> String {
    String::new()
}

fn scalar_3_mib() -> String {
    format!("\"{}\"", "x".repeat(3 * 1_048_576 - 2))
}

fn document_16_mib() -> String {
    padded_document(16 * 1_048_576)
}

// The pre-existing file-route counters remain compatibility observations.
// Only this allocator sample can establish funding at native allocation time.
#[derive(Debug)]
struct FundingBudget {
    json: Arc<AtomicUsize>,
    already_funded: usize,
}

impl contextdb_core::read_memory::ReadMemoryBudget for FundingBudget {
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> contextdb_core::Result<()> {
        let used = self.already_funded + self.json.load(Ordering::SeqCst);
        let available = DEFAULT_MEMORY.saturating_sub(used);
        if bytes > available {
            return Err(Error::MemoryBudgetExceeded {
                subsystem: subsystem.into(),
                operation: operation.into(),
                requested_bytes: bytes,
                available_bytes: available,
                budget_limit_bytes: DEFAULT_MEMORY,
                hint: hint.into(),
            });
        }
        self.json.fetch_add(bytes, Ordering::SeqCst);
        Ok(())
    }

    fn release(&self, bytes: usize) {
        let previous = self.json.fetch_sub(bytes, Ordering::SeqCst);
        assert!(bytes <= previous, "release cannot invent refund authority");
    }
}

fn raw_funding(row: &Row) {
    let document = (row.document)();
    let expected: serde_json::Value = serde_json::from_str(&document).expect("fixture JSON");
    let json = Arc::new(AtomicUsize::new(0));
    // Serialized input remains live; include the engine's documented decoder
    // control floor plus an operation control allowance, never lend these
    // already-owned bytes to observed AST/scratch allocations.
    let already_funded = document.capacity() + 4 * std::mem::size_of::<Value>() + 128 + 4096;
    let budget = Arc::new(FundingBudget {
        json: json.clone(),
        already_funded,
    });
    let guard = contextdb_core::types::JsonReadAdmissionGuard::enter(budget);
    let (decoded, peak, unfunded, allocations) =
        super::cursor_survives_writer_handover::append_heap_oracle::measure_funding(
            json.clone(),
            || contextdb_core::types::deserialize_json_in_read_scope(&document),
        );
    drop(guard);
    assert_eq!(
        unfunded, 0,
        "{}: each native allocation, including realloc overlap, must already be funded",
        row.name
    );
    let decoded = decoded.unwrap_or_else(|error| {
        panic!(
            "{}: decode must fit with input/control funded: {error}",
            row.name
        )
    });
    assert_eq!(decoded, expected, "{}: exact decoded payload", row.name);
    assert!(
        allocations > 0 && peak > 0,
        "{}: native allocation oracle participated",
        row.name
    );
    let retained = contextdb_core::types::json_retained_heap_bytes(&decoded);
    assert_eq!(
        json.load(Ordering::SeqCst),
        retained,
        "{}: final charge is transferred retained AST only",
        row.name
    );
    drop(decoded);
    // Raw entrypoint transfers its lease to the caller. Drop native payload
    // first, then return the caller's owned retained credit explicitly.
    assert_eq!(json.fetch_sub(retained, Ordering::SeqCst), retained);
    assert_eq!(
        json.load(Ordering::SeqCst),
        0,
        "{}: raw owner released",
        row.name
    );
    eprintln!(
        "CDB140 ROW {} PASS class=allocation-order peak={peak} unfunded={unfunded} allocations={allocations}",
        row.name
    );
}

fn writer_reopen(row: &Row) {
    let root = tempfile::tempdir().expect("task-scoped writer/reopen fixture");
    let path = root.path().join("large-document.db");
    let document = (row.document)();
    seed(&path, &document, false);
    Database::open(&path)
        .expect("default writer reopens committed 16 MiB JSON document")
        .close()
        .expect("close default reopened writer");
    let mut reopen_options = contextdb_engine::DatabaseOpenOptions::default();
    reopen_options.owner_reads.limits.limits.memory = 64 * 1_048_576;
    let database = Database::open_with_options(&path, reopen_options)
        .expect("reopen committed 16 MiB JSON document with explicit verification owner budget");
    // Qualifies writer commit and reopen only. A full document read would
    // exceed the independently shipped 4 MiB result ceiling.
    // COUNT(id) still decodes the row. Give only this verification read an
    // explicit 64 MiB memory budget; its small result keeps the default result
    // ceiling. This is not a shipped-reader acceptance claim.
    let verification_limits = ReadLimits {
        memory: 64 * 1_048_576,
        ..ReadLimits::default()
    };
    let verification = database
        .read_session(verification_limits)
        .expect("open explicit-budget verification read");
    let answer = verification
        .execute("SELECT COUNT(id) FROM json_decode_rows", &HashMap::new())
        .expect("count persisted row after reopening 16 MiB document");
    assert_eq!(
        answer.rows.len(),
        1,
        "16 MiB writer/reopen returns one count row"
    );
    assert_eq!(
        answer.rows[0].first(),
        Some(&Value::Int64(1)),
        "16 MiB writer/reopen preserves the committed row"
    );
    drop(answer);
    drop(verification);
    database.close().expect("close reopened writer");
    eprintln!(
        "CDB140 ROW {} PASS class=compatibility route=writer-commit-reopen",
        row.name
    );
}

fn seed(path: &Path, document: &str, sequential: bool) {
    let database = Database::open(path).expect("open task-scoped writer");
    database
        .execute(
            "CREATE TABLE json_decode_rows (id INTEGER PRIMARY KEY, doc JSON NOT NULL)",
            &HashMap::new(),
        )
        .expect("create JSON fixture");
    let value = Value::Json(serde_json::from_str(document).expect("well-formed JSON fixture"));
    for id in 1..=if sequential { 2 } else { 1 } {
        database
            .execute(
                "INSERT INTO json_decode_rows VALUES ($id, $doc)",
                &HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("doc".into(), value.clone()),
                ]),
            )
            .expect("persist JSON fixture");
    }
    database.close().expect("close writer before file read");
}

fn session(path: &Path) -> ReadSession {
    let options = ReadSessionOptions {
        memory_limit: Some(DEFAULT_MEMORY),
        limits: ReadLimits {
            memory: ReadLimits::SHIPPED_MEMORY,
            ..ReadLimits::default()
        },
        ..ReadSessionOptions::default()
    };
    ReadSession::open_with_options(path, options).expect("open file route under shipped memory")
}

#[allow(clippy::useless_conversion)] // Preserve the existing test expression and its assertion contract.
fn receipt(reader: &ReadSession) -> (usize, usize, usize, usize) {
    let (usage, held, high_water, _) = reader
        .file_memory_for_test()
        .expect("this proof deliberately uses the file-route accounting receipt");
    (
        usize::try_from(usage.used).expect("accounted bytes fit usize"),
        held,
        high_water,
        reader
            .file_admitted_bytes_for_test()
            .expect("file route reports admitted bytes"),
    )
}

fn resolved_serde_json_features() -> Vec<String> {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("engine crate has workspace root");
    let rustc = Command::new(std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_owned()))
        .arg("-vV")
        .output()
        .expect("identify this test binary's target");
    assert!(rustc.status.success(), "rustc reports its host target");
    let target = String::from_utf8(rustc.stdout)
        .expect("rustc version output is UTF-8")
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .expect("rustc host target")
        .to_owned();
    let output = Command::new(env!("CARGO"))
        .current_dir(workspace)
        .args([
            "metadata",
            "--format-version=1",
            "--locked",
            "--filter-platform",
            &target,
            "--features",
            "contextdb-engine/test-seams",
        ])
        .output()
        .expect("run cargo metadata for the resolved dependency graph");
    assert!(
        output.status.success(),
        "cargo metadata must resolve the workspace: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("cargo metadata is JSON");
    let package = metadata["packages"]
        .as_array()
        .expect("metadata packages")
        .iter()
        .filter(|package| package["name"] == "serde_json")
        .collect::<Vec<_>>();
    assert_eq!(
        package.len(),
        1,
        "metadata must resolve exactly one serde_json package for the decoder"
    );
    let package = package[0];
    assert_eq!(
        package["version"].as_str(),
        Some(AUDITED_SERDE_JSON_VERSION),
        "a serde_json version change requires a fresh scratch-allocation audit"
    );
    let id = package["id"].as_str().expect("serde_json package id");
    let features = metadata["resolve"]["nodes"]
        .as_array()
        .expect("resolved dependency nodes")
        .iter()
        .filter(|node| node["id"] == id)
        .collect::<Vec<_>>();
    assert_eq!(
        features.len(),
        1,
        "metadata must contain exactly one resolved serde_json node"
    );
    let mut features = features[0]["features"]
        .as_array()
        .expect("serde_json resolved features")
        .iter()
        .map(|feature| feature.as_str().expect("feature name").to_owned())
        .collect::<Vec<_>>();
    features.sort();
    features
}

/// JSON decode is charged before allocation and a real
/// ceiling crossing remains `MemoryBudgetExceeded`. Existing tests miss an
/// unfunded serde_json AST decode; decisive mutants are removed charge,
/// under-bound admission, duplicate-occurrence charging, and missing true-up.
/// This cannot be a row of the mapped test because it has one test body and no
/// shared driver. The metadata row rejects a newly unified serde_json feature
/// before its un-audited scratch schedule can be relied on. It asks metadata
/// for this test binary's engine feature selection, so either a missing or an
/// additional resolved serde_json feature is RED rather than silently using a
/// different parser configuration.
/// Success/file-counter rows are compatibility controls, not allocation-order
/// proof. The raw funding row compares funding and actual live native bytes
/// at every allocation boundary; its array growth is decisive for removed,
/// under-bound, and post-construction AST funding mutations.
#[test]
fn json_decode_rows_are_admitted_before_allocation_and_release_to_their_baseline() {
    let rows = [
        Row {
            name: "raw_array_allocation_funded",
            drive: Drive::RawFunding,
            expected: Expected::Answers,
            document: fitting_array_peak,
        },
        Row {
            name: "scalar_3_mib_input_control_funded",
            drive: Drive::RawFunding,
            expected: Expected::Answers,
            document: scalar_3_mib,
        },
        Row {
            name: "document_16_mib_writer_reopen",
            drive: Drive::WriterReopen,
            expected: Expected::Answers,
            document: document_16_mib,
        },
        Row {
            name: "scalar_1_mib",
            drive: Drive::One,
            expected: Expected::Answers,
            document: scalar_1_mib,
        },
        Row {
            name: "document_64_kib",
            drive: Drive::One,
            expected: Expected::Answers,
            document: document_64_kib,
        },
        Row {
            name: "two_fields_64_kib",
            drive: Drive::One,
            expected: Expected::Answers,
            document: two_fields_64_kib,
        },
        Row {
            name: "sequential_row_release",
            drive: Drive::Sequential,
            expected: Expected::Answers,
            document: document_64_kib,
        },
        Row {
            name: "fitting_array_peak",
            drive: Drive::One,
            expected: Expected::Answers,
            document: fitting_array_peak,
        },
        Row {
            name: "growing_escaped_newlines",
            drive: Drive::One,
            expected: Expected::Answers,
            document: growing_escaped_newlines,
        },
        Row {
            name: "retained_fits_peak_exceeds",
            drive: Drive::One,
            expected: Expected::MemoryRefusal,
            document: retained_fits_peak_exceeds,
        },
        Row {
            name: "serde_json_feature_set",
            drive: Drive::Metadata,
            expected: Expected::Answers,
            document: metadata_row,
        },
    ];

    let selected = std::env::var("CDB140_ROW").ok();
    assert!(
        selected
            .as_ref()
            .is_none_or(|name| rows.iter().any(|row| row.name == name)),
        "CDB140_ROW must name an existing row"
    );
    for row in rows {
        // One selected row yields an independent receipt without adding test
        // functions or allowing an earlier failure to mask later rows.
        if selected
            .as_ref()
            .is_some_and(|selected| selected != row.name)
        {
            continue;
        }
        if matches!(row.drive, Drive::RawFunding) {
            raw_funding(&row);
            continue;
        }
        if matches!(row.drive, Drive::WriterReopen) {
            writer_reopen(&row);
            continue;
        }
        if matches!(row.drive, Drive::Metadata) {
            assert_eq!(
                resolved_serde_json_features(),
                AUDITED_SERDE_JSON_FEATURES,
                "{}: a changed resolved serde_json feature set needs a scratch audit before decode admission can be trusted",
                row.name
            );
            eprintln!(
                "CDB140 ROW {} PASS class=dependency-compatibility",
                row.name
            );
            continue;
        }

        let root = tempfile::tempdir().expect("task-scoped store directory");
        let path = root.path().join(format!("{}.db", row.name));
        seed(
            &path,
            &(row.document)(),
            matches!(row.drive, Drive::Sequential),
        );
        let reader = session(&path);
        let baseline = receipt(&reader);
        let ids: &[i64] = match row.drive {
            Drive::One => &[1],
            Drive::Sequential => &[1, 2],
            Drive::Metadata | Drive::RawFunding | Drive::WriterReopen => unreachable!(),
        };

        for id in ids {
            let outcome = reader.execute(
                "SELECT doc FROM json_decode_rows WHERE id = $id",
                &HashMap::from([("id".to_owned(), Value::Int64(*id))]),
            );
            match row.expected {
                Expected::Answers => {
                    let answer = outcome.unwrap_or_else(|error| {
                        panic!("{}: shipped memory must answer, got {error:?}", row.name)
                    });
                    assert_eq!(
                        answer.rows.len(),
                        1,
                        "{}: selected row answers once",
                        row.name
                    );
                    drop(answer);
                    let released = receipt(&reader);
                    assert_eq!(
                        (released.0, released.1),
                        (baseline.0, baseline.1),
                        "{}: dropping each decoded row returns its retained lease to the baseline",
                        row.name
                    );
                    assert!(
                        released.2 >= baseline.2 && released.3 > baseline.3,
                        "{}: the existing accounting receipt must observe admission before the row is released",
                        row.name
                    );
                }
                Expected::MemoryRefusal => {
                    let error = match outcome {
                        Ok(_) => panic!("{}: actual realloc peak crosses shipped memory", row.name),
                        Err(error) => error,
                    };
                    assert!(
                        matches!(error, Error::MemoryBudgetExceeded { .. }),
                        "{}: a real decoder peak is a typed memory refusal, got {error:?}",
                        row.name
                    );
                    assert_eq!(
                        receipt(&reader).0,
                        baseline.0,
                        "{}: refused partial JSON releases its row owner",
                        row.name
                    );
                }
            }
        }
        eprintln!(
            "CDB140 ROW {} PASS class={}",
            row.name,
            if matches!(row.expected, Expected::MemoryRefusal) {
                "refusal-regression"
            } else {
                "compatibility"
            }
        );
    }
}
