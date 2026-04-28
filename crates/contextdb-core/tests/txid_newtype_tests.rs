use bincode::config::standard;
use bincode::serde::{decode_from_slice, encode_to_vec};
use contextdb_core::{
    AdjEntry, Lsn, RowId, SnapshotId, TxId, Value, VectorEntry, VectorIndexRef, VersionedRow,
    Wallclock,
};
use contextdb_engine::composite_store::ChangeLogEntry;
use contextdb_engine::sync_types::{EdgeChange, RowChange, VectorChange};

// ======== T1 ========

#[test]
fn newtype_u64_does_not_coerce_to_any_identifier() {
    // Current stub: TxId/SnapshotId/RowId/Lsn/Wallclock are real newtypes in types.rs.
    // Impl must keep them as #[repr(transparent)] newtypes so bare u64 does not coerce.
    // Each fixture constructs `let x: u64 = 42;` and passes it where the newtype is expected;
    // compilation must fail with "mismatched types: expected <Newtype>, found u64".
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/fixtures/compile_fail/assign_u64_to_txid.rs");
    t.compile_fail("tests/fixtures/compile_fail/assign_u64_to_snapshotid.rs");
    t.compile_fail("tests/fixtures/compile_fail/assign_u64_to_rowid.rs");
    t.compile_fail("tests/fixtures/compile_fail/assign_u64_to_lsn.rs");
    t.compile_fail("tests/fixtures/compile_fail/assign_u64_to_wallclock.rs");
    // trybuild runs on drop; the pinned .stderr files must match exactly.
}

// ======== T2 ========

#[test]
fn newtype_cross_assignments_do_not_compile() {
    // Current stub: the five newtypes exist as distinct structs. The compiler refuses to
    // convert between them; each fixture constructs one via tuple literal (e.g., `TxId(1)`)
    // and attempts to bind it to a variable of another type.
    // Impl must preserve these distinct types — no From/Into bridges between them beyond
    // the two explicit semantic bridges covered by T3.
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/fixtures/compile_fail/txid_into_lsn.rs");
    t.compile_fail("tests/fixtures/compile_fail/lsn_into_txid.rs");
    t.compile_fail("tests/fixtures/compile_fail/rowid_into_txid.rs");
    t.compile_fail("tests/fixtures/compile_fail/snapshotid_into_rowid.rs");
    t.compile_fail("tests/fixtures/compile_fail/wallclock_into_txid.rs");
    t.compile_fail("tests/fixtures/compile_fail/txid_into_snapshotid.rs");
    t.compile_fail("tests/fixtures/compile_fail/lsn_into_wallclock.rs");
    t.compile_fail("tests/fixtures/compile_fail/rowid_into_lsn.rs");
    t.compile_fail("tests/fixtures/compile_fail/wallclock_into_lsn.rs");
    t.compile_fail("tests/fixtures/compile_fail/snapshotid_into_wallclock.rs");
}

// ======== T3 ========

#[test]
fn semantic_bridges_preserve_inner_value() {
    // Current stub: SnapshotId::from_tx and TxId::from_snapshot are `pub const fn` that
    // re-wrap the inner u64. No arithmetic, no coercion.
    // Impl must keep the bridges as identity re-wraps.

    // Forward: TxId -> SnapshotId.
    let tx = TxId(42);
    let s = SnapshotId::from_tx(tx);
    assert_eq!(s.0, 42, "SnapshotId::from_tx must preserve the inner u64");

    // Inverse: SnapshotId -> TxId.
    let s2 = SnapshotId(100);
    let tx2 = TxId::from_snapshot(s2);
    assert_eq!(
        tx2.0, 100,
        "TxId::from_snapshot must preserve the inner u64"
    );

    // Edge: u64::MAX through both directions.
    let tx_max = TxId(u64::MAX);
    assert_eq!(
        SnapshotId::from_tx(tx_max).0,
        u64::MAX,
        "u64::MAX must survive TxId -> SnapshotId"
    );
    assert_eq!(
        TxId::from_snapshot(SnapshotId(u64::MAX)).0,
        u64::MAX,
        "u64::MAX must survive SnapshotId -> TxId"
    );

    // Edge: 0 through both directions.
    assert_eq!(SnapshotId::from_tx(TxId(0)).0, 0);
    assert_eq!(TxId::from_snapshot(SnapshotId(0)).0, 0);

    // Proof of const-fn and external reachability: compile-pass trybuild fixture lives at
    // tests/fixtures/compile_pass/bridge_from_tx_external.rs and imports via the public
    // `contextdb_core::` path from a separate crate root.
    let t = trybuild::TestCases::new();
    t.pass("tests/fixtures/compile_pass/bridge_from_tx_external.rs");
}

// ======== T4 ========

#[test]
fn wire_value_variants_match_golden_bytes() {
    use uuid::Uuid;

    // Golden fixtures at tests/fixtures/bincode_golden/value_<variant>.bin were captured
    // from main HEAD before any stubs land. The scaffold commit includes them.
    // Encoding each of the nine pre-existing Value variants must match byte-for-byte.
    // Appending Value::TxId as the tenth variant must not renumber discriminants 0..=8.

    let cfg = standard();

    // Discriminant 0: Value::Null
    let bytes = encode_to_vec(Value::Null, cfg).expect("encode Value::Null");
    let golden = std::fs::read("tests/fixtures/bincode_golden/value_null.bin")
        .expect("golden file must exist");
    assert_eq!(bytes, golden, "Value::Null bytes must match golden");

    // Discriminant 1: Value::Bool(true)
    let bytes = encode_to_vec(Value::Bool(true), cfg).expect("encode Value::Bool");
    let golden = std::fs::read("tests/fixtures/bincode_golden/value_bool.bin")
        .expect("golden file must exist");
    assert_eq!(bytes, golden, "Value::Bool(true) bytes must match golden");

    // Discriminant 2: Value::Int64(42)
    let bytes = encode_to_vec(Value::Int64(42), cfg).expect("encode Value::Int64");
    let golden = std::fs::read("tests/fixtures/bincode_golden/value_int64.bin")
        .expect("golden file must exist");
    assert_eq!(bytes, golden, "Value::Int64(42) bytes must match golden");

    // Discriminant 3: Value::Float64(3.14)
    #[allow(clippy::approx_constant)]
    let bytes = encode_to_vec(Value::Float64(3.14f64), cfg).expect("encode Value::Float64");
    let golden = std::fs::read("tests/fixtures/bincode_golden/value_float64.bin")
        .expect("golden file must exist");
    assert_eq!(
        bytes, golden,
        "Value::Float64(3.14) bytes must match golden"
    );

    // Discriminant 4: Value::Text("x")
    let bytes = encode_to_vec(Value::Text("x".to_string()), cfg).expect("encode Value::Text");
    let golden = std::fs::read("tests/fixtures/bincode_golden/value_text.bin")
        .expect("golden file must exist");
    assert_eq!(bytes, golden, "Value::Text(\"x\") bytes must match golden");

    // Discriminant 5: Value::Uuid — fixed bytes for reproducibility.
    let fixed_uuid = Uuid::from_bytes([
        0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
        0x88,
    ]);
    let bytes = encode_to_vec(Value::Uuid(fixed_uuid), cfg).expect("encode Value::Uuid");
    let golden = std::fs::read("tests/fixtures/bincode_golden/value_uuid.bin")
        .expect("golden file must exist");
    assert_eq!(bytes, golden, "Value::Uuid bytes must match golden");

    // Discriminant 6: Value::Timestamp(100)
    let bytes = encode_to_vec(Value::Timestamp(100), cfg).expect("encode Value::Timestamp");
    let golden = std::fs::read("tests/fixtures/bincode_golden/value_timestamp.bin")
        .expect("golden file must exist");
    assert_eq!(
        bytes, golden,
        "Value::Timestamp(100) bytes must match golden"
    );

    // Discriminant 7: Value::Json — fixed JSON string for reproducibility.
    let fixed_json: serde_json::Value = serde_json::json!({"k":"v"});
    let bytes = encode_to_vec(Value::Json(fixed_json), cfg).expect("encode Value::Json");
    let golden = std::fs::read("tests/fixtures/bincode_golden/value_json.bin")
        .expect("golden file must exist");
    assert_eq!(
        bytes, golden,
        "Value::Json({{k:v}}) bytes must match golden"
    );

    // Discriminant 8: Value::Vector — fixed three-element vector.
    let bytes =
        encode_to_vec(Value::Vector(vec![1.0f32, 2.0, 3.0]), cfg).expect("encode Value::Vector");
    let golden = std::fs::read("tests/fixtures/bincode_golden/value_vector.bin")
        .expect("golden file must exist");
    assert_eq!(
        bytes, golden,
        "Value::Vector([1,2,3]) bytes must match golden"
    );
}

// ======== T5 ========

#[test]
fn wire_value_txid_serializes_as_u64() {
    // Current stub: Value::TxId is the tenth Value variant (discriminant 9).
    // TxId is #[repr(transparent)] #[serde(transparent)] around u64, so bincode encodes
    // it as a bare u64 under the variant's discriminant byte.
    // Impl must keep #[serde(transparent)] so Value::TxId(TxId(n)) bytes equal
    // [disc=9] ++ encode_u64(n).

    let cfg = standard();
    let n: u64 = 0x0123_4567_89AB_CDEF;

    let tx_bytes = encode_to_vec(Value::TxId(TxId(n)), cfg).expect("encode Value::TxId");
    let inner_u64_bytes = encode_to_vec(n, cfg).expect("encode bare u64");

    // Discriminant byte: Value::TxId is appended as the tenth variant so its discriminant
    // is 9 (Null=0, Bool=1, Int64=2, Float64=3, Text=4, Uuid=5, Timestamp=6, Json=7,
    // Vector=8, TxId=9). bincode standard config encodes enum discriminants as varint;
    // for a single byte value 9 this is exactly [0x09].
    assert_eq!(
        tx_bytes[0], 9u8,
        "Value::TxId discriminant must be 9 (tenth variant, appended last)"
    );

    // Payload after the discriminant byte must be byte-for-byte identical to `encode(n)`.
    assert_eq!(
        &tx_bytes[1..],
        inner_u64_bytes.as_slice(),
        "Value::TxId payload must be the bare u64 encoding with no newtype wrapping"
    );

    // Full equality as belt-and-braces: the complete encoded form equals
    // [discriminant] ++ inner u64 encoding.
    let mut expected = Vec::with_capacity(1 + inner_u64_bytes.len());
    expected.push(9u8);
    expected.extend_from_slice(&inner_u64_bytes);
    assert_eq!(
        tx_bytes, expected,
        "complete Value::TxId encoding must be [disc=9] ++ encode_u64(n)"
    );
}

// ======== T6 ========

#[test]
fn wire_option_txid_matches_option_u64() {
    // Current stub: TxId is #[serde(transparent)] around u64. Option<TxId> therefore
    // encodes identically to Option<u64> at the bincode level (variant 0 => None, variant
    // 1 => Some followed by the inner encoding — and `transparent` makes the inner shape
    // identical).
    // Impl must keep #[serde(transparent)] so VersionedRow.deleted_tx: Option<TxId>
    // remains wire-compatible with the pre-change Option<u64> shape.

    let cfg = standard();

    // Some branch: Some(TxId(42)) bytes must equal Some(42u64) bytes.
    let some_tx = encode_to_vec(Some(TxId(42)), cfg).expect("encode Some(TxId(42))");
    let some_u64 = encode_to_vec(Some(42u64), cfg).expect("encode Some(42u64)");
    assert_eq!(
        some_tx, some_u64,
        "Some(TxId(42)) bytes must equal Some(42u64) bytes under #[serde(transparent)]"
    );

    // None branch: None::<TxId> bytes must equal None::<u64> bytes.
    let none_tx = encode_to_vec(Option::<TxId>::None, cfg).expect("encode None::<TxId>");
    let none_u64 = encode_to_vec(Option::<u64>::None, cfg).expect("encode None::<u64>");
    assert_eq!(
        none_tx, none_u64,
        "None::<TxId> bytes must equal None::<u64> bytes under #[serde(transparent)]"
    );

    // Edge: u64::MAX in Some survives the niche.
    let some_max_tx = encode_to_vec(Some(TxId(u64::MAX)), cfg).expect("encode Some(u64::MAX)");
    let some_max_u64 = encode_to_vec(Some(u64::MAX), cfg).expect("encode Some(u64::MAX) bare");
    assert_eq!(
        some_max_tx, some_max_u64,
        "u64::MAX must survive Option<TxId>"
    );
}

// ======== T7 ========

#[test]
fn wire_struct_round_trip_byte_identical() {
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use uuid::Uuid;

    // Current stub: every persisted struct has its identifier fields retyped to typed
    // newtypes (TxId / Lsn / RowId / Wallclock) with #[serde(transparent)]. That makes
    // the wire shape byte-identical to a pre-change struct whose fields were bare u64.
    // Impl must keep #[serde(transparent)] on every newtype so the mirror-byte identity
    // continues to hold.

    let cfg = standard();

    // ---------- VersionedRow ----------
    let vrow = VersionedRow {
        row_id: RowId(7),
        values: HashMap::new(),
        created_tx: TxId(3),
        deleted_tx: Some(TxId(5)),
        lsn: Lsn(11),
        created_at: Some(Wallclock(1_700_000_000_000)),
    };
    let vrow_bytes = encode_to_vec(&vrow, cfg).expect("encode VersionedRow");
    let (vrow_back, _): (VersionedRow, usize) =
        decode_from_slice(&vrow_bytes, cfg).expect("decode VersionedRow");
    assert_eq!(vrow_back, vrow, "VersionedRow round-trip must be identity");

    // u64-mirror — field names and order identical to VersionedRow; identifier types
    // collapsed to bare u64. Mirror derives Serialize/Deserialize locally for this test.
    #[derive(Serialize, Deserialize)]
    struct VersionedRowMirror {
        row_id: u64,
        values: HashMap<String, Value>,
        created_tx: u64,
        deleted_tx: Option<u64>,
        lsn: u64,
        #[serde(default)]
        created_at: Option<u64>,
    }
    let vrow_mirror = VersionedRowMirror {
        row_id: 7,
        values: HashMap::new(),
        created_tx: 3,
        deleted_tx: Some(5),
        lsn: 11,
        created_at: Some(1_700_000_000_000),
    };
    let vrow_mirror_bytes = encode_to_vec(&vrow_mirror, cfg).expect("encode mirror");
    assert_eq!(
        vrow_bytes, vrow_mirror_bytes,
        "VersionedRow bytes must equal u64-mirror bytes — #[serde(transparent)] on every newtype"
    );

    // ---------- AdjEntry ----------
    // NodeId and EdgeType are type aliases (Uuid / String), not newtypes.
    let src: contextdb_core::NodeId = Uuid::from_u128(0x1111);
    let tgt: contextdb_core::NodeId = Uuid::from_u128(0x2222);
    let adj = AdjEntry {
        source: src,
        target: tgt,
        edge_type: "parent".to_string(),
        properties: HashMap::new(),
        created_tx: TxId(3),
        deleted_tx: Some(TxId(5)),
        lsn: Lsn(11),
    };
    let adj_bytes = encode_to_vec(&adj, cfg).expect("encode AdjEntry");
    let (adj_back, _): (AdjEntry, usize) =
        decode_from_slice(&adj_bytes, cfg).expect("decode AdjEntry");
    assert_eq!(adj_back, adj, "AdjEntry round-trip must be identity");

    #[derive(Serialize, Deserialize)]
    struct AdjEntryMirror {
        source: Uuid,
        target: Uuid,
        edge_type: String,
        properties: HashMap<String, Value>,
        created_tx: u64,
        deleted_tx: Option<u64>,
        lsn: u64,
    }
    let adj_mirror = AdjEntryMirror {
        source: Uuid::from_u128(0x1111),
        target: Uuid::from_u128(0x2222),
        edge_type: "parent".into(),
        properties: HashMap::new(),
        created_tx: 3,
        deleted_tx: Some(5),
        lsn: 11,
    };
    let adj_mirror_bytes = encode_to_vec(&adj_mirror, cfg).expect("encode AdjEntry mirror");
    assert_eq!(
        adj_bytes, adj_mirror_bytes,
        "AdjEntry bytes must equal u64-mirror bytes"
    );

    // ---------- VectorEntry ----------
    let vec_entry = VectorEntry {
        index: VectorIndexRef::default(),
        row_id: RowId(7),
        vector: vec![1.0f32, 2.0, 3.0],
        created_tx: TxId(3),
        deleted_tx: None,
        lsn: Lsn(11),
    };
    let vec_bytes = encode_to_vec(&vec_entry, cfg).expect("encode VectorEntry");
    let (vec_back, _): (VectorEntry, usize) =
        decode_from_slice(&vec_bytes, cfg).expect("decode VectorEntry");
    assert_eq!(
        vec_back, vec_entry,
        "VectorEntry round-trip must be identity"
    );

    #[derive(Serialize, Deserialize)]
    struct VectorIndexRefMirror {
        table: String,
        column: String,
    }
    #[derive(Serialize, Deserialize)]
    struct VectorEntryMirror {
        index: VectorIndexRefMirror,
        row_id: u64,
        vector: Vec<f32>,
        created_tx: u64,
        deleted_tx: Option<u64>,
        lsn: u64,
    }
    let vec_mirror = VectorEntryMirror {
        index: VectorIndexRefMirror {
            table: String::new(),
            column: String::new(),
        },
        row_id: 7,
        vector: vec![1.0, 2.0, 3.0],
        created_tx: 3,
        deleted_tx: None,
        lsn: 11,
    };
    let vec_mirror_bytes = encode_to_vec(&vec_mirror, cfg).expect("encode VectorEntry mirror");
    assert_eq!(
        vec_bytes, vec_mirror_bytes,
        "VectorEntry bytes must equal u64-mirror bytes"
    );

    // ---------- RowChange ----------
    // NaturalKey is a struct with named fields `column: String` and `value: Value`
    // (NOT a newtype over Vec<Value>). The mirror below replicates that exact shape.
    use contextdb_engine::sync_types::NaturalKey;
    let row_change = RowChange {
        table: "t".into(),
        natural_key: NaturalKey {
            column: "id".to_string(),
            value: Value::Int64(1),
        },
        values: HashMap::new(),
        deleted: false,
        lsn: Lsn(11),
    };
    let rc_bytes = encode_to_vec(&row_change, cfg).expect("encode RowChange");
    let (rc_back, _): (RowChange, usize) =
        decode_from_slice(&rc_bytes, cfg).expect("decode RowChange");
    assert_eq!(rc_back, row_change, "RowChange round-trip must be identity");

    #[derive(Serialize, Deserialize)]
    struct NaturalKeyMirror {
        column: String,
        value: Value,
    }
    #[derive(Serialize, Deserialize)]
    struct RowChangeMirror {
        table: String,
        natural_key: NaturalKeyMirror,
        values: HashMap<String, Value>,
        deleted: bool,
        lsn: u64,
    }
    let rc_mirror = RowChangeMirror {
        table: "t".into(),
        natural_key: NaturalKeyMirror {
            column: "id".to_string(),
            value: Value::Int64(1),
        },
        values: HashMap::new(),
        deleted: false,
        lsn: 11,
    };
    let rc_mirror_bytes = encode_to_vec(&rc_mirror, cfg).expect("encode RowChange mirror");
    assert_eq!(
        rc_bytes, rc_mirror_bytes,
        "RowChange bytes must equal u64-mirror bytes"
    );

    // ---------- EdgeChange ----------
    let edge_change = EdgeChange {
        source: Uuid::from_u128(0x1111),
        target: Uuid::from_u128(0x2222),
        edge_type: "parent".into(),
        properties: HashMap::new(),
        lsn: Lsn(11),
    };
    let ec_bytes = encode_to_vec(&edge_change, cfg).expect("encode EdgeChange");
    let (ec_back, _): (EdgeChange, usize) =
        decode_from_slice(&ec_bytes, cfg).expect("decode EdgeChange");
    assert_eq!(
        ec_back, edge_change,
        "EdgeChange round-trip must be identity"
    );

    #[derive(Serialize, Deserialize)]
    struct EdgeChangeMirror {
        source: Uuid,
        target: Uuid,
        edge_type: String,
        properties: HashMap<String, Value>,
        lsn: u64,
    }
    let ec_mirror = EdgeChangeMirror {
        source: Uuid::from_u128(0x1111),
        target: Uuid::from_u128(0x2222),
        edge_type: "parent".into(),
        properties: HashMap::new(),
        lsn: 11,
    };
    let ec_mirror_bytes = encode_to_vec(&ec_mirror, cfg).expect("encode EdgeChange mirror");
    assert_eq!(
        ec_bytes, ec_mirror_bytes,
        "EdgeChange bytes must equal u64-mirror bytes"
    );

    // ---------- VectorChange ----------
    let vec_change = VectorChange {
        index: VectorIndexRef::default(),
        row_id: RowId(7),
        vector: vec![1.0f32, 2.0, 3.0],
        lsn: Lsn(11),
    };
    let vc_bytes = encode_to_vec(&vec_change, cfg).expect("encode VectorChange");
    let (vc_back, _): (VectorChange, usize) =
        decode_from_slice(&vc_bytes, cfg).expect("decode VectorChange");
    assert_eq!(
        vc_back, vec_change,
        "VectorChange round-trip must be identity"
    );

    #[derive(Serialize, Deserialize)]
    struct VectorChangeMirror {
        index: VectorIndexRefMirror,
        row_id: u64,
        vector: Vec<f32>,
        lsn: u64,
    }
    let vc_mirror = VectorChangeMirror {
        index: VectorIndexRefMirror {
            table: String::new(),
            column: String::new(),
        },
        row_id: 7,
        vector: vec![1.0, 2.0, 3.0],
        lsn: 11,
    };
    let vc_mirror_bytes = encode_to_vec(&vc_mirror, cfg).expect("encode VectorChange mirror");
    assert_eq!(
        vc_bytes, vc_mirror_bytes,
        "VectorChange bytes must equal u64-mirror bytes"
    );

    // ---------- ChangeLogEntry::RowInsert ----------
    let cle_row = ChangeLogEntry::RowInsert {
        table: "t".into(),
        row_id: RowId(7),
        lsn: Lsn(11),
    };
    let cle_bytes = encode_to_vec(&cle_row, cfg).expect("encode ChangeLogEntry::RowInsert");
    let (cle_back, _): (ChangeLogEntry, usize) =
        decode_from_slice(&cle_bytes, cfg).expect("decode ChangeLogEntry");
    assert_eq!(
        cle_back, cle_row,
        "ChangeLogEntry round-trip must be identity"
    );

    // Mirror mirrors only the single RowInsert variant (discriminant 0) — sufficient to
    // pin the typed fields inside the variant against a bare-u64 equivalent.
    #[derive(Serialize, Deserialize)]
    enum ChangeLogEntryMirror {
        RowInsert {
            table: String,
            row_id: u64,
            lsn: u64,
        },
    }
    let cle_mirror = ChangeLogEntryMirror::RowInsert {
        table: "t".into(),
        row_id: 7,
        lsn: 11,
    };
    let cle_mirror_bytes =
        encode_to_vec(&cle_mirror, cfg).expect("encode ChangeLogEntry::RowInsert mirror");
    assert_eq!(
        cle_bytes, cle_mirror_bytes,
        "ChangeLogEntry::RowInsert bytes must equal u64-mirror bytes"
    );
}

// ======== T26 ========

#[test]
fn value_estimated_bytes_txid_counted() {
    // Current stub: Value::estimated_bytes() has arm `Value::TxId(_) => 0` — intentionally
    // wrong so this test fails on assert_eq!(actual, 16).
    // Impl must change the stub arm to return 16, matching Int64/Timestamp/Float64.
    // Test also pins Int64 and Timestamp at 16 as a symmetry regression guard — if impl
    // accidentally changes those too, the symmetry asserts fire.

    assert_eq!(
        Value::TxId(TxId(42)).estimated_bytes(),
        16,
        "Value::TxId(_) must report 16 bytes (matches Int64/Timestamp). Stub returns 0; impl must return 16."
    );

    // Symmetry regression: Int64 and Timestamp are already 16 today.
    assert_eq!(
        Value::Int64(42).estimated_bytes(),
        16,
        "Value::Int64(_) must report 16 bytes (symmetry gate)"
    );
    assert_eq!(
        Value::Timestamp(42).estimated_bytes(),
        16,
        "Value::Timestamp(_) must report 16 bytes (symmetry gate)"
    );

    // Edge inputs do not change the reported size — estimated_bytes is a per-variant
    // constant for fixed-width payloads.
    assert_eq!(Value::TxId(TxId(0)).estimated_bytes(), 16);
    assert_eq!(Value::TxId(TxId(u64::MAX)).estimated_bytes(), 16);
}

// ======== T29 ========

#[test]
fn from_raw_wire_reachable_from_serde() {
    // Current stub: #[serde(transparent)] on TxId(pub u64). Deserialize lands on the
    // tuple-struct constructor, which is semantically identical to from_raw_wire.
    // Impl must keep #[serde(transparent)] and keep from_raw_wire as `pub const fn` so
    // wire-level payloads produce a well-formed TxId without any private-constructor gate.

    let cfg = standard();
    let original = Value::TxId(TxId(0xDEAD_BEEF));

    // Encode.
    let bytes = encode_to_vec(&original, cfg).expect("encode Value::TxId(TxId(0xDEAD_BEEF))");

    // Decode — no panic, no error.
    let (decoded, consumed): (Value, usize) =
        decode_from_slice(&bytes, cfg).expect("decode Value from bincode payload must succeed");

    // Decoded value must be exactly the original.
    assert_eq!(
        decoded, original,
        "round-tripped Value::TxId must equal the original (0xDEAD_BEEF)"
    );

    // The decode must have consumed all bytes — no trailing garbage and no short reads.
    assert_eq!(
        consumed,
        bytes.len(),
        "decode_from_slice must consume the full encoded payload"
    );

    // Destructure to prove the inner u64 is exactly 0xDEAD_BEEF and the variant is TxId.
    match decoded {
        Value::TxId(TxId(n)) => assert_eq!(
            n, 0xDEAD_BEEF_u64,
            "decoded inner u64 must equal 0xDEAD_BEEF"
        ),
        other => panic!("expected Value::TxId, got {other:?}"),
    }
}

// ======== TU2 ========

#[test]
fn display_impl_forwards_to_inner_u64() {
    use contextdb_core::{Lsn, RowId, SnapshotId, TxId, Wallclock};

    let tx = TxId(42);
    assert_eq!(
        format!("{}", tx),
        "42",
        "Display(TxId) must forward to inner u64"
    );
    assert_eq!(
        format!("{:?}", tx),
        "TxId(42)",
        "Debug(TxId) retains wrapper form"
    );

    let s = SnapshotId(42);
    assert_eq!(
        format!("{}", s),
        "42",
        "Display(SnapshotId) must forward to inner u64"
    );
    assert_eq!(
        format!("{:?}", s),
        "SnapshotId(42)",
        "Debug(SnapshotId) retains wrapper form"
    );

    let r = RowId(42);
    assert_eq!(
        format!("{}", r),
        "42",
        "Display(RowId) must forward to inner u64"
    );
    assert_eq!(
        format!("{:?}", r),
        "RowId(42)",
        "Debug(RowId) retains wrapper form"
    );

    let l = Lsn(42);
    assert_eq!(
        format!("{}", l),
        "42",
        "Display(Lsn) must forward to inner u64"
    );
    assert_eq!(
        format!("{:?}", l),
        "Lsn(42)",
        "Debug(Lsn) retains wrapper form"
    );

    let w = Wallclock(42);
    assert_eq!(
        format!("{}", w),
        "42",
        "Display(Wallclock) must forward to inner u64"
    );
    assert_eq!(
        format!("{:?}", w),
        "Wallclock(42)",
        "Debug(Wallclock) retains wrapper form"
    );
}

// ======== TU5 ========

#[test]
fn no_from_u64_for_typed_identifiers() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/fixtures/compile_fail/no_from_u64_for_txid.rs");
    t.compile_fail("tests/fixtures/compile_fail/no_from_u64_for_snapshot_id.rs");
    t.compile_fail("tests/fixtures/compile_fail/no_from_u64_for_row_id.rs");
    t.compile_fail("tests/fixtures/compile_fail/no_from_u64_for_lsn.rs");
    t.compile_fail("tests/fixtures/compile_fail/no_from_u64_for_wallclock.rs");
}

// ======== TU9 ========

#[test]
fn semantic_bridges_are_inline_and_zero_cost() {
    use regex::Regex;

    let types_rs = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("types.rs");
    let body = std::fs::read_to_string(&types_rs)
        .unwrap_or_else(|e| panic!("read {}: {e}", types_rs.display()));

    // `SnapshotId::from_tx` — declared as `pub const fn from_tx`, preceded by `#[inline]`.
    let from_tx = Regex::new(r"#\[inline\]\s*(?:\n|\r\n)\s*pub\s+const\s+fn\s+from_tx\s*\(")
        .expect("from_tx regex compiles");
    assert!(
        from_tx.is_match(&body),
        "SnapshotId::from_tx must be `#[inline]\\npub const fn from_tx(...)`; not found in types.rs"
    );

    // `TxId::from_snapshot` — same shape.
    let from_snap =
        Regex::new(r"#\[inline\]\s*(?:\n|\r\n)\s*pub\s+const\s+fn\s+from_snapshot\s*\(")
            .expect("from_snapshot regex compiles");
    assert!(
        from_snap.is_match(&body),
        "TxId::from_snapshot must be `#[inline]\\npub const fn from_snapshot(...)`; not found in types.rs"
    );

    // Runtime sanity: bridges round-trip the inner u64 exactly.
    use contextdb_core::{SnapshotId, TxId};
    assert_eq!(SnapshotId::from_tx(TxId(7)).0, 7);
    assert_eq!(TxId::from_snapshot(SnapshotId(7)).0, 7);
}

// ======== TU10 ========

#[test]
fn wallclock_seam_enables_clock_injection() {
    use contextdb_core::Wallclock;

    // Install mock clock returning a fixed value.
    Wallclock::set_test_clock(|| 2000);
    let mocked = Wallclock::now();
    assert_eq!(
        mocked,
        Wallclock(2000),
        "after set_test_clock(|| 2000), Wallclock::now() must return Wallclock(2000); got {mocked:?}"
    );

    // Uninstall mock; subsequent call must NOT return 2000.
    Wallclock::clear_test_clock();
    let real = Wallclock::now();
    assert_ne!(
        real,
        Wallclock(2000),
        "after clear_test_clock, Wallclock::now() must return real system time, not the mock 2000; got {real:?}"
    );

    // Real-time sanity: post-clear value is within plausible epoch range (>= year-2000 in ms).
    // 946684800000 = 2000-01-01T00:00:00Z in ms since epoch.
    assert!(
        real.0 >= 946684800000,
        "post-clear Wallclock::now() must be a real system-time reading; got {real:?}"
    );
}
