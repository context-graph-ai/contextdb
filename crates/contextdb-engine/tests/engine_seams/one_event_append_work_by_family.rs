//! Where one event append's proof work goes, family by family and namespace by
//! namespace.
//!
//! This is the instrument every append repair is graded on, so it is pinned
//! rather than merely printed: a repair that moves work between families
//! without removing it, or that removes a member a reader needs, changes these
//! numbers and has to say so.
//!
//! The fixture is the pinned cg startup schema already in this tree
//! (`tests/fixtures/pinned_cg_startup`, generated from the consumer's own
//! `schema.rs`), installed in one transaction exactly as cg installs it, and
//! the append writes the three tables the consumer's one-event append writes:
//! `events`, `event_anchors` and `event_local_commits`, in one transaction.
//!
//! Three observations are read together, because any one of them alone would
//! be a claim about something other than the append:
//!
//! * the per-family vector — coalesced mutations, node reads and writes, bytes;
//! * the `Status` split by namespace literal, beside the copies the two
//!   fan-out constructors produced;
//! * the page lists the commit itself had to write.
//!
//! The per-family totals are asserted equal to the untagged counters that
//! already existed, so a family tag that leaks or is dropped fails here instead
//! of producing a plausible-looking vector.

#[path = "../fixtures/pinned_cg_startup/mod.rs"]
mod pinned;

use contextdb_core::{TxId, Value};
use contextdb_engine::append_work::{
    self, FamilyWork, IndexPrefixCountTouch, with_coalesced_mutation_observer_for_test,
};
use contextdb_engine::{Database, MaintenancePolicy};
use std::cell::RefCell;
use std::collections::HashMap;

const COMMITTED_MEMBER_CAPACITY: usize = 16;
const COMMITTED_MEMBER_BYTES: usize = 4_096;

#[derive(Clone, Copy)]
struct CommittedMembers {
    keys: [[u8; COMMITTED_MEMBER_BYTES]; COMMITTED_MEMBER_CAPACITY],
    lengths: [usize; COMMITTED_MEMBER_CAPACITY],
    len: usize,
    overflow: bool,
    absent: bool,
}

impl CommittedMembers {
    const fn empty() -> Self {
        Self {
            keys: [[0; COMMITTED_MEMBER_BYTES]; COMMITTED_MEMBER_CAPACITY],
            lengths: [0; COMMITTED_MEMBER_CAPACITY],
            len: 0,
            overflow: false,
            absent: false,
        }
    }

    fn push(&mut self, key: &[u8]) -> bool {
        if self.len == COMMITTED_MEMBER_CAPACITY || key.len() > COMMITTED_MEMBER_BYTES {
            self.overflow = true;
            return false;
        }
        self.keys[self.len][..key.len()].copy_from_slice(key);
        self.lengths[self.len] = key.len();
        self.len += 1;
        true
    }

    fn key(&self, index: usize) -> &[u8] {
        &self.keys[index][..self.lengths[index]]
    }

    fn remove_last(&mut self) {
        assert!(
            self.len > 0,
            "the committed-member fixture produced a member"
        );
        self.len -= 1;
    }
}

thread_local! {
    static OBSERVED_CHANGE_AND_DDL_MEMBERS: RefCell<CommittedMembers> =
        const { RefCell::new(CommittedMembers::empty()) };
}

fn observe_change_and_ddl_member(family: &'static str, key: &[u8], present: bool) {
    if family != "ChangeAndDdl" {
        return;
    }
    OBSERVED_CHANGE_AND_DDL_MEMBERS.with(|members| {
        let mut members = members.borrow_mut();
        if present {
            members.push(key);
        } else {
            members.absent = true;
        }
    });
}

fn observed_change_and_ddl_members(db: &Database, event: usize) -> (TxId, CommittedMembers) {
    OBSERVED_CHANGE_AND_DDL_MEMBERS.with(|members| {
        *members.borrow_mut() = CommittedMembers::empty();
    });
    let members = with_coalesced_mutation_observer_for_test(observe_change_and_ddl_member, || {
        append_event(db, event)
    });
    let observed = OBSERVED_CHANGE_AND_DDL_MEMBERS.with(|members| *members.borrow());
    assert!(
        !observed.overflow,
        "the finite caller-owned committed-member receiver overflowed"
    );
    assert!(
        !observed.absent,
        "the append's committed ChangeAndDdl member was absent from the final coalesced set"
    );
    (members, observed)
}

fn context_uuid() -> uuid::Uuid {
    uuid::Uuid::from_u128(0xc0de_0000_0000_0000_0000_0000_0000_0001)
}

fn event_uuid(event: usize) -> uuid::Uuid {
    uuid::Uuid::from_u128(0xe7e7_0000_0000_0000_0000_0000_0000_0000 + event as u128)
}

fn anchor_uuid(event: usize) -> uuid::Uuid {
    uuid::Uuid::from_u128(0xa4c0_0000_0000_0000_0000_0000_0000_0000 + event as u128)
}

/// cg's startup: every declaration in one transaction, then the host's trigger
/// callbacks, then initialization is complete.
pub(crate) fn declare(db: &Database) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let tx = db.begin().expect("begin the startup schema transaction");
    for (ordinal, ddl) in pinned::LOCAL.iter().enumerate() {
        if let Err(error) = db.execute_in_tx(tx, ddl, &HashMap::new()) {
            db.rollback(tx).expect("roll back a refused startup schema");
            panic!("pinned cg startup ordinal={ordinal} DDL={ddl}: {error}");
        }
    }
    db.commit(tx).expect("commit the whole startup schema once");
    for trigger in db.list_triggers() {
        db.register_trigger_callback(&trigger.name, |_db, _ctx| Ok(()))
            .expect("register a declared trigger callback");
    }
    db.complete_initialization()
        .expect("every trigger callback registered before the first write");
    db.execute(
        "INSERT INTO contexts (id, name, labels, properties, created_at) \
         VALUES ($id, $name, $labels, $properties, $created_at)",
        &HashMap::from([
            ("id".into(), Value::Uuid(context_uuid())),
            ("name".into(), Value::Text("append-family-work".into())),
            ("labels".into(), Value::Json(serde_json::json!([]))),
            ("properties".into(), Value::Json(serde_json::json!({}))),
            ("created_at".into(), Value::TxId(TxId(1))),
        ]),
    )
    .expect("insert the parent Context row");
}

/// `MAINTAIN COUNT ALL PREFIXES` on the three tables a one-event append
/// writes: every prefix of every index those tables declare, which is what
/// the engine maintained unconditionally before the declaration existed.
pub(crate) fn declare_all_prefixes_on_the_append_tables(db: &Database) {
    for table in ["events", "event_anchors", "event_local_commits"] {
        let ddl = format!("ALTER TABLE {table} SET MAINTAIN COUNT ALL PREFIXES");
        db.execute(&ddl, &HashMap::new())
            .unwrap_or_else(|error| panic!("declare every prefix on {table}: {error}"));
    }
}

/// One event: the row, one anchor and the local-commit marker, in one
/// transaction — the consumer's one-event append
/// (`context-graph/crates/context-graph/src/store/event.rs:2190`, `:2218`,
/// `:2276`).
pub(crate) fn append_event(db: &Database, event: usize) -> TxId {
    let tx = db.begin().expect("begin the append transaction");
    db.execute_in_tx(
        tx,
        "INSERT INTO events (id, context_id, connector_id, adapter_version, \
         anchor_declaration_version, source_instance_ref, session_ref, source_event_key, \
         source_position, seq, content_hash, anchor_count, anchor_set_hash, occurred_at, author, \
         kind, render_role, interaction_ref, text, payload, anchors, source_remainder, coverage) \
         VALUES ($id, $context_id, 'claude-code', 'v1', 'v1', $source, $session, $key, \
         $position, $seq, $hash, 1, $hash, $occurred_at, 'agent', 'message', 'primary', NULL, \
         $text, $payload, $anchors, $remainder, 'complete')",
        &HashMap::from([
            ("id".into(), Value::Uuid(event_uuid(event))),
            ("context_id".into(), Value::Uuid(context_uuid())),
            ("source".into(), Value::Text("source-000-0000.jsonl".into())),
            ("session".into(), Value::Text("session-000-0000".into())),
            ("key".into(), Value::Text(format!("source-{event:08x}"))),
            (
                "position".into(),
                Value::Json(serde_json::json!({ "line": event })),
            ),
            ("seq".into(), Value::Int64(event as i64)),
            ("hash".into(), Value::Text(format!("{event:064x}"))),
            ("occurred_at".into(), Value::Timestamp(1_700_000_000_000)),
            ("text".into(), Value::Text("x".repeat(512))),
            ("payload".into(), Value::Json(serde_json::json!({}))),
            ("anchors".into(), Value::Json(serde_json::json!({}))),
            ("remainder".into(), Value::Json(serde_json::json!({}))),
        ]),
    )
    .expect("insert the event row");
    db.execute_in_tx(
        tx,
        "INSERT INTO event_anchors (id, context_id, event_id, correlation_scope_hash, \
         declaration_version, connector_id, source_instance_ref, session_ref, render_role, \
         interaction_ref, key, value, identity_value, original_value) \
         VALUES ($id, $context_id, $event_id, $scope, 'v1', 'claude-code', \
         'source-000-0000.jsonl', 'session-000-0000', 'primary', NULL, 'file', $value, $value, \
         $original)",
        &HashMap::from([
            ("id".into(), Value::Uuid(anchor_uuid(event))),
            ("context_id".into(), Value::Uuid(context_uuid())),
            ("event_id".into(), Value::Uuid(event_uuid(event))),
            ("scope".into(), Value::Text(format!("{event:032x}"))),
            ("value".into(), Value::Text(format!("src/file{event}.rs"))),
            (
                "original".into(),
                Value::Json(serde_json::json!({ "path": event })),
            ),
        ]),
    )
    .expect("insert the anchor row");
    db.execute_in_tx(
        tx,
        "INSERT INTO event_local_commits (event_id, context_id, committed_at) \
         VALUES ($event_id, $context_id, $committed_at)",
        &HashMap::from([
            ("event_id".into(), Value::Uuid(event_uuid(event))),
            ("context_id".into(), Value::Uuid(context_uuid())),
            ("committed_at".into(), Value::TxId(TxId(1))),
        ]),
    )
    .expect("insert the local-commit marker");
    db.commit(tx).expect("commit the one-event append");
    tx
}

struct AppendWork {
    families: Vec<(u8, &'static str, FamilyWork)>,
    namespaces: Vec<(String, u64)>,
    untagged_reads: u64,
    untagged_writes: u64,
    untagged_bytes_read: u64,
    untagged_bytes_written: u64,
    freed_pages: u64,
    allocated_pages: u64,
    commits: u64,
}

impl AppendWork {
    fn total(&self, of: impl Fn(&FamilyWork) -> u64) -> u64 {
        self.families.iter().map(|(_, _, work)| of(work)).sum()
    }

    fn mutations(&self) -> Vec<(&'static str, u64)> {
        self.families
            .iter()
            .map(|(_, name, work)| (*name, work.mutations))
            .collect()
    }

    /// Config keys carry the row's LSN, so a raw `Status` key is grouped by the
    /// config family it belongs to rather than pinned verbatim.
    fn normalized_namespaces(&self) -> Vec<(String, u64)> {
        let mut grouped: std::collections::BTreeMap<String, u64> =
            std::collections::BTreeMap::new();
        for (namespace, count) in &self.namespaces {
            let key = match namespace.strip_prefix("raw:") {
                Some(raw) => format!("raw:{}", raw.split('.').next().unwrap_or(raw)),
                None => namespace.clone(),
            };
            *grouped.entry(key).or_default() += count;
        }
        grouped.into_iter().collect()
    }

    fn node_writes(&self) -> Vec<(&'static str, u64)> {
        self.families
            .iter()
            .map(|(_, name, work)| (*name, work.node_writes))
            .collect()
    }

    fn report(&self, label: &str) -> String {
        let mut lines = vec![format!(
            "{label}: mutations={} node_writes={} node_reads={} bytes_written={} \
             freed_pages={} allocated_pages={} commits={}",
            self.total(|work| work.mutations),
            self.total(|work| work.node_writes),
            self.total(|work| work.node_reads),
            self.total(|work| work.bytes_written),
            self.freed_pages,
            self.allocated_pages,
            self.commits,
        )];
        for (_, name, work) in &self.families {
            lines.push(format!(
                "  {name:<18} mutations={:<4} node_writes={:<5} node_reads={:<5} \
                 bytes_written={:<8} bytes_read={}",
                work.mutations,
                work.node_writes,
                work.node_reads,
                work.bytes_written,
                work.bytes_read,
            ));
        }
        lines.push(format!("  Status by namespace: {:?}", self.namespaces));
        lines.join("\n")
    }
}

fn observe_append(db: &Database, event: usize) -> AppendWork {
    append_work::reset();
    append_event(db, event);
    observed_work()
}

fn observe_append_with_committed_members(
    db: &Database,
    event: usize,
) -> (AppendWork, TxId, CommittedMembers) {
    append_work::reset();
    let (tx, observed) = observed_change_and_ddl_members(db, event);
    (observed_work(), tx, observed)
}

fn observed_work() -> AppendWork {
    let (untagged_reads, untagged_writes, _removes, untagged_bytes_read, untagged_bytes_written) =
        append_work::take_untagged_totals();
    let pages = append_work::take_commit_pages_for_test();
    let copies = append_work::take_page_copies();
    assert_eq!(
        copies.total(),
        pages.freed_pages,
        "every page the commit listed as freed is a page the storage engine copied under some \
         tag: {copies:?}"
    );
    eprintln!(
        "  copy-on-write pages: {} inside a family's walk {:?}, {} outside every family, {} in \
         the commit's own list",
        copies.inside_a_family(),
        copies
            .by_family
            .iter()
            .map(|(_, name, pages)| (*name, *pages))
            .collect::<Vec<_>>(),
        copies.outside_every_family,
        pages.freed_pages,
    );
    AppendWork {
        families: append_work::take_family_work(),
        namespaces: append_work::take_status_namespaces(),
        untagged_reads,
        untagged_writes,
        untagged_bytes_read,
        untagged_bytes_written,
        freed_pages: pages.freed_pages,
        allocated_pages: pages.allocated_pages,
        commits: pages.commits,
    }
}

fn committed_change_and_ddl_members(db: &Database, tx: TxId) -> CommittedMembers {
    let mut expected = CommittedMembers::empty();
    db.__visit_committed_change_and_ddl_members_for_test(
        tx,
        &["events", "event_anchors", "event_local_commits"],
        |key| {
            if expected.push(key) {
                Ok(())
            } else {
                Err(contextdb_core::Error::Other(
                    "the finite caller-owned expected-member receiver overflowed".into(),
                ))
            }
        },
    )
    .expect("derive committed ChangeAndDdl members from authenticated physical data");
    assert!(
        !expected.overflow,
        "the finite caller-owned expected-member receiver overflowed"
    );
    expected
}

fn has_exact_committed_members(expected: &CommittedMembers, observed: &CommittedMembers) -> bool {
    if expected.overflow
        || observed.overflow
        || expected.absent
        || observed.absent
        || expected.len != observed.len
    {
        return false;
    }
    for members in [expected, observed] {
        for left in 0..members.len {
            if (left + 1..members.len).any(|right| members.key(left) == members.key(right)) {
                return false;
            }
        }
    }
    (0..expected.len).all(|index| {
        (0..observed.len)
            .filter(|candidate| expected.key(index) == observed.key(*candidate))
            .count()
            == 1
    })
}

fn assert_committed_member_contract(expected: &CommittedMembers, observed: &CommittedMembers) {
    assert!(
        has_exact_committed_members(expected, observed),
        "the final ChangeAndDdl keys differ from the committed physical members"
    );
}

/// A family tag that leaks past its walk, or is dropped before one, shows up
/// here as an inequality rather than as a plausible vector.
fn assert_self_consistent(work: &AppendWork, label: &str) {
    assert_eq!(
        work.total(|counts| counts.node_writes),
        work.untagged_writes,
        "{label}: every node write is attributed to exactly one family"
    );
    assert_eq!(
        work.total(|counts| counts.node_reads),
        work.untagged_reads,
        "{label}: every node read is attributed to exactly one family"
    );
    assert_eq!(
        work.total(|counts| counts.bytes_written),
        work.untagged_bytes_written,
        "{label}: attributed written bytes equal the untagged total"
    );
    assert_eq!(
        work.total(|counts| counts.bytes_read),
        work.untagged_bytes_read,
        "{label}: attributed read bytes equal the untagged total"
    );
    let status = work
        .families
        .iter()
        .find(|(_, name, _)| *name == "Status")
        .map(|(_, _, counts)| counts.mutations)
        .unwrap_or_default();
    let namespaced: u64 = work.namespaces.iter().map(|(_, count)| count).sum();
    assert_eq!(
        namespaced, status,
        "{label}: the namespace split accounts for every coalesced Status member"
    );
}

#[test]
fn one_event_append_work_by_family() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let db = Database::open(root.path().join("append-family-work.db"))
        .expect("open the writable fixture");
    declare(&db);

    // The very first append also installs what an append reuses afterwards, so
    // the pinned shape is read from appends that follow one.
    append_event(&db, 0);
    let shallow = observe_append(&db, 1);
    for event in 2..10 {
        append_event(&db, event);
    }
    let (deep, deep_tx, observed_members) = observe_append_with_committed_members(&db, 10);

    eprintln!("{}", shallow.report("one-event append at 2 events"));
    eprintln!("{}", deep.report("one-event append at 10 events"));

    assert_self_consistent(&shallow, "at 2 events");
    assert_self_consistent(&deep, "at 10 events");

    assert_eq!(
        shallow.mutations(),
        deep.mutations(),
        "the coalesced mutation vector does not vary with the depth of the store"
    );

    // PINNED against the pinned cg startup schema, one event = events +
    // event_anchors + event_local_commits in one transaction.
    //
    // A 20,010-event benchmark observed 51 mutations (Status 19,
    // SchemaAndIndexes 13, ChangeAndDdl 7, Sync 6, Relational 4,
    // Configuration 2).
    // This fixture is the whole pinned cg startup schema with one three-row
    // append, so it carries more index postings and more prefix counts and its
    // vector is larger. The operation shape is the same one measured here.
    // The pinned cg startup schema declares no `MAINTAIN COUNT` clause, and
    // silence maintains none, so the whole `Status`
    // family is gone from an ordinary append: twenty-six authenticated
    // index-prefix counts that nothing in this fixture reads. The twenty-two
    // index postings that carry the same information are untouched, and every
    // `COUNT(*)` still answers exactly from the index range. What an append
    // costs once a consumer DOES declare the two prefixes cg reads is pinned
    // beside this, in
    // `a_declared_count_is_the_only_count_an_append_writes`.
    assert_eq!(
        deep.mutations(),
        vec![
            ("Relational", 3),
            ("SchemaAndIndexes", 22),
            ("Sync", 3),
            ("ChangeAndDdl", 7),
            ("Configuration", 3),
        ],
        "the per-family coalesced mutation vector of a one-event append"
    );
    let expected_members = committed_change_and_ddl_members(&db, deep_tx);
    assert_committed_member_contract(&expected_members, &observed_members);

    let mut missing_member = observed_members;
    missing_member.remove_last();
    assert!(
        !has_exact_committed_members(&expected_members, &missing_member),
        "removing one actual committed member must fail the exact-member contract"
    );
    let mut duplicate_member = observed_members;
    assert!(
        duplicate_member.len > 1,
        "the fixture has distinct committed members"
    );
    duplicate_member.keys[1] = duplicate_member.keys[0];
    duplicate_member.lengths[1] = duplicate_member.lengths[0];
    assert!(
        !has_exact_committed_members(&expected_members, &duplicate_member),
        "duplicating one actual committed member must fail the exact-member contract"
    );
    let mut wrong_member = observed_members;
    assert!(
        wrong_member.lengths[0] > 0,
        "a committed member key is nonempty before the wrong-member control"
    );
    let original = wrong_member.keys[0][0];
    let replacement = (0_u8..=u8::MAX)
        .find(|candidate| {
            *candidate != original
                && !(0..expected_members.len).any(|index| {
                    expected_members.lengths[index] == wrong_member.lengths[0]
                        && expected_members.keys[index][0] == *candidate
                        && expected_members.keys[index][1..wrong_member.lengths[0]]
                            == wrong_member.keys[0][1..wrong_member.lengths[0]]
                })
        })
        .expect("a byte value outside the authenticated expected member set");
    wrong_member.keys[0][0] = replacement;
    assert!(
        !has_exact_committed_members(&expected_members, &wrong_member),
        "replacing one actual committed member with a non-member must fail the exact-member contract"
    );
    // Every Status member of an ordinary append is an authenticated index
    // prefix count over a column list no declared constraint already bounds,
    // which
    // `RedbPersistence::authenticated_relational_index_prefix_count`
    // (`persistence.rs:4599-4615`) proves by MEMBERSHIP on the read path. The
    // three `sync_creation_lineage.v1.…` config writes this append performs are
    // no longer copied into Status and Sync: each is one Configuration member,
    // and the `status` and `sync` digests derive from the Configuration root.
    // An ordinary append queues no sink event and runs no trigger, so it writes
    // no `sink-queue`, `sink-audit`, `trigger-audit` or `trigger-stamp` member
    // at all.
    assert_eq!(
        deep.normalized_namespaces(),
        Vec::<(String, u64)>::new(),
        "the Status split by namespace: an undeclared table writes no member at all"
    );
    assert!(
        !deep
            .families
            .iter()
            .any(|(_, name, _)| *name == "Policy" || *name == "Graph"),
        "an ordinary append touches neither the schema-record family nor the Graph family: {:?}",
        deep.mutations(),
    );
    assert!(
        deep.freed_pages > shallow.freed_pages,
        "the commit's own copy-on-write page list grows with the store: {} at 2 events, {} at 10",
        shallow.freed_pages,
        deep.freed_pages,
    );
    assert_eq!(
        deep.allocated_pages, 0,
        "an ordinary append holds no savepoint, so redb's allocation tracking is off and the \
         commit writes no allocated-page list"
    );
    // Sync stages three mutations and writes nothing: the per-row sync-source
    // entries a local insert stages are removals of keys the store does not
    // hold, so the family's tree is unchanged. Everything else that is staged
    // is written.
    let writes = deep.node_writes();
    assert_eq!(
        writes
            .iter()
            .find(|(name, _)| *name == "Sync")
            .map(|(_, writes)| *writes),
        Some(0),
        "an append's Sync mutations change nothing: {writes:?}"
    );
    assert!(
        writes
            .iter()
            .filter(|(name, _)| *name != "Sync")
            .all(|(_, writes)| *writes > 0),
        "every other family that staged a mutation wrote a node: {writes:?}"
    );
}

/// What the thirty-five `rel-index-count-v2` members of one event append
/// actually are: one authenticated live-row count per distinct
/// `(table, column list, prefix values)` key.
///
/// A count is keyed by columns and values, not by index, so two indexes that
/// share leading columns share one count and it is maintained once. A count
/// over a column list the table declares a primary key or unique constraint
/// over can only ever be 0 or 1, and the same answer is already available from
/// an exact-key range over the ordered posting tree that
/// `visit_relational_index_postings` (`content_identity.rs:3474`) serves — so
/// storing and proving it separately buys nothing. This test names every count
/// so that claim can be checked rather than argued.
#[test]
fn one_event_append_maintains_a_count_per_index_prefix() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let db = Database::open(root.path().join("index-prefix-counts.db"))
        .expect("open the writable fixture");
    declare(&db);
    // This test is ABOUT what `ALL PREFIXES` means, so the three tables the
    // append writes declare it. Silence would maintain none and there would
    // be nothing here to enumerate.
    declare_all_prefixes_on_the_append_tables(&db);
    append_event(&db, 0);

    append_work::reset();
    append_event(&db, 1);
    let mut counts = append_work::take_index_prefix_counts();
    counts.sort();

    eprintln!("index-prefix counts maintained by one event append:");
    for IndexPrefixCountTouch {
        table,
        prefix_columns,
        uniquely_constrained,
        maintained_by,
        count_after_the_write,
    } in &counts
    {
        eprintln!(
            "  {table:<20} ({}) count={count_after_the_write:?} {} maintained_by={maintained_by:?}",
            prefix_columns.join(", "),
            if *uniquely_constrained {
                "UNIQUELY CONSTRAINED"
            } else {
                "                    "
            },
        );
    }
    let bounded_by_a_constraint = counts
        .iter()
        .filter(|touch| touch.uniquely_constrained)
        .count();
    eprintln!("  totals: {} counts maintained", counts.len());

    assert_eq!(
        counts.len(),
        26,
        "one event append maintains one authenticated count per distinct index-prefix key, and \
         those are every Status member it writes in the rel-index-count-v2 namespace"
    );
    assert_eq!(
        bounded_by_a_constraint, 0,
        "a count over a column list a primary key or unique constraint already bounds at 0 or 1 \
         is not maintained: the index postings hold at most one entry for such a key"
    );
}
