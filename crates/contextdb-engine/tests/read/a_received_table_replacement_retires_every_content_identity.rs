//! A received table drop or replacement leaves a committed identity that
//! describes exactly what the store holds.
//!
//! The receiver installs an authenticated schema unit by rewriting every
//! affected table from a staged image. Whatever the old generation stored --
//! local rows, synced rows and their sync provenance, current vectors and
//! their versions, index postings and counts, links -- has to leave the
//! committed identity in the same Redb transaction that removes it from the
//! store. A DDL-only drop carries no row deletes of its own, so nothing in the
//! arriving unit names those members; the receiver has to find them itself.
//!
//! Every check here recomputes the family roots from the committed store with
//! the independent oracle in `content_identity_independent_contract.rs` and
//! compares them with the maintained roots and the root a reader
//! authenticates, after the drop, after an empty recreation, after new rows in
//! the recreated table, after a restart, after a failed durable commit and
//! after a crash on either side of the durable commit.

use crate::content_identity_independent_contract::independent_typed_digest;
use contextdb_core::{Incarnation, Lsn, TenantId, Value};
use contextdb_engine::Database;
use contextdb_engine::ReadSession;
use contextdb_engine::direct_file_reader::test_seams::DirectTypedImageDigest;
use contextdb_engine::protocol::WireDdlProvenance;
use contextdb_engine::read_content_test_adapter::{
    CommittedContentForTest, committed_content_for_test,
};
use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write as _};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, mpsc};
use std::time::Duration;
use uuid::Uuid;

const TENANT: &str = "received-replacement-identity";
const SOURCE_INCARNATION: Incarnation = Incarnation(1);
const CHILD_ROOT_ENV: &str = "CONTEXTDB_RECEIVED_REPLACEMENT_CHILD_ROOT";
const CHILD_BOUNDARY_ENV: &str = "CONTEXTDB_RECEIVED_REPLACEMENT_CHILD_BOUNDARY";
const CHILD_SINCE_ENV: &str = "CONTEXTDB_RECEIVED_REPLACEMENT_CHILD_SINCE";
const BOUNDARY_MARKER: &str = "RECEIVED_REPLACEMENT_BOUNDARY";
const CHILD_TIMEOUT: Duration = Duration::from_secs(60);

const WIDGETS: &str = "CREATE TABLE widgets (id UUID PRIMARY KEY, body TEXT NOT NULL, \
                       embedding VECTOR(2))";
const WIDGETS_INDEX: &str = "CREATE INDEX widgets_body ON widgets (body ASC)";
const LINKS: &str = "CREATE TABLE widget_links (id UUID PRIMARY KEY, source_id UUID NOT NULL, \
                     target_id UUID NOT NULL, edge_type TEXT NOT NULL)";

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn execute(database: &Database, sql: &str, params: HashMap<String, Value>) {
    database
        .execute(sql, &params)
        .unwrap_or_else(|error| panic!("fixture statement `{sql}`: {error}"));
}

fn insert_widget(database: &Database, id: u128, body: &str, embedding: [f32; 2]) {
    execute(
        database,
        "INSERT INTO widgets (id, body, embedding) VALUES ($id, $body, $embedding)",
        HashMap::from([
            ("id".to_owned(), Value::Uuid(Uuid::from_u128(id))),
            ("body".to_owned(), Value::Text(body.to_owned())),
            ("embedding".to_owned(), Value::Vector(embedding.to_vec())),
        ]),
    );
}

fn insert_link(database: &Database, id: u128, source: u128, target: u128) {
    execute(
        database,
        "INSERT INTO widget_links (id, source_id, target_id, edge_type) \
         VALUES ($id, $source, $target, 'WIDGET_LINK')",
        HashMap::from([
            ("id".to_owned(), Value::Uuid(Uuid::from_u128(id))),
            ("source".to_owned(), Value::Uuid(Uuid::from_u128(source))),
            ("target".to_owned(), Value::Uuid(Uuid::from_u128(target))),
        ]),
    );
}

/// Derive the wire provenance the authoring store attaches to a schema-only
/// unit and install it through the authenticated received-schema door.
fn received(author: &Database, changes: &ChangeSet) -> Vec<WireDdlProvenance> {
    static AUTHOR: std::sync::OnceLock<contextdb_engine::identity::FabricIdentity> =
        std::sync::OnceLock::new();
    let identity = AUTHOR.get_or_init(contextdb_engine::identity::FabricIdentity::generate);
    {
        let seam_read_memory = author
            .declared_reader_memory()
            .expect("admit the fixture operation reader");
        author.outbound_ddl_provenance_for_test(
            changes,
            &TenantId::from(TENANT),
            &identity.node_id(),
            SOURCE_INCARNATION,
            &|bytes| Ok(identity.sign_for_test(bytes)),
            seam_read_memory,
        )
    }
    .expect("the authoring store produces canonical DDL provenance for this schema unit")
}

fn apply_received(
    receiver: &Database,
    changes: ChangeSet,
    provenance: Vec<WireDdlProvenance>,
) -> contextdb_core::Result<()> {
    let source = provenance
        .first()
        .and_then(|entry| entry.author.as_ref())
        .expect("signed received schema has an author")
        .node_id
        .clone();
    {
        let seam_read_memory = receiver
            .declared_reader_memory()
            .expect("admit the fixture operation reader");
        let mut seam_copy_owner =
            contextdb_engine::vector_read_memory::admission(seam_read_memory.clone());
        receiver.apply_received_schema_for_test(
            changes,
            provenance,
            TenantId::from(TENANT),
            &source,
            SOURCE_INCARNATION,
            &mut seam_copy_owner,
            seam_read_memory,
        )
    }
    .map(|_| ())
}

struct Paths {
    _root: tempfile::TempDir,
    author: PathBuf,
    receiver: PathBuf,
}

impl Paths {
    fn root(&self) -> &Path {
        self._root.path()
    }
}

fn paths_in(root: &Path) -> (PathBuf, PathBuf) {
    (root.join("author.db"), root.join("receiver.db"))
}

/// Build a receiver whose `widgets` and `widget_links` tables carry every
/// content family a replacement must retire, and an author that is about to
/// drop both, one table per unit. Returns the author's frontier before each
/// drop.
fn prepared() -> (Paths, DropFrontiers) {
    let root = tempfile::TempDir::new().expect("task-scoped replacement directory");
    let (author_path, receiver_path) = paths_in(root.path());
    let author = Database::open(&author_path).expect("open the authoring store");
    let receiver = Database::open(&receiver_path).expect("open the receiving store");

    let tx = author.begin().expect("begin the authored schema");
    for ddl in [WIDGETS, WIDGETS_INDEX, LINKS] {
        author
            .execute_in_tx(tx, ddl, &empty())
            .unwrap_or_else(|error| panic!("author `{ddl}`: {error}"));
    }
    author.commit(tx).expect("commit the authored schema");
    let schema = author
        .changes_since(Lsn(0))
        .expect("read the declared changes");
    assert_eq!(schema.ddl.len(), 3, "one authored schema unit");
    let provenance = received(&author, &schema);
    apply_received(&receiver, schema, provenance).expect("the receiver installs the schema");

    // Rows the receiver pulls from the author: they arrive with vectors and
    // per-row sync provenance.
    let after_schema = author.current_lsn();
    insert_widget(&author, 0xa1, "synced-one", [1.0, 0.0]);
    insert_widget(&author, 0xa2, "synced-two", [0.0, 1.0]);
    insert_link(&author, 0xa3, 0xa1, 0xa2);
    let rows = author
        .changes_since(after_schema)
        .expect("read the declared changes");
    assert!(
        !rows.rows.is_empty() && !rows.vectors.is_empty(),
        "the pulled unit carries rows and vectors"
    );
    receiver
        .apply_changes(rows, &ConflictPolicies::uniform(ConflictPolicy::ServerWins))
        .expect("the receiver applies the author's rows");

    // Rows the receiver authors itself, and a second vector version of a
    // synced row.
    insert_widget(&receiver, 0xb1, "local-one", [0.5, 0.5]);
    insert_widget(&receiver, 0xb2, "local-two", [0.25, 0.75]);
    insert_link(&receiver, 0xb3, 0xb1, 0xa1);
    execute(
        &receiver,
        "UPDATE widgets SET embedding = $embedding WHERE id = $id",
        HashMap::from([
            ("id".to_owned(), Value::Uuid(Uuid::from_u128(0xa1))),
            ("embedding".to_owned(), Value::Vector(vec![0.75, 0.25])),
        ]),
    );
    receiver.close().expect("release the receiving store");

    // Two authored units: the vector-bearing, indexed table first, then the
    // link table.
    let before_drop = author.current_lsn();
    execute(&author, "DROP TABLE widgets", empty());
    let between_drops = author.current_lsn();
    execute(&author, "DROP TABLE widget_links", empty());
    author.close().expect("release the authoring store");

    let content = {
        let declared_read_memory =
            contextdb_engine::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                contextdb_engine::OwnerReadConfig::default()
                    .limits
                    .limits
                    .memory,
            );
        let operation_read_memory = declared_read_memory
            .child()
            .expect("admit the committed-content observation owner");
        committed_content_for_test(&receiver_path, operation_read_memory, declared_read_memory)
    }
    .expect("inspect the receiver");
    assert!(
        content
            .relational_rows
            .iter()
            .filter(|row| row.table == "widgets")
            .count()
            == 4,
        "the receiver holds synced and local widgets before the drop"
    );
    assert!(
        !content.sync.sources.is_empty(),
        "the synced rows carry per-row sync provenance before the drop"
    );
    assert!(
        content
            .vectors
            .iter()
            .filter(|vector| vector.index.table == "widgets")
            .count()
            >= 4,
        "every widget carries a current vector before the drop"
    );
    assert!(
        !content.graph_edges.is_empty(),
        "the link rows are graph edges before the drop"
    );
    assert_identity_describes_the_store(&receiver_path, "before the drop");
    (
        Paths {
            _root: root,
            author: author_path,
            receiver: receiver_path,
        },
        DropFrontiers {
            before_drop,
            between_drops,
        },
    )
}

#[derive(Clone, Copy)]
struct DropFrontiers {
    before_drop: Lsn,
    between_drops: Lsn,
}

/// The authored schema statements committed after `after` and at or before
/// `through`, as the one schema unit a source sends for them.
fn authored_unit(author: &Database, after: Lsn, through: Option<Lsn>) -> ChangeSet {
    let mut changes = author
        .changes_since(after)
        .expect("read the declared changes");
    let (ddl_lsn, ddl): (Vec<_>, Vec<_>) = changes
        .ddl_lsn
        .iter()
        .copied()
        .zip(changes.ddl.drain(..))
        .filter(|(lsn, _)| through.is_none_or(|through| *lsn <= through))
        .unzip();
    changes.ddl = ddl;
    changes.ddl_lsn = ddl_lsn;
    assert_eq!(changes.ddl.len(), 1, "one authored DROP TABLE per unit");
    changes
}

/// The two authored drop units, widgets first, each with the provenance its
/// source attaches.
fn drop_units(
    author_path: &Path,
    frontiers: DropFrontiers,
) -> [(ChangeSet, Vec<WireDdlProvenance>); 2] {
    let author = Database::open(author_path).expect("reopen the authoring store");
    let units = [
        authored_unit(
            &author,
            frontiers.before_drop,
            Some(frontiers.between_drops),
        ),
        authored_unit(&author, frontiers.between_drops, None),
    ]
    .map(|changes| {
        let provenance = received(&author, &changes);
        (changes, provenance)
    });
    author.close().expect("release the authoring store");
    units
}

fn families(digest: &DirectTypedImageDigest) -> [(&'static str, [u8; 32]); 11] {
    [
        ("relational", digest.relational),
        ("graph", digest.graph),
        ("f32 vectors", digest.f32_vectors),
        ("sq8 vectors", digest.sq8_vectors),
        ("sq4 vectors", digest.sq4_vectors),
        ("schema and indexes", digest.schema_and_indexes),
        ("policy", digest.policy),
        ("sync", digest.sync),
        ("change and ddl", digest.change_and_ddl),
        ("configuration", digest.configuration),
        ("status", digest.status),
    ]
}

/// Recompute every family root from the committed store and require the
/// maintained roots, and the root a reader authenticates, to equal them.
fn assert_identity_describes_the_store(path: &Path, phase: &str) -> CommittedContentForTest {
    let content = {
        let declared_read_memory =
            contextdb_engine::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                contextdb_engine::OwnerReadConfig::default()
                    .limits
                    .limits
                    .memory,
            );
        let operation_read_memory = declared_read_memory
            .child()
            .expect("admit the committed-content observation owner");
        committed_content_for_test(path, operation_read_memory, declared_read_memory)
    }
    .expect("inspect the committed store");
    let expected = independent_typed_digest(&content);
    let stale = families(&content.maintained_digest)
        .into_iter()
        .zip(families(&expected))
        .filter(|((_, maintained), (_, recomputed))| maintained != recomputed)
        .map(|((name, _), _)| name)
        .collect::<Vec<_>>();
    assert!(
        stale.is_empty(),
        "{phase}: the committed identity must describe exactly what the store holds; \
         these family roots disagree with the store: {stale:?}"
    );
    assert_eq!(content.maintained_digest, expected, "{phase}");
    let reader = ReadSession::open(path).expect("open the committed store as a reader");
    assert_eq!(
        reader
            .file_snapshot_for_test()
            .expect("the file route exposes its authenticated identity")
            .image_id,
        expected.complete,
        "{phase}: a reader authenticates the root recomputed from the store"
    );
    drop(reader);
    content
}

/// What a received drop leaves: nothing of `widgets` and, once its own drop
/// has arrived too, nothing of `widget_links` -- not its rows, not its links.
fn assert_widgets_gone(content: &CommittedContentForTest, links_dropped: bool, phase: &str) {
    assert!(
        !content
            .relational_rows
            .iter()
            .any(|row| row.table == "widgets"),
        "{phase}: the dropped table's rows are gone"
    );
    assert!(
        !content
            .vectors
            .iter()
            .any(|vector| vector.index.table == "widgets"),
        "{phase}: the dropped table's vectors are gone"
    );
    if links_dropped {
        assert!(
            !content
                .relational_rows
                .iter()
                .any(|row| row.table == "widget_links"),
            "{phase}: the dropped link table's rows are gone"
        );
        assert!(
            content.graph_edges.is_empty(),
            "{phase}: the dropped link table's links are gone"
        );
        // Every synced row lived in a dropped table.
        assert!(
            content.sync.sources.is_empty(),
            "{phase}: the dropped tables' sync provenance is gone"
        );
    }
}

fn restart(path: &Path) {
    Database::open(path)
        .expect("reopen the receiving store")
        .close()
        .expect("release the reopened store");
}

#[test]
fn a_received_drop_retires_rows_provenance_vectors_and_links_from_the_identity() {
    // Writers this proof opens are counted process-wide by the direct
    // reader contract, which runs under this lock.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    let (paths, frontiers) = prepared();
    let receiver = Database::open(&paths.receiver).expect("reopen to receive the drops");
    for (changes, provenance) in drop_units(&paths.author, frontiers) {
        apply_received(&receiver, changes, provenance).expect("the received drop installs");
    }
    receiver
        .close()
        .expect("release the receiver after the drops");

    let dropped = assert_identity_describes_the_store(&paths.receiver, "after the drops");
    assert_widgets_gone(&dropped, true, "after the drops");
    restart(&paths.receiver);
    let restarted = assert_identity_describes_the_store(&paths.receiver, "after a restart");
    assert_eq!(
        restarted.maintained_digest, dropped.maintained_digest,
        "a restart changes nothing the drop committed"
    );

    // Empty recreation under the old name, then new rows in it.
    let author = Database::open(&paths.author).expect("reopen the authoring store");
    let before_recreate = author.current_lsn();
    execute(&author, WIDGETS, empty());
    let recreate = author
        .changes_since(before_recreate)
        .expect("read the declared changes");
    let provenance = received(&author, &recreate);
    author.close().expect("release the authoring store");
    let receiver = Database::open(&paths.receiver).expect("reopen to receive the recreate");
    apply_received(&receiver, recreate, provenance).expect("the received recreate installs");
    receiver
        .close()
        .expect("release the receiver after the recreate");
    let recreated =
        assert_identity_describes_the_store(&paths.receiver, "after an empty recreation");
    assert_widgets_gone(&recreated, true, "after an empty recreation");

    let receiver = Database::open(&paths.receiver).expect("reopen to write the new generation");
    insert_widget(&receiver, 0xc1, "new-generation", [0.1, 0.9]);
    receiver.close().expect("release the receiver");
    let refilled =
        assert_identity_describes_the_store(&paths.receiver, "after a row in the recreated table");
    assert_eq!(
        refilled
            .relational_rows
            .iter()
            .filter(|row| row.table == "widgets")
            .count(),
        1,
        "only the new generation's row is stored"
    );
    restart(&paths.receiver);
    assert_identity_describes_the_store(&paths.receiver, "after the final restart");
}

#[test]
fn a_received_drop_whose_durable_commit_fails_leaves_the_old_identity_and_store() {
    // Writers this proof opens are counted process-wide by the direct
    // reader contract, which runs under this lock.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    let (paths, frontiers) = prepared();
    let before = {
        let declared_read_memory =
            contextdb_engine::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                contextdb_engine::OwnerReadConfig::default()
                    .limits
                    .limits
                    .memory,
            );
        let operation_read_memory = declared_read_memory
            .child()
            .expect("admit the committed-content observation owner");
        committed_content_for_test(&paths.receiver, operation_read_memory, declared_read_memory)
    }
    .expect("inspect the receiver");
    let [(changes, provenance), _] = drop_units(&paths.author, frontiers);
    let receiver = Database::open(&paths.receiver).expect("reopen to receive the drop");
    receiver.fail_next_received_schema_durable_commit_for_test();
    apply_received(&receiver, changes, provenance)
        .expect_err("the injected durable-commit failure refuses the received drop");
    assert!(
        receiver.table_meta("widgets").is_some(),
        "a refused received drop publishes nothing"
    );
    receiver
        .close()
        .expect("release the receiver after the refusal");
    let after = assert_identity_describes_the_store(&paths.receiver, "after a refused drop");
    assert_eq!(
        after.maintained_digest, before.maintained_digest,
        "a refused received drop leaves the committed identity exactly as it was"
    );
    assert_eq!(
        after.relational_rows, before.relational_rows,
        "and the stored rows exactly as they were"
    );
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CrashBoundary {
    BeforeDurableCommit,
    AfterDurableBeforePublication,
}

impl CrashBoundary {
    fn name(self) -> &'static str {
        match self {
            Self::BeforeDurableCommit => "before-durable-commit",
            Self::AfterDurableBeforePublication => "after-durable-before-publication",
        }
    }

    fn parse(value: &str) -> Self {
        [
            Self::BeforeDurableCommit,
            Self::AfterDurableBeforePublication,
        ]
        .into_iter()
        .find(|boundary| boundary.name() == value)
        .unwrap_or_else(|| panic!("unknown received-replacement boundary {value}"))
    }
}

/// Runs only as the child of `crash_at`: installs the received drop and stops
/// at the named boundary until the parent kills it.
#[test]
fn received_replacement_crash_child() {
    let Some(root) = std::env::var_os(CHILD_ROOT_ENV) else {
        return;
    };
    let boundary = CrashBoundary::parse(
        &std::env::var(CHILD_BOUNDARY_ENV).expect("child boundary environment is present"),
    );
    let frontier = |at: usize| {
        Lsn(std::env::var(CHILD_SINCE_ENV)
            .expect("child drop frontiers environment is present")
            .split(',')
            .nth(at)
            .expect("two drop frontiers")
            .parse()
            .expect("a drop frontier is a number"))
    };
    let frontiers = DropFrontiers {
        before_drop: frontier(0),
        between_drops: frontier(1),
    };
    let (author_path, receiver_path) = paths_in(Path::new(&root));
    let [(changes, provenance), _] = drop_units(&author_path, frontiers);
    let receiver = Arc::new(Database::open(&receiver_path).expect("open the child receiver"));
    let pause = match boundary {
        CrashBoundary::BeforeDurableCommit => {
            receiver.pause_before_received_schema_durable_commit_for_test()
        }
        CrashBoundary::AfterDurableBeforePublication => {
            receiver.pause_before_received_schema_publish_for_test()
        }
    };
    let applying = receiver.clone();
    std::thread::spawn(move || {
        let _ = apply_received(&applying, changes, provenance);
    });
    assert!(
        pause.wait_until_reached(CHILD_TIMEOUT),
        "the received drop never reached {}",
        boundary.name()
    );
    println!("{BOUNDARY_MARKER} {}", boundary.name());
    std::io::stdout()
        .flush()
        .expect("flush the boundary marker");
    loop {
        std::thread::park();
    }
}

struct CrashChild(Child);

impl Drop for CrashChild {
    fn drop(&mut self) {
        unsafe {
            libc::kill(-(self.0.id() as i32), libc::SIGKILL);
        }
        let _ = self.0.wait();
    }
}

fn crash_at(boundary: CrashBoundary) {
    let (paths, frontiers) = prepared();
    let before = {
        let declared_read_memory =
            contextdb_engine::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                contextdb_engine::OwnerReadConfig::default()
                    .limits
                    .limits
                    .memory,
            );
        let operation_read_memory = declared_read_memory
            .child()
            .expect("admit the committed-content observation owner");
        committed_content_for_test(&paths.receiver, operation_read_memory, declared_read_memory)
    }
    .expect("inspect the receiver");
    let mut command = Command::new(std::env::current_exe().expect("locate this test binary"));
    command
        .arg("--exact")
        .arg(crate::child_test_filter(
            module_path!(),
            "received_replacement_crash_child",
        ))
        .arg("--nocapture")
        .env(CHILD_ROOT_ENV, paths.root())
        .env(CHILD_BOUNDARY_ENV, boundary.name())
        .env(
            CHILD_SINCE_ENV,
            format!("{},{}", frontiers.before_drop.0, frontiers.between_drops.0),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    command.process_group(0);
    let mut child = CrashChild(command.spawn().expect("spawn the crash child"));
    let stdout = child.0.stdout.take().expect("capture the child marker");
    let (sender, marker) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Some(at) = line.find(BOUNDARY_MARKER) {
                let _ = sender.send(line[at..].to_string());
                return;
            }
        }
    });
    assert_eq!(
        marker
            .recv_timeout(CHILD_TIMEOUT)
            .expect("the child reaches its boundary"),
        format!("{BOUNDARY_MARKER} {}", boundary.name())
    );
    drop(child);

    restart(&paths.receiver);
    let recovered = assert_identity_describes_the_store(
        &paths.receiver,
        &format!("after a crash {}", boundary.name()),
    );
    match boundary {
        CrashBoundary::BeforeDurableCommit => {
            assert_eq!(
                recovered.maintained_digest, before.maintained_digest,
                "a crash before the durable commit leaves the old identity"
            );
            assert_eq!(recovered.relational_rows, before.relational_rows);
        }
        CrashBoundary::AfterDurableBeforePublication => {
            assert_widgets_gone(&recovered, false, "after a crash past the durable commit");
        }
    }
    restart(&paths.receiver);
    assert_eq!(
        {
            let declared_read_memory =
                contextdb_engine::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                    contextdb_engine::OwnerReadConfig::default()
                        .limits
                        .limits
                        .memory,
                );
            let operation_read_memory = declared_read_memory
                .child()
                .expect("admit the committed-content observation owner");
            committed_content_for_test(&paths.receiver, operation_read_memory, declared_read_memory)
        }
        .expect("inspect the reopened receiver")
        .maintained_digest,
        recovered.maintained_digest,
        "reopening after recovery changes nothing"
    );
}

#[test]
fn a_crash_before_the_received_drop_commits_leaves_the_old_identity_and_store() {
    // Writers this proof opens are counted process-wide by the direct
    // reader contract, which runs under this lock.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    crash_at(CrashBoundary::BeforeDurableCommit);
}

#[test]
fn a_crash_after_the_received_drop_commits_recovers_an_identity_matching_the_store() {
    // Writers this proof opens are counted process-wide by the direct
    // reader contract, which runs under this lock.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    crash_at(CrashBoundary::AfterDurableBeforePublication);
}
