// Included into `received_schema_stage_tests`: uses its signed journey peers.

/// Two statements an author wrote separately are two authored occurrences,
/// even when their text and the declaration they produce are identical. The
/// receiver keeps both, each under its own signed author block and its own
/// commit position; delivering the same occurrence again, before or after a
/// restart, adds neither an occurrence nor a commit position.
#[test]
fn identical_alters_authored_separately_stay_two_signed_occurrences_and_redelivery_adds_nothing() {
    let root = tempfile::TempDir::new().expect("scratch peer directory");
    let author = JourneyPeer::open(root.path(), "repeat-author");
    let mut hub = JourneyPeer::open(root.path(), "repeat-hub");
    author
        .db
        .set_maintenance_policy(MaintenancePolicy::CallerDriven);
    hub.db
        .set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .db
        .execute(
 "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT, embedding VECTOR(3) SEARCH_MODE INDEXED IVF (PROBES = 7)) SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
            &HashMap::new(),
        )
        .expect("author creates the table");
    pushed_to(&hub, &author, served_by(&author, Lsn(0)));

 let statement = "ALTER TABLE notes ALTER COLUMN embedding SET IVF (PROBES = 11)";
    let mut units = Vec::new();
    for _ in 0..2 {
        let since = author.db.current_lsn();
        author
            .db
            .execute(statement, &HashMap::new())
            .unwrap_or_else(|error| panic!("author runs `{statement}`: {error:?}"));
        let wire = served_by(&author, since);
        assert_eq!(
            wire.ddl.len(),
            1,
            "each ALTER is its own authored transaction"
        );
        units.push(wire);
    }
    let first = units[0].ddl_provenance[0]
        .author
        .clone()
        .expect("the author names itself on the first ALTER");
    let second = units[1].ddl_provenance[0]
        .author
        .clone()
        .expect("the author names itself on the second ALTER");
    assert_eq!(
        units[0].ddl, units[1].ddl,
        "the author wrote the same statement twice"
    );
    assert_ne!(
        first.source_ddl_lsn, second.source_ddl_lsn,
        "separately authored occurrences have distinct source positions"
    );
    assert_ne!(
        first, second,
        "each occurrence carries its own signed author block"
    );
    let (authored_ddl, authored_identity) = authored_migration(&served_by(&author, Lsn(0)));
    assert_eq!(
        authored_ddl.len(),
        3,
        "the author's migration is CREATE, ALTER, ALTER"
    );

    for (occurrence, wire) in units.iter().enumerate() {
        let held = hub.db.ddl_log_since(Lsn(0)).unwrap().len();
        let position = hub.db.current_lsn();
        pushed_to(&hub, &author, wire.clone());
        assert_eq!(
            hub.db.ddl_log_since(Lsn(0)).unwrap().len(),
            held + 1,
            "ALTER occurrence {occurrence} is retained even though its declaration equals the live one"
        );
        assert!(
            hub.db.current_lsn() > position,
            "ALTER occurrence {occurrence} is committed at its own receiver position"
        );
    }
    assert_holds_the_authors_migration(
        &hub,
        &author,
        &authored_ddl,
        &authored_identity,
        "after both deliveries",
    );
    let positions = hub.db.changes_since(Lsn(0)).unwrap().ddl_lsn;
    assert_eq!(positions.len(), 3);
    assert_ne!(
        positions[1], positions[2],
        "the two equal ALTERs hold distinct receiver commit positions"
    );

    let committed = hub.db.current_lsn();
    for wire in &units {
        pushed_to(&hub, &author, wire.clone());
    }
    assert_eq!(
        hub.db.current_lsn(),
        committed,
        "redelivering an occurrence the receiver holds commits nothing"
    );
    assert_holds_the_authors_migration(
        &hub,
        &author,
        &authored_ddl,
        &authored_identity,
        "after redelivery",
    );

    hub = hub.reopen();
    hub.db
        .set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(hub.db.current_lsn(), committed);
    assert_holds_the_authors_migration(
        &hub,
        &author,
        &authored_ddl,
        &authored_identity,
        "after reopen",
    );
    assert_eq!(hub.db.changes_since(Lsn(0)).unwrap().ddl_lsn, positions);
    for wire in &units {
        pushed_to(&hub, &author, wire.clone());
    }
    assert_eq!(
        hub.db.current_lsn(),
        committed,
        "redelivery after a restart adds no commit position"
    );
    assert_eq!(
        hub.db.ddl_log_since(Lsn(0)).unwrap().len(),
        3,
        "redelivery after a restart adds no occurrence"
    );
    assert_holds_the_authors_migration(
        &hub,
        &author,
        &authored_ddl,
        &authored_identity,
        "after redelivery following reopen",
    );
    assert_eq!(hub.db.changes_since(Lsn(0)).unwrap().ddl_lsn, positions);
    hub.db.close().unwrap();
    author.db.close().unwrap();
}

/// A selector row that cannot be decoded for want of an operation owner is not
/// a damaged store: the caller is told about the owner, and the same valid row
/// decodes once the owner is active.
#[test]
fn selector_decode_without_an_owner_reports_the_owner_not_corruption() {
    let root = tempfile::TempDir::new().expect("scratch peer directory");
    let peer = JourneyPeer::open(root.path(), "selector-owner");
    peer.db
        .execute(
            "CREATE SINK selector_owner_sink TYPE callback",
            &HashMap::new(),
        )
        .expect("declare the sink");
    let delta = peer.db.changes_since(Lsn(0)).unwrap();
    let position = delta
        .ddl
        .iter()
        .position(|change| matches!(change, DdlChange::CreateSink { .. }))
        .expect("the declaration is in the log");
    let change = &delta.ddl[position];
    let read_memory: contextdb_core::read_memory::ReadMemoryOwner =
        peer.db.declared_reader_memory().unwrap().into();
    let read_selector = || {
        RedbPersistence::first_eligible_ddl_occurrence(
            &peer.db.writer_read_root().unwrap().unwrap(),
            crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
            change,
            None,
        )
    };

    let unowned = read_selector().expect_err("no operation owner is active");
    assert!(
        crate::read_image_memory::is_missing_decode_owner(&unowned),
        "the missing owner must reach the caller as itself, not as corruption: {unowned:?}"
    );

    let decode = crate::read_image_memory::ImageMemory::operation_reader(
        crate::memory_accounting::AccountantHandle::clone(&peer.db.finalization.accountant),
        read_memory.clone(),
    )
    .unwrap();
    let _decode = decode.enter();
    let (lsn, ordinal) = read_selector()
        .expect("the same row decodes under its owner")
        .expect("the local declaration is an eligible occurrence");
    assert_eq!(lsn, delta.ddl_lsn[position]);
    assert_eq!(ordinal, 0);
}
