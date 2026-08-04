use super::*;
use uuid::Uuid;

/// Replace-not-version: the row's identity is its natural key, while `id` is
/// ordinary content that a replacement is free to change. Declared two-way and
/// KEEP LATEST, so a later value legitimately supersedes an earlier one.
const REPLACE_DDL: &str = "CREATE TABLE decisions (\
     context_id UUID, decision_id UUID, id UUID, body TEXT, \
     PRIMARY KEY (context_id, decision_id)) \
     SYNC TWO WAY SYNC CONFLICT KEEP LATEST";

fn declare(db: &Database) {
    db.execute(REPLACE_DDL, &HashMap::new())
        .expect("declare decisions");
}

fn insert_decision(db: &Database, context: Uuid, decision: Uuid, row_id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO decisions (context_id, decision_id, id, body) \
         VALUES ($context_id, $decision_id, $id, $body)",
        &HashMap::from([
            ("context_id".to_string(), Value::Uuid(context)),
            ("decision_id".to_string(), Value::Uuid(decision)),
            ("id".to_string(), Value::Uuid(row_id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert decision");
}

fn delete_decision(db: &Database, context: Uuid, decision: Uuid) {
    db.execute(
        "DELETE FROM decisions WHERE context_id = $context_id AND decision_id = $decision_id",
        &HashMap::from([
            ("context_id".to_string(), Value::Uuid(context)),
            ("decision_id".to_string(), Value::Uuid(decision)),
        ]),
    )
    .expect("delete decision");
}

/// What the receiver holds for the key: its `id` and body, or None.
fn held(db: &Database, context: Uuid, decision: Uuid) -> Option<(Uuid, String)> {
    let result = db
        .execute(
            "SELECT id, body FROM decisions WHERE context_id = $context_id \
             AND decision_id = $decision_id",
            &HashMap::from([
                ("context_id".to_string(), Value::Uuid(context)),
                ("decision_id".to_string(), Value::Uuid(decision)),
            ]),
        )
        .expect("query decisions");
    let id_col = result
        .columns
        .iter()
        .position(|name| name == "id")
        .expect("id column");
    let body_col = result
        .columns
        .iter()
        .position(|name| name == "body")
        .expect("body column");
    result.rows.first().map(|row| {
        let id = match &row[id_col] {
            Value::Uuid(id) => *id,
            other => panic!("unexpected id: {other:?}"),
        };
        let body = match &row[body_col] {
            Value::Text(body) => body.clone(),
            other => panic!("unexpected body: {other:?}"),
        };
        (id, body)
    })
}

fn apply_to_receiver(receiver: &Database, changes: ChangeSet) {
    receiver
        .apply_synced_changes(
            changes,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
            &HashMap::new(),
            SyncAdoption::Continuing,
        )
        .expect("the receiver applies the replacement unit");
}

/// A replacement committed as ONE transaction — delete the old row, insert the
/// replacement under the SAME natural key with a new `id` — must reach the
/// receiver as the REPLACEMENT, not as a bare deletion. Applying only the
/// delete destroys the row on every machine that pulls it: the sender holds the
/// new value, the receiver holds nothing, and nothing reports an error.
#[test]
fn same_key_delete_and_reinsert_in_one_transaction_replaces_on_the_receiver() {
    let sender = Database::open_memory();
    let receiver = Database::open_memory();
    declare(&sender);
    declare(&receiver);

    let context = Uuid::new_v4();
    let decision = Uuid::new_v4();
    let original_id = Uuid::new_v4();
    insert_decision(&sender, context, decision, original_id, "original");
    apply_to_receiver(&receiver, sender.changes_since(Lsn(0)));
    assert_eq!(
        held(&receiver, context, decision).map(|(_, body)| body),
        Some("original".to_string()),
        "the receiver holds the original before the replacement"
    );

    // The replacement: one transaction, same key, new id.
    let before_replace = sender.current_lsn();
    let replacement_id = Uuid::new_v4();
    let tx = sender.begin().expect("begin replacement transaction");
    sender
        .execute_in_tx(
            tx,
            "DELETE FROM decisions WHERE context_id = $context_id AND decision_id = $decision_id",
            &HashMap::from([
                ("context_id".to_string(), Value::Uuid(context)),
                ("decision_id".to_string(), Value::Uuid(decision)),
            ]),
        )
        .expect("delete the superseded row");
    sender
        .execute_in_tx(
            tx,
            "INSERT INTO decisions (context_id, decision_id, id, body) \
             VALUES ($context_id, $decision_id, $id, $body)",
            &HashMap::from([
                ("context_id".to_string(), Value::Uuid(context)),
                ("decision_id".to_string(), Value::Uuid(decision)),
                ("id".to_string(), Value::Uuid(replacement_id)),
                ("body".to_string(), Value::Text("replacement".to_string())),
            ]),
        )
        .expect("insert the replacement row");
    sender.commit(tx).expect("commit the replacement");

    assert_eq!(
        held(&sender, context, decision),
        Some((replacement_id, "replacement".to_string())),
        "the sender holds the replacement it just committed"
    );

    apply_to_receiver(&receiver, sender.changes_since(before_replace));

    assert_eq!(
        held(&receiver, context, decision),
        Some((replacement_id, "replacement".to_string())),
        "a replacement committed in one transaction must arrive as the replacement; \
         applying only its delete silently destroys the row on every receiver"
    );
}

/// The single-variable control: the SAME two mutations split across two
/// separate units converge. Only the one-transaction shape loses the row, which
/// is what points at the same-key handling rather than at delete or insert
/// individually.
#[test]
fn the_same_replacement_split_across_two_units_converges() {
    let sender = Database::open_memory();
    let receiver = Database::open_memory();
    declare(&sender);
    declare(&receiver);

    let context = Uuid::new_v4();
    let decision = Uuid::new_v4();
    let original_id = Uuid::new_v4();
    insert_decision(&sender, context, decision, original_id, "original");
    apply_to_receiver(&receiver, sender.changes_since(Lsn(0)));

    let before_delete = sender.current_lsn();
    delete_decision(&sender, context, decision);
    apply_to_receiver(&receiver, sender.changes_since(before_delete));
    assert_eq!(
        held(&receiver, context, decision),
        None,
        "the standalone delete removes the row"
    );

    let before_insert = sender.current_lsn();
    let replacement_id = Uuid::new_v4();
    insert_decision(&sender, context, decision, replacement_id, "replacement");
    apply_to_receiver(&receiver, sender.changes_since(before_insert));

    assert_eq!(
        held(&receiver, context, decision),
        Some((replacement_id, "replacement".to_string())),
        "split across two units the replacement converges"
    );
}
