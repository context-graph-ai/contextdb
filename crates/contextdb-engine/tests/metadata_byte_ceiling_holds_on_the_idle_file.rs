//! A byte ceiling means the same thing whoever answers.
//!
//! Reading a store as an idle file used to ignore the ceiling entirely: a
//! complete answer went out whatever its size, and an inventory item larger
//! than the whole ceiling was published as a one-item page. Over a channel the
//! same questions were refused with the setting that would carry them. A
//! ceiling that binds on one route and not the other is not a ceiling, so both
//! are pinned here against the same store.

use contextdb_core::Error;
use contextdb_core::read_contract::{
    ReadFailureDetail, ReadFailureKind, ReadFailureLimit, ReadLimits,
};
use contextdb_engine::{
    Database, MetadataRequest, ReadSession, ReadSessionOptions,
    direct_file_reader::DirectMetadataBody,
};
use std::collections::HashMap;

fn store_with_one_long_table(directory: &std::path::Path) -> std::path::PathBuf {
    let path = directory.join("ceiling.db");
    let database = Database::open(&path).expect("open the store for writing");
    database
        .execute(
            "CREATE TABLE a_table_whose_name_alone_is_longer_than_a_very_small_byte_ceiling \
             (id INTEGER PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create the fixture table");
    database.close().expect("close the store");
    path
}

fn options_with_result_bytes(result_bytes: u64) -> ReadSessionOptions {
    ReadSessionOptions {
        limits: ReadLimits {
            result_bytes,
            // A page can never be larger than the whole answer.
            cursor_page_bytes: result_bytes,
            ..ReadLimits::default()
        },
        ..ReadSessionOptions::default()
    }
}

fn required_bytes_of(error: &Error, ceiling: u64) -> u64 {
    let Error::ReadFailure(failure) = error else {
        panic!("expected a typed read refusal, got {error:?}");
    };
    assert_eq!(
        failure.kind(),
        ReadFailureKind::OwnerLimitExceeded,
        "a crossed byte ceiling is reported as OwnerLimitExceeded: {failure:?}"
    );
    let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail() else {
        panic!("the refusal carries no ceiling: {failure:?}");
    };
    assert_eq!(detail.limit, ReadFailureLimit::ResultBytes);
    assert_eq!(
        detail.value, ceiling,
        "the refusal names the ceiling that stopped it, not the size that crossed it"
    );
    let required = detail
        .required
        .as_ref()
        .expect("a caller is told the number that would carry its answer");
    assert!(
        required.required_bytes > ceiling,
        "the number offered is larger than the ceiling that refused: {required:?}"
    );
    assert_eq!(
        required.required_setting,
        format!("effective result_bytes >= {}", required.required_bytes),
        "the refusal renders the setting that would let it through"
    );
    required.required_bytes
}

#[test]
fn a_complete_answer_the_idle_file_cannot_fit_is_refused_with_the_setting_that_would_carry_it() {
    let directory = tempfile::tempdir().expect("task-scoped store directory");
    let path = store_with_one_long_table(directory.path());

    // Read whole first, so the size that has to be beaten is measured rather
    // than guessed, and so a refusal below cannot be the ceiling simply being
    // too small for every read.
    let whole = ReadSession::open(&path).expect("open the idle store");
    let schema = whole
        .metadata(
            MetadataRequest::Schema {
                table: "a_table_whose_name_alone_is_longer_than_a_very_small_byte_ceiling"
                    .to_owned(),
            },
            None,
        )
        .expect("with room for it, the idle file answers");
    assert!(matches!(schema.body, DirectMetadataBody::Schema { .. }));

    let ceiling = 64;
    let refused = ReadSession::open_with_options(&path, options_with_result_bytes(ceiling))
        .expect("open the idle store under a small byte ceiling")
        .metadata(
            MetadataRequest::Schema {
                table: "a_table_whose_name_alone_is_longer_than_a_very_small_byte_ceiling"
                    .to_owned(),
            },
            None,
        )
        .expect_err("a complete answer past the ceiling is refused, not published");
    let _required = required_bytes_of(&refused, ceiling);
}

#[test]
fn an_inventory_item_that_cannot_fit_even_alone_is_refused_rather_than_published() {
    let directory = tempfile::tempdir().expect("task-scoped store directory");
    let path = store_with_one_long_table(directory.path());

    let ceiling = 8;
    let refused = ReadSession::open_with_options(&path, options_with_result_bytes(ceiling))
        .expect("open the idle store under a byte ceiling smaller than one item")
        .metadata(MetadataRequest::Tables, None)
        .expect_err("one item that does not fit is refused; there is no smaller page");
    let _required = required_bytes_of(&refused, ceiling);
}

#[test]
fn the_same_store_answers_the_same_way_when_it_has_room() {
    let directory = tempfile::tempdir().expect("task-scoped store directory");
    let path = store_with_one_long_table(directory.path());

    let session = ReadSession::open(&path).expect("open the idle store");
    let answered = session
        .metadata(MetadataRequest::Tables, None)
        .expect("with the shipped ceiling the inventory answers");
    let DirectMetadataBody::Tables { items, has_more } = answered.body else {
        panic!("the table inventory answers with tables");
    };
    assert!(
        items
            .iter()
            .any(|table| table.starts_with("a_table_whose_name_alone_is_longer")),
        "the fixture table is in the inventory: {items:?}"
    );
    assert!(!has_more, "one page holds this inventory");
    assert!(
        answered.continuation.is_none(),
        "an inventory that ended issues no continuation"
    );
}

/// The encoded size of the page a one-table inventory really publishes: one
/// item, no `has_more`, no continuation. Measured rather than guessed, so the
/// ceilings below are stated against the shape the reader would actually be
/// handed.
fn terminal_one_item_page_bytes() -> u64 {
    use contextdb_core::read_contract::{MetadataItem, MetadataPage, MetadataPageVocabulary};
    let page = MetadataPage {
        vocabulary: MetadataPageVocabulary::Tables,
        items: vec![MetadataItem::Table(
            "a_table_whose_name_alone_is_longer_than_a_very_small_byte_ceiling".to_owned(),
        )],
        has_more: false,
        continuation: None,
    };
    u64::try_from(
        contextdb_engine::read_contract::metadata_page_encoded_size(&page)
            .expect("measure the terminal one-item page"),
    )
    .expect("a page size fits a u64")
}

#[test]
fn the_last_item_is_measured_in_the_shape_it_is_published_in_not_in_a_resumable_one() {
    let directory = tempfile::tempdir().expect("task-scoped store directory");
    let path = store_with_one_long_table(directory.path());
    let terminal = terminal_one_item_page_bytes();

    // Exactly enough for the page that ends the inventory. Charging this item
    // for a `has_more` and a continuation it will never carry would refuse an
    // answer that fits.
    let answered = ReadSession::open_with_options(&path, options_with_result_bytes(terminal))
        .expect("open the idle store at exactly the terminal page size")
        .metadata(MetadataRequest::Tables, None)
        .expect("a ceiling equal to the page that would be published carries it");
    let DirectMetadataBody::Tables { items, has_more } = answered.body else {
        panic!("the table inventory answers with tables");
    };
    assert!(!has_more, "one page holds this inventory");
    assert!(
        items
            .iter()
            .any(|table| table.starts_with("a_table_whose_name_alone_is_longer")),
        "the item that exactly fits is published: {items:?}"
    );
    assert!(answered.continuation.is_none());

    // One byte less, and the refusal names the size of that same terminal
    // shape -- not the larger size of a resumable page nobody would have been
    // sent.
    let ceiling = terminal - 1;
    let refused = ReadSession::open_with_options(&path, options_with_result_bytes(ceiling))
        .expect("open the idle store one byte below the terminal page size")
        .metadata(MetadataRequest::Tables, None)
        .expect_err("one byte short of the page that would be published refuses");
    assert_eq!(
        required_bytes_of(&refused, ceiling),
        terminal,
        "the refusal asks for exactly the size of the page it would have published"
    );
}
