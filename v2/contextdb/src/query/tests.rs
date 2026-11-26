//! How two reals compare: the evaluator's pure function.

use std::{cmp::Ordering, collections::HashMap};

use super::{EvalContext, Kept, Scope, compare, eval};
use crate::memory::Budget;
use crate::schema::Catalog;
use crate::sql::ast::Statement;
use crate::store::Store;
use crate::{Database, DatabaseOpenOptions, Maintenance, types::Value};

#[test]
fn a_real_compares_as_a_key_holds_it() {
    let real = Value::Float64;
    assert_eq!(compare(&real(-0.0), &real(0.0)), Some(Ordering::Equal));
    assert_eq!(
        compare(&real(f64::NAN), &real(-f64::NAN)),
        Some(Ordering::Equal)
    );
    assert_eq!(
        compare(&real(-f64::NAN), &real(f64::INFINITY)),
        Some(Ordering::Greater)
    );
    assert_eq!(compare(&Value::Int64(2), &real(2.5)), Some(Ordering::Less));
}

#[test]
fn a_json_literal_is_read_once_for_one_statement() {
    let budget = Budget::new(None).expect("a statement budget");
    let folder = crate::vector::tests::fresh("json-cache");
    let options = DatabaseOpenOptions {
        maintenance: Maintenance::Caller,
        ..DatabaseOpenOptions::default()
    };
    let db = Database::open_with_options(&folder, options.clone()).unwrap();
    db.execute(
        "CREATE TABLE t(id INTEGER PRIMARY KEY,d JSON,e JSON)",
        &crate::params! {},
    )
    .unwrap();
    db.execute("CREATE INDEX j ON t((d -> 'k'))", &crate::params! {})
        .unwrap();
    db.execute("CREATE INDEX j2 ON t((e -> 'k'))", &crate::params! {})
        .unwrap();
    db.close().unwrap();
    drop(db);
    let store = Store::open_for_writing(&folder, &budget, &options).unwrap();
    let view = store.view().unwrap();
    let catalog = Catalog::load(&view, &budget).unwrap();
    let table = catalog.table("t").unwrap();
    let scope = Scope::of("t", &table.meta.columns);
    let kept = Kept::new(budget.passing("literal comparison").expect("a cover"));
    let params = HashMap::from([
        ("x".to_owned(), Value::Text("1".to_owned())),
        ("y".to_owned(), Value::Text("1".to_owned())),
    ]);
    let ctx = EvalContext::new(&params, 0, &kept);
    let (statement, _parsing, _) = crate::sql::parse(
        "SELECT id FROM t WHERE d -> 'k' = $x AND e -> 'k' = $x \
             AND d -> 'k' = '1' AND e -> 'k' = $y",
        &budget,
    )
    .unwrap();
    let Statement::Select(select) = statement else {
        panic!("a SELECT")
    };
    let expression = select.filter.as_ref().unwrap();
    let columns = crate::query::filter::Columns {
        scope: &scope,
        table: Some(table),
    };
    let reading = crate::query::filter::read(Some(expression), &columns, &ctx).unwrap();
    let why = Some("a JSON field's key is not the field");
    let held = reading.parts.iter().filter(|p| p.kept == why).count();
    let facts = (reading.cases.len(), reading.cases[0].len(), held);
    assert_eq!(
        (facts, kept.values.borrow().len()),
        ((1, 2, 4), 1),
        "planning uses cache"
    );
    let row = [
        Value::Int64(1),
        Value::Json(serde_json::json!({"k":1})),
        Value::Json(serde_json::json!({"k":1})),
    ];
    let bytes = kept.bytes.get();
    let evaluate = || eval(expression, &scope, &row, &ctx).expect("a JSON comparison");
    assert_eq!(evaluate().as_ref(), &Value::Bool(true));
    assert_eq!(kept.values.borrow().len(), 1);
    assert!(bytes > 0, "the parsed literal is charged");
    assert_eq!(evaluate().as_ref(), &Value::Bool(true));
    assert_eq!(
        kept.values.borrow().len(),
        1,
        "one parse across columns, literal and bound operands"
    );
    assert_eq!(kept.bytes.get(), bytes, "one parse charge");
    drop(view);
    drop(store);
    std::fs::remove_dir_all(&folder).unwrap();
}

#[test]
fn ties_let_go_when_a_walk_turns_direct_take_their_room_with_them() {
    use crate::query::search::{keep, let_go};
    let budget = Budget::new(None).expect("a statement budget");
    let mut held = budget.reserve(0, "query").expect("a reservation");
    let mut best = Vec::new();
    for n in 0..600_u32 {
        let key = crate::store::Key::from_bytes(&n.to_be_bytes());
        let pair = (Value::Float64(1.0), &key);
        keep(&mut best, (2, Some(1)), pair, &mut held).expect("room");
    }
    assert_eq!(best.len(), 600, "every pair tied at the cutoff is held");
    let_go(&mut best, 2, &mut held).expect("room");
    let slot = size_of::<(Value, crate::store::Key)>() as u64;
    assert_eq!((best.len(), best.capacity()), (2, 2));
    assert_eq!(
        held.bytes(),
        2 * (slot + 4),
        "two pairs and their room, nothing more"
    );
}
