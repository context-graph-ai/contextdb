use contextdb::Value;

use crate::input::{Scan, scan};
use crate::output::table;

#[test]
fn a_statement_ends_at_the_first_semicolon_outside_quotes_and_comments() {
    assert_eq!(scan("SELECT 1; SELECT 2;"), Scan::Ends(9));
    assert_eq!(scan("SELECT ';' -- ;\n, \"a;b\" /* ; */ ;"), Scan::Ends(33));
    assert_eq!(scan("SELECT 'it''s; here'"), Scan::Open);
    assert_eq!(scan("SELECT 1\n"), Scan::Open);
    assert_eq!(scan("/* not closed ;"), Scan::Open);
    assert_eq!(scan("  -- done\n/* and */\n"), Scan::Blank);
    assert_eq!(scan(""), Scan::Blank);
}

#[test]
fn a_table_is_as_wide_as_its_widest_cell() {
    let columns = ["id".to_owned(), "body".to_owned()];
    let rows = [
        vec![Value::Int64(1), Value::Text("première".to_owned())],
        vec![Value::Int64(20), Value::Null],
    ];
    assert_eq!(
        table(&columns, &rows),
        "\
+----+----------+
| id | body     |
+----+----------+
| 1  | première |
| 20 | NULL     |
+----+----------+
(2 rows)
"
    );
    assert_eq!(table(&columns, &[]), "(0 rows)\n");
}
