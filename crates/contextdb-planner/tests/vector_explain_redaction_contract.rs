//! The static planner explain is a disclosure boundary too: it must preserve
//! source structure without serializing the row key that selects a vector.

use contextdb_parser::parse;
use contextdb_planner::plan;

#[test]
fn static_vector_explain_redacts_every_supported_row_vector_source_key_shape() {
    for (literal, rendered_secret) in [
        (
            "'literal-row-vector-key-that-must-not-leak'",
            "literal-row-vector-key-that-must-not-leak",
        ),
        ("TRUE", "true"),
        ("424242", "424242"),
        ("12345.6789", "12345.6789"),
        ("$secret_row_vector_key", "secret_row_vector_key"),
    ] {
        let statement = parse(&format!(
            "SELECT id FROM evidence \
             ORDER BY embedding <=> ROW_VECTOR('evidence','embedding',{literal}) \
             USE VECTOR EXACT LIMIT 1"
        ))
        .unwrap_or_else(|error| panic!("the row-source literal {literal} parses: {error}"));
        let explain = plan(&statement)
            .unwrap_or_else(|error| panic!("the row-source literal {literal} plans: {error}"))
            .explain();

        assert!(
            explain.contains("VectorSearch(table=evidence, column=embedding"),
            "static explain retains the structural vector source: {explain}"
        );
        assert!(
            explain.contains("RowVectorSource(table=evidence, column=embedding, key=<redacted>)"),
            "static explain uses one structural row-key placeholder for {literal}: {explain}"
        );
        assert!(
            !explain.contains(rendered_secret),
            "static explain must not disclose the {literal} row-vector source key: {explain}"
        );
    }
}
