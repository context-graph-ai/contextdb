use contextdb_parser::{Statement, StatementEffect, parse, statement_effect};

#[test]
fn session_cache_settings_do_not_require_a_writable_store() {
    for sql in [
        "SET READ_CACHE_SHARE = 40",
        "/* session */ SET read_cache_share = DEFAULT;",
        "SHOW READ_CACHE_SHARE",
    ] {
        let statement = parse(sql).unwrap();
        assert_eq!(statement_effect(&statement), StatementEffect::Read);
        assert!(matches!(
            statement,
            Statement::SetReadCacheShare(_) | Statement::ShowReadCacheShare
        ));
    }
    for value in ["-1", "101", "999999999999999999999999999999999999"] {
        assert!(
            matches!(parse(&format!("SET READ_CACHE_SHARE = {value}")).unwrap(), Statement::SetReadCacheShare(Some(parsed)) if parsed == value)
        );
    }
}
