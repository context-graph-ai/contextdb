use contextdb_parser::{Statement, StatementEffect, parse, statement_effect};

#[test]
fn session_cache_settings_do_not_require_a_writable_store() {
    for sql in [
        "SET READ_CACHE_SHARE = 40",
        "SET READ_CACHE_SHARE 40",
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

#[test]
fn memory_and_disk_settings_share_optional_assignment_syntax() {
    for sql in [
        "SET MEMORY_LIMIT 64M",
        "SET MEMORY_LIMIT = 64M",
        "SET MEMORY_LIMIT = '64M'",
    ] {
        assert!(matches!(
            parse(sql).unwrap(),
            Statement::SetMemoryLimit(contextdb_parser::SetMemoryLimitValue::Bytes(67_108_864))
        ));
    }
    for sql in [
        "SET DISK_LIMIT 64M",
        "SET DISK_LIMIT = 64M",
        "SET DISK_LIMIT = '64M'",
    ] {
        assert!(matches!(
            parse(sql).unwrap(),
            Statement::SetDiskLimit(contextdb_parser::SetDiskLimitValue::Bytes(67_108_864))
        ));
    }
    for token in ["40%", "4.5", "NONE", "'not an integer'"] {
        assert!(
            matches!(parse(&format!("SET READ_CACHE_SHARE {token}")).unwrap(), Statement::SetReadCacheShare(Some(value)) if value == token)
        );
    }
}
