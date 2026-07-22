//! INT-10: `TenantId` is a real newtype (like `ContextId`) so a tenant id is
//! unambiguous at the call site, and the persisted watermark config-key format
//! is authored in exactly one place (`TenantId::config_key`).
//!
//! On current source this fails to COMPILE (no `TenantId`, no `config_key`) —
//! that compile failure is the RED.

use contextdb_core::TenantId;

// ======== config-key centralization + byte-identity ========

#[test]
fn config_key_is_byte_identical_to_the_legacy_interpolation() {
    let tenant = TenantId::from("acme");

    // The persisted key strings that existed before this newtype were built by
    // `format!("<prefix>:{tenant_id}")`. The centralized helper must reproduce
    // them byte-for-byte, or existing on-disk watermarks would be orphaned.
    for prefix in [
        "sync_push_watermark",
        "sync_pull_watermark",
        "sync_applied_push_watermark",
    ] {
        let legacy = format!("{prefix}:{}", "acme");
        assert_eq!(
            tenant.config_key(prefix),
            legacy,
            "config_key must match the legacy format!(\"{prefix}:{{tenant_id}}\") exactly"
        );
    }

    assert_eq!(
        tenant.config_key("sync_push_watermark"),
        "sync_push_watermark:acme"
    );
}

// ======== ContextId-style ergonomics ========

#[test]
fn tenant_id_string_ergonomics_match_the_newtype_precedent() {
    let from_owned = TenantId::from(String::from("acme"));
    let from_borrowed = TenantId::from("acme");
    assert_eq!(from_owned, from_borrowed);

    // as_str / AsRef expose the inner id without cloning.
    assert_eq!(from_owned.as_str(), "acme");
    let as_ref: &str = from_owned.as_ref();
    assert_eq!(as_ref, "acme");

    // Display renders the bare id (used in tracing + subjects).
    assert_eq!(from_owned.to_string(), "acme");

    // Hash + Eq so it can key a map, exactly like ContextId.
    use std::collections::HashSet;
    let mut set = HashSet::new();
    set.insert(TenantId::from("acme"));
    assert!(set.contains(&TenantId::from("acme")));
}
