use contextdb_engine::ReadSessionOptions;

#[test]
fn cache_inspection_keeps_the_declaration_and_the_admitted_session_budget() {
    let mut options = ReadSessionOptions {
        memory_limit: Some(64 * 1024 * 1024),
        ..Default::default()
    };
    let default = options.read_cache_share_status();
    assert_eq!(default.declared, None);
    assert_eq!(default.default_percent, 14);
    assert_eq!(default.declaration_source(), "default (14%)");
    assert_eq!(default.budget_bytes, 64 * 1024 * 1024);
    assert!(default.effective_bytes + default.fixed_reservation_bytes < default.budget_bytes);
    assert!(default.effective_bytes > 8 * 1024 * 1024);
    options.read_cache_share = Some(2);
    let declared = options.read_cache_share_status();
    assert_eq!(declared.declared, Some(2));
    assert_eq!(declared.budget_bytes, default.budget_bytes);
    assert_eq!(
        declared.fixed_reservation_bytes,
        default.fixed_reservation_bytes
    );
    assert!(declared.effective_bytes < default.effective_bytes);
    assert!(declared.effective_percent > 1.0 && declared.effective_percent <= 2.0);
    assert_eq!(options.clone().read_cache_share, Some(2));
}

#[test]
fn cache_inspection_accounts_for_fixed_reservations_at_both_percent_boundaries() {
    let mut options = ReadSessionOptions::default();
    options.read_cache_share = Some(0);
    let disabled = options.read_cache_share_status();
    assert_eq!(disabled.effective_bytes, 0);
    assert_eq!(disabled.effective_percent, 0.0);
    options.read_cache_share = Some(100);
    let full = options.read_cache_share_status();
    assert_eq!(
        full.effective_bytes + full.fixed_reservation_bytes,
        full.budget_bytes
    );
    assert_eq!(full.effective_percent, 100.0);
    options.memory_limit = Some(1);
    assert_eq!(options.read_cache_share_status().effective_bytes, 0);
}
