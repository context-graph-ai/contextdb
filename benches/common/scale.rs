use contextdb_core::Value;
use std::collections::HashMap;

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance_three_paradigm_query, composite_index_routing, indexed_scan_filter_en"
)]
pub fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance_three_paradigm_query, composite_index_routing, indexed_scan_filter_en"
)]
pub fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}
