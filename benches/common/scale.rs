use contextdb_core::Value;
use std::collections::HashMap;

pub fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

pub fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}
