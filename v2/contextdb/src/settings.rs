//! Declared settings: one slot, reader and check for every policy.
use crate::store::Read;
use crate::{Error, Result};

pub(crate) type Values = [Option<u64>; 8];
#[derive(Clone, Copy)]
pub(crate) enum Kind {
    Bytes,
    Ratio(usize),
    Seconds,
    Count,
    /// A count that may be 0.
    Many,
}
use Kind::*;
const START: u64 = 0.05f64.to_bits();
const STOP: u64 = 0.02f64.to_bits();
pub(crate) const SETTINGS: [(&str, Kind, u64); 8] = [
    ("SET MEMORY_LIMIT", Bytes, u64::MAX),
    ("SET DISK_LIMIT", Bytes, u64::MAX),
    ("SET STORAGE_COMPACTION START_RATIO", Ratio(3), START),
    ("SET STORAGE_COMPACTION STOP_RATIO", Ratio(2), STOP),
    ("SET STORAGE_COMPACTION MIN_INTERVAL", Seconds, 60_000),
    ("SET UNINDEXED_VECTOR_LIMIT", Count, 0),
    ("SET STATEMENT_STATISTICS_LIMIT", Many, 1024),
    ("SET MAINTENANCE_POLL_INTERVAL", Seconds, 60_000),
];
pub(crate) const RESET: u64 = u64::MAX - 1;
pub(crate) fn applied(at: usize, n: u64) -> Option<u64> {
    let default = (at == 0).then(crate::system::default_memory_limit);
    let default = default.unwrap_or(SETTINGS[at].2);
    limit(if n == RESET { default } else { n })
}
pub(crate) fn limit(value: u64) -> Option<u64> {
    (value != u64::MAX).then_some(value)
}
pub(crate) fn named(verb: &str, name: &str) -> Result<usize> {
    SETTINGS
        .iter()
        .position(|row| row.0[4..].eq_ignore_ascii_case(name))
        .ok_or_else(|| match name.to_ascii_uppercase().as_str() {
            "READ_CACHE_SHARE" | "VECTOR_SERVING_CACHE" => crate::sql::not_carried(
                name,
                "the page cache share is set when a reader opens: ReadLimits::cache_share",
            ),
            "STORAGE_COMPACTION MAX_SWEEPS" => crate::sql::not_carried(
                name,
                "a page-bounded step runs once per cycle; the shell's maintenance compact [steps] runs more",
            ),
            _ => crate::sql::not_supported(&format!("{verb} {name}")),
        })
}
/// Reads bytes for command-line options using the same byte reader as SQL settings.
pub fn parse_size(text: &str) -> Result<usize> {
    let end = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let factor = match text[end..].to_ascii_uppercase().as_str() {
        "" => 1,
        "K" => 1 << 10,
        "M" => 1 << 20,
        "G" => 1 << 30,
        _ => 0,
    };
    text[..end]
        .parse::<usize>()
        .ok()
        .and_then(|n| n.checked_mul(factor))
        .filter(|_| factor != 0)
        .ok_or_else(|| Error::ParseError(format!("'{text}' is not a size in bytes, K, M or G")))
}
pub(crate) fn read(at: usize, text: &str) -> Result<u64> {
    let row = &SETTINGS[at];
    let text = text.trim().trim_matches('\'');
    if text.eq_ignore_ascii_case("DEFAULT") {
        return Ok(RESET);
    }
    if matches!(row.1, Bytes) && text.eq_ignore_ascii_case("NONE") {
        return Ok(u64::MAX);
    }
    let bad = || Error::PlanError(format!("{} has an invalid value", row.0));
    match row.1 {
        Bytes => match text
            .find(|c: char| !c.is_ascii_digit())
            .map(|end| &text[end..])
        {
            Some("K" | "M" | "G") => parse_size(text).map(|n| n as u64).map_err(|_| bad()),
            _ => Err(bad()),
        },
        Ratio(_) => text.parse::<f64>().map(f64::to_bits).map_err(|_| bad()),
        Seconds => {
            let (n, unit) = text.split_once(char::is_whitespace).ok_or_else(bad)?;
            let factor = match unit.trim().to_ascii_lowercase().as_str() {
                "hours" => 3_600_000,
                "minutes" => 60_000,
                "seconds" => 1000,
                "milliseconds" => 1,
                _ => return Err(bad()),
            };
            n.parse::<u64>()
                .ok()
                .and_then(|n| n.checked_mul(factor))
                .ok_or_else(bad)
        }
        Count | Many => text.parse().map_err(|_| bad()),
    }
}
pub(crate) fn check(at: usize, next: u64, values: &Values) -> Result<()> {
    let valid = match SETTINGS[at].1 {
        Bytes => true,
        Ratio(other) => {
            let n = f64::from_bits(next);
            let peer = f64::from_bits(values[other].unwrap_or(SETTINGS[other].2));
            (0.0..=1.0).contains(&n) && if at < other { n > peer } else { n < peer }
        }
        // The compaction interval may be set to zero. A poll interval of zero is refused.
        Seconds => next > 0 || at == 4,
        Count => next > 0 && next <= u32::MAX as u64,
        Many => next <= u32::MAX as u64,
    };
    if valid || next == RESET {
        Ok(())
    } else {
        Err(Error::PlanError(format!(
            "{} is out of range or crosses the other ratio",
            SETTINGS[at].0
        )))
    }
}
pub(crate) fn load(read: &dyn Read) -> Result<Values> {
    let mut values = [None; 8];
    for (at, value) in values.iter_mut().enumerate() {
        *value = read.setting(at)?;
    }
    Ok(values)
}
