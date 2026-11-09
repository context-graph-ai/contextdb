//! A malformed custody record whose enum-variant name is megabytes long, in
//! multibyte text, refuses with the established codec diagnostic and a detail
//! of at most 16 KiB. The decoder never builds the unbounded text: what the
//! decode allocates at its peak is bounded independently of the name's
//! length, every reader limit answers either a typed memory refusal or that
//! bounded diagnostic, and nothing stays admitted afterwards. A short unknown
//! name keeps the exact diagnostic the resident path gives.
//!
//! Runs in a fresh child process, where the counted allocator sees only this
//! test's work.
use crate::writer_reopen_and_doubling_keep_a_fixed_resident_envelope::{
    allocation_peak_for_test, live_allocations_for_test, reset_allocation_peak_for_test,
};
use contextdb_core::Error;
use contextdb_engine::Database;

const MODE: &str = "CONTEXTDB_LONG_DECODER_ERROR";
const KEY: &str = "delivery_manifest.v1.00000000000000000000000000000000";
const PREFIX: &str = "delivery metadata decode: ";
const DETAIL_MAX: usize = 16 * 1024;
/// What one decode may hold at its peak, whatever the name's length: the
/// bounded detail and its copies, the error reserve and decoder state.
const PEAK_BOUND: usize = 192 * 1024;

/// A one-entry MessagePack map `{name: nil}`: an externally tagged enum
/// value naming the variant `name`.
fn record_named(name: &str) -> Vec<u8> {
    let mut bytes = vec![0x81, 0xdb];
    bytes.extend_from_slice(&u32::try_from(name.len()).unwrap().to_be_bytes());
    bytes.extend_from_slice(name.as_bytes());
    bytes.push(0xc0);
    bytes
}

/// Multibyte text of `bytes` bytes or slightly more, mixing 1-, 2- and
/// 4-byte characters so a cut at 16 KiB can land inside a character.
fn multibyte(bytes: usize) -> String {
    let mut name = String::with_capacity(bytes + 8);
    while name.len() < bytes {
        name.push_str("é😀a");
    }
    name
}

fn budget(error: &Error) -> bool {
    matches!(error, Error::MemoryBudgetExceeded { .. })
}

fn bounded_diagnostic(error: &Error) -> bool {
    match error {
        Error::SyncError(text) => {
            text.starts_with(PREFIX)
                && text.contains("unknown variant")
                && text.len() <= PREFIX.len() + DETAIL_MAX
        }
        _ => false,
    }
}

fn long_names() {
    for length in [64 * 1024, 4 * 1024 * 1024] {
        let bytes = record_named(&multibyte(length));
        let before = live_allocations_for_test();
        reset_allocation_peak_for_test();
        // The durable decode alone: the resident loader is not this path.
        let (durable, held) = Database::__custody_probe_for_test(KEY, &bytes, 1 << 30);
        let peak = allocation_peak_for_test().saturating_sub(before);
        assert_eq!(held, 0);
        let error = durable.expect_err("an unknown variant refuses");
        assert!(
            bounded_diagnostic(&error),
            "{length}-byte name: the established bounded diagnostic: {}",
            error.to_string().get(..200).unwrap_or_default()
        );
        assert!(
            peak <= PEAK_BOUND,
            "{length}-byte name: the decode allocated {peak} bytes at its peak"
        );
        // Every limit answers a typed refusal or the bounded diagnostic, and
        // holds nothing afterwards.
        let mut refused = 0;
        for step in 0..48usize {
            let limit = 1024 + step * 4096;
            let (answer, held) = Database::__custody_probe_for_test(KEY, &bytes, limit);
            assert_eq!(held, 0, "limit {limit}: nothing stays admitted");
            match answer {
                Err(error) if budget(&error) => refused += 1,
                Err(error) => assert!(
                    bounded_diagnostic(&error),
                    "limit {limit}: {}",
                    error.to_string().get(..200).unwrap_or_default()
                ),
                Ok(()) => panic!("limit {limit}: an unknown variant decoded"),
            }
        }
        assert!(refused > 0, "{length}-byte name: some limit refuses");
        println!("S3_DECODE_ERROR length={length} peak={peak} refused={refused}");
    }
    // A short unknown name keeps the resident path's exact diagnostic.
    let bytes = record_named("NotARecordKind");
    let (durable, resident) = Database::__custody_decode_refusals_for_test(KEY, &bytes, None);
    let durable = durable.expect("durable refuses").to_string();
    let resident = resident.expect("resident refuses").to_string();
    assert!(
        durable.contains("unknown variant `NotARecordKind`"),
        "{durable}"
    );
    assert_eq!(durable, resident, "short diagnostics keep parity");
}

#[test]
fn a_long_decoder_error_is_bounded_before_it_is_built() {
    if std::env::var(MODE).is_ok() {
        long_names();
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &crate::child_test_filter(
                module_path!(),
                "a_long_decoder_error_is_bounded_before_it_is_built",
            ),
            "--nocapture",
            "--test-threads=1",
        ])
        .env(MODE, "1")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout
        .lines()
        .filter(|line| line.contains("S3_DECODE_ERROR"))
    {
        eprintln!("{line}");
    }
    assert!(
        output.status.success(),
        "{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
