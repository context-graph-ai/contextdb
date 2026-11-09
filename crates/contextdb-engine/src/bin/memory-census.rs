//! Read existing live-writer checkpoint receipts or inspect a reopened copy.
use contextdb_engine::Database;
use serde::Serialize;
use serde_json::{Value, json};
use std::io::{Read, Seek, SeekFrom, Write};

const TAIL_BYTES: u64 = 1024 * 1024;

#[derive(Serialize)]
struct SnapshotCensus<T: Serialize> {
    observation_kind: &'static str,
    live_writer: bool,
    process_id: u32,
    charged_bytes: usize,
    components: T,
}

fn latest_writer_checkpoint(
    bytes: &[u8],
    pid: u32,
    database_id: u64,
) -> Result<Value, Box<dyn std::error::Error>> {
    // A concurrent append can leave the final record incomplete. Only complete
    // newline-terminated records qualify, and a different process never does.
    let end = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .ok_or("no complete receipt")?;
    for line in bytes[..end].rsplit(|byte| *byte == b'\n') {
        let Ok(mut receipt) = serde_json::from_slice::<Value>(line) else {
            continue;
        };
        if receipt["kind"] != "writer_memory_ownership"
            || receipt["pid"].as_u64() != Some(pid as u64)
            || receipt["database_id"].as_u64() != Some(database_id)
        {
            continue;
        }
        let charged = receipt["receipt"]["vector"]["accountant_total"]
            .as_u64()
            .ok_or("writer receipt has no charged byte total")?;
        receipt["observation_kind"] = json!("live_writer_checkpoint");
        receipt["live_writer"] = json!(true);
        receipt["charged_bytes"] = json!(charged);
        return Ok(receipt);
    }
    Err("no complete writer checkpoint for the requested process in the receipt tail".into())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let mode = args
        .next()
        .ok_or("expected --live-receipt or --reopened-snapshot")?;
    let path = args
        .next()
        .ok_or("a receipt path or disposable store copy is required")?;
    let mut stdout = std::io::stdout().lock();
    if mode == "--live-receipt" {
        if args.next().as_deref() != Some(std::ffi::OsStr::new("--pid")) {
            return Err("usage: memory-census --live-receipt JSONL --pid WRITER_PID --database-id DATABASE_ID [--bytes-only]".into());
        }
        let pid: u32 = args
            .next()
            .ok_or("writer PID is required")?
            .to_str()
            .ok_or("invalid PID")?
            .parse()?;
        if args.next().as_deref() != Some(std::ffi::OsStr::new("--database-id")) {
            return Err("the writer database identity is required".into());
        }
        let database_id: u64 = args
            .next()
            .ok_or("database identity is required")?
            .to_str()
            .ok_or("invalid database identity")?
            .parse()?;
        let bytes_only = match args.next() {
            None => false,
            Some(flag) if flag == "--bytes-only" => true,
            _ => return Err("unexpected argument".into()),
        };
        if args.next().is_some() {
            return Err("unexpected argument".into());
        }
        let mut file = std::fs::File::open(path)?;
        let size = file.metadata()?.len();
        file.seek(SeekFrom::Start(size.saturating_sub(TAIL_BYTES)))?;
        let mut bytes = Vec::new();
        file.take(TAIL_BYTES).read_to_end(&mut bytes)?;
        let receipt = latest_writer_checkpoint(&bytes, pid, database_id)?;
        if bytes_only {
            writeln!(stdout, "{}", receipt["charged_bytes"])?;
        } else {
            serde_json::to_writer(&mut stdout, &receipt)?;
            writeln!(stdout)?;
        }
    } else if mode == "--reopened-snapshot" {
        if args.next().is_some() || !std::path::Path::new(&path).is_file() {
            return Err(
                "usage: memory-census --reopened-snapshot EXISTING_DISPOSABLE_STORE_COPY".into(),
            );
        }
        let db = Database::open(path)?;
        let receipt = db.writer_memory_ownership_receipt_for_test();
        let census = SnapshotCensus {
            observation_kind: "reopened_snapshot",
            live_writer: false,
            process_id: std::process::id(),
            charged_bytes: receipt.vector.accountant_total,
            components: receipt,
        };
        serde_json::to_writer(&mut stdout, &census)?;
        writeln!(stdout)?;
    } else {
        return Err("expected --live-receipt or --reopened-snapshot".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Growing-store measurements must use the measured writer's actual charge,
    // even with interleaved search receipts and an incomplete concurrent append.
    // No existing test covers this command. Decisive mutants: accept another
    // PID, accept a non-writer record, or invent zero when a receipt is missing.
    #[test]
    fn census_selects_only_complete_checkpoints_from_the_requested_writer() {
        let bytes = concat!(
            "{\"kind\":\"writer_memory_ownership\",\"pid\":41,\"database_id\":1,\"lsn\":7,\"receipt\":{\"vector\":{\"accountant_total\":1234}}}\n",
            "{\"kind\":\"writer_memory_ownership\",\"pid\":42,\"database_id\":1,\"receipt\":{\"vector\":{\"accountant_total\":9999}}}\n",
            "{\"kind\":\"vector_search\",\"pid\":41}\n",
            "{\"kind\":\"writer_memory_ownership\",\"pid\":41,\"database_id\":1,\"receipt\":{\"vector\":{\"accountant_total\":5678}}}"
        ).as_bytes();
        let receipt = latest_writer_checkpoint(bytes, 41, 1).unwrap();
        assert_eq!(receipt["charged_bytes"], 1234);
        assert_eq!(receipt["lsn"], 7);
        assert_eq!(receipt["observation_kind"], "live_writer_checkpoint");
        assert_eq!(receipt["live_writer"], true);
        assert!(latest_writer_checkpoint(bytes, 43, 1).is_err());
        assert!(latest_writer_checkpoint(bytes, 41, 2).is_err());
        assert!(latest_writer_checkpoint(b"{}", 41, 1).is_err());
        assert!(
            latest_writer_checkpoint(
                b"{\"kind\":\"writer_memory_ownership\",\"pid\":41,\"database_id\":1}\n",
                41,
                1
            )
            .is_err()
        );
    }
}
