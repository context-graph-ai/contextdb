//! Dedicated-process bounded-memory guard for the media-transfer resolver.
#![cfg(target_os = "linux")]

use contextdb_engine::Database;
use contextdb_engine::work_ledger::{BlobHash, MovementPolicy, install_work_ledger_schema};
use contextdb_server::blob_resolver::BlobService;
use contextdb_server::transport::iroh::IrohServer;
use std::path::Path;
use std::sync::Arc;

#[path = "media_support/mod.rs"]
mod media_support;
use media_support::*;

const MB: usize = 1024 * 1024;

struct DiscardingHashSink {
    hasher: blake3::Hasher,
}

impl DiscardingHashSink {
    fn new() -> Self {
        Self {
            hasher: blake3::Hasher::new(),
        }
    }

    fn finalized_hex(&self) -> String {
        self.hasher.finalize().to_hex().to_string()
    }
}

impl std::io::Write for DiscardingHashSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn marked_to_file(marker: &str, size: usize, path: &Path) -> String {
    use std::io::Write as _;
    let mut hasher = blake3::Hasher::new();
    let mut writer = std::io::BufWriter::new(std::fs::File::create(path).expect("create fixture"));
    let head = marker.as_bytes();
    let head = &head[..head.len().min(size)];
    hasher.update(head);
    writer.write_all(head).expect("write marker head");
    let mut remaining = size - head.len();
    let mut buf = vec![0u8; MB];
    let mut n: u8 = 0;
    while remaining > 0 {
        let take = remaining.min(MB);
        for b in buf.iter_mut().take(take) {
            *b = n;
            n = n.wrapping_add(1);
        }
        hasher.update(&buf[..take]);
        writer.write_all(&buf[..take]).expect("write chunk");
        remaining -= take;
    }
    writer.flush().expect("flush fixture");
    hasher.finalize().to_hex().to_string()
}

fn peak_rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .map(|l| l.to_string())
        })
        .and_then(|l| l.split_whitespace().nth(1).and_then(|n| n.parse().ok()))
        .unwrap_or(0)
}

#[tokio::test]
async fn resolver_peak_memory_stays_bounded_for_a_large_blob() {
    let dir = tempfile::tempdir().expect("dir");
    let holder_dir = tempfile::tempdir().expect("holder dir");
    let consumer_dir = tempfile::tempdir().expect("consumer dir");
    let holder_key = identity_file(&holder_dir);
    let consumer_key = identity_file(&consumer_dir);
    let holder_node = node_id_of(&holder_key);
    let consumer_node = node_id_of(&consumer_key);

    let size = 128 * MB;
    let file = dir.path().join("big.bin");
    let expected_hex = marked_to_file("BOUNDED-2525", size, &file);
    let h = BlobHash::from_hex(&expected_hex).expect("valid hex");

    let holder_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&holder_db).expect("holder schema");
    seed_entitlement(&holder_db, "job-1", &holder_node, &consumer_node, &h);
    let holder = BlobService::new(
        holder_db,
        MovementPolicy {
            auto_propagate: true,
        },
        holder_key.clone(),
    );
    holder.set_test_clock(T0);

    let consumer_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&consumer_db).expect("consumer schema");
    seed_entitlement(&consumer_db, "job-1", &holder_node, &consumer_node, &h);
    let consumer = BlobService::new(
        consumer_db,
        MovementPolicy {
            auto_propagate: true,
        },
        consumer_key,
    );
    consumer.set_test_clock(T0);

    let baseline = peak_rss_kb();
    assert_eq!(
        holder.ingest_file(&file).expect("ingest file"),
        h,
        "ingest_file must content-address the whole file"
    );
    let endpoint = within(IrohServer::bind(&bind_spec(&holder_key)))
        .await
        .expect("bind holder");
    holder.serve_on(&endpoint);
    let ticket = endpoint.ticket();

    let mut sink = DiscardingHashSink::new();
    let written = within(consumer.resolve_blob_ref(&h, &ticket, &mut sink))
        .await
        .expect("large resolve must succeed");
    let peak = peak_rss_kb();

    assert_eq!(written as usize, size);
    assert_eq!(sink.finalized_hex(), expected_hex);
    assert_eq!(holder.fetch_requests_received_for_test(), 1);
    assert_eq!(holder.payload_bytes_emitted_for_test(), size as u64);
    let delta_kb = peak.saturating_sub(baseline);
    assert!(
        delta_kb < 64 * 1024,
        "peak RSS delta must stay below 64 MB, observed {delta_kb} KB"
    );
}
