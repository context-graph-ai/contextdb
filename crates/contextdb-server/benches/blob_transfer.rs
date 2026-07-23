use contextdb_engine::Database;
use contextdb_engine::work_ledger::{
    BlobHash, ClaimInsert, InputRef, JobSpec, MovementPolicy, insert_claim,
    install_work_ledger_schema, submit_job,
};
use contextdb_server::FabricIdentity;
use contextdb_server::blob_resolver::BlobStore;
use contextdb_server::transport::iroh::IrohServer;
use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

const T0: i64 = 1_700_000_000_000;
const LEASE: i64 = 5 * 60_000;
const MB: usize = 1024 * 1024;
const RSS_CEILING_KB: u64 = 64 * 1024;

fn identity_file(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().join("fabric-identity.key")
}

fn bind_spec(identity: &Path) -> String {
    format!("iroh:?identity={}", identity.display())
}

fn node_id_of(key: &Path) -> String {
    FabricIdentity::load_or_generate(key)
        .expect("identity")
        .node_id()
}

fn blob_job(job_id: &str, submitter: &str, hash: &BlobHash) -> JobSpec {
    JobSpec::builder(job_id, "media.bench", "batch", submitter)
        .input_refs(vec![InputRef::blob_ref(hash.clone())])
        .submitted_at_ms(T0)
        .build()
}

fn seed_entitlement(db: &Database, job_id: &str, submitter: &str, claimant: &str, hash: &BlobHash) {
    submit_job(db, &blob_job(job_id, submitter, hash), &[] as &[&[u8]]).expect("submit blob job");
    match insert_claim(db, job_id, 1, claimant, T0 + LEASE, T0).expect("insert claim") {
        ClaimInsert::Inserted => {}
        other => panic!("claim seed must insert, got {other:?}"),
    }
}

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

struct BenchFixture {
    _file_dir: tempfile::TempDir,
    _holder_dir: tempfile::TempDir,
    _consumer_dir: tempfile::TempDir,
    _holder: BlobStore,
    consumer: BlobStore,
    endpoint: IrohServer,
    ticket: String,
    hash: BlobHash,
    expected_hex: String,
}

async fn build_fixture(size: usize, iteration: u64) -> BenchFixture {
    let file_dir = tempfile::tempdir().expect("file dir");
    let holder_dir = tempfile::tempdir().expect("holder dir");
    let consumer_dir = tempfile::tempdir().expect("consumer dir");
    let holder_key = identity_file(&holder_dir);
    let consumer_key = identity_file(&consumer_dir);
    let holder_node = node_id_of(&holder_key);
    let consumer_node = node_id_of(&consumer_key);

    let file = file_dir.path().join("bench-blob.bin");
    let expected_hex = marked_to_file(&format!("BENCH-{size}-{iteration}"), size, &file);
    let hash = BlobHash::from_hex(&expected_hex).expect("fixture hash");

    let holder_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&holder_db).expect("holder schema");
    seed_entitlement(&holder_db, "bench-job", &holder_node, &consumer_node, &hash);
    let holder = BlobStore::new(
        holder_db,
        MovementPolicy {
            auto_propagate: true,
        },
        holder_key.clone(),
    );
    holder.set_test_clock(T0);
    assert_eq!(
        holder.ingest_file(&file).expect("ingest file"),
        hash,
        "bench ingest_file must content-address the whole file"
    );

    let endpoint = IrohServer::bind(&bind_spec(&holder_key))
        .await
        .expect("bind holder");
    holder.serve_on(&endpoint);
    let ticket = endpoint.ticket();

    let consumer_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&consumer_db).expect("consumer schema");
    seed_entitlement(
        &consumer_db,
        "bench-job",
        &holder_node,
        &consumer_node,
        &hash,
    );
    let consumer = BlobStore::new(
        consumer_db,
        MovementPolicy {
            auto_propagate: true,
        },
        consumer_key,
    );
    consumer.set_test_clock(T0);

    BenchFixture {
        _file_dir: file_dir,
        _holder_dir: holder_dir,
        _consumer_dir: consumer_dir,
        _holder: holder,
        consumer,
        endpoint,
        ticket,
        hash,
        expected_hex,
    }
}

async fn run_one_transfer(size: usize, iteration: u64) -> Duration {
    let fixture = build_fixture(size, iteration).await;
    let baseline = peak_rss_kb();
    let start = Instant::now();
    let mut sink = DiscardingHashSink::new();
    let written = fixture
        .consumer
        .resolve_blob_ref(&fixture.hash, &fixture.ticket, &mut sink)
        .await
        .expect("bench resolve must succeed");
    let elapsed = start.elapsed();
    let peak = peak_rss_kb();

    assert_eq!(written as usize, size);
    assert_eq!(sink.finalized_hex(), fixture.expected_hex);
    let delta_kb = peak.saturating_sub(baseline);
    assert!(
        delta_kb < RSS_CEILING_KB,
        "consumer peak RSS delta must stay below {RSS_CEILING_KB} KB, observed {delta_kb} KB"
    );
    fixture.endpoint.close().await;

    elapsed
}

fn blob_transfer_bench(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let mut group = c.benchmark_group("blob_transfer");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);

    for size in [64 * 1024, 4 * MB, 64 * MB + 4096, 512 * MB, 1024 * MB] {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_custom(|iters| {
                runtime.block_on(async move {
                    let mut total = Duration::ZERO;
                    for iteration in 0..iters {
                        total += run_one_transfer(size, iteration).await;
                    }
                    total
                })
            });
        });
    }

    group.finish();
}

criterion_group!(benches, blob_transfer_bench);
criterion_main!(benches);
