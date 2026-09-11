//! Measure actual file-backed, authenticated Iroh pushes.
//! One source transaction and ordinary push per unit; no fixture batch submission.
use contextdb_core::{TenantId, Value};
use contextdb_engine::protocol::{MessageType, decode};
use contextdb_engine::sync_types::NaturalKey;
use contextdb_engine::transport::iroh::IrohServer;
use contextdb_engine::transport::{
    ClientTransport, TransportFuture, TransportStatusFuture, client_transport,
};
use contextdb_engine::{
    ApplicationTablePolicyExpectation, Database, DeliveryManifest, FabricIdentity, SyncClient,
    SyncServer,
};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Default)]
struct Counts {
    requests: AtomicU64,
    status: AtomicU64,
    bytes: AtomicU64,
}
impl Counts {
    fn read(&self) -> (u64, u64, u64) {
        (
            self.requests.load(Ordering::SeqCst),
            self.status.load(Ordering::SeqCst),
            self.bytes.load(Ordering::SeqCst),
        )
    }
    fn observe(&self, bytes: &[u8]) {
        self.requests.fetch_add(1, Ordering::SeqCst);
        self.bytes.fetch_add(bytes.len() as u64, Ordering::SeqCst);
        if decode(bytes).is_ok_and(|e| e.message_type == MessageType::StatusRequest) {
            self.status.fetch_add(1, Ordering::SeqCst);
        }
    }
}
struct Observed {
    inner: Arc<dyn ClientTransport>,
    counts: Arc<Counts>,
}
impl ClientTransport for Observed {
    fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.ensure_connected()
    }
    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        self.inner.is_connected()
    }
    fn peer_node_id(&self) -> Option<String> {
        self.inner.peer_node_id()
    }
    fn local_node_id(&self) -> Option<String> {
        self.inner.local_node_id()
    }
    fn has_stable_edge_identity(&self) -> bool {
        self.inner.has_stable_edge_identity()
    }
    fn request<'a>(
        &'a self,
        subject: &'a str,
        bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        self.counts.observe(&bytes);
        self.inner.request(subject, bytes, timeout)
    }
    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        self.counts.observe(&bytes);
        self.inner.request_single_reply(subject, bytes, timeout)
    }
}
fn delta(a: (u64, u64, u64), b: (u64, u64, u64)) -> (u64, u64, u64) {
    (a.0 - b.0, a.1 - b.1, a.2 - b.2)
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 3 {
        return Err("usage: custody_scale EMPTY_TASK_DIRECTORY UNIT_COUNT".into());
    }
    let root = std::path::Path::new(&args[1]);
    std::fs::create_dir_all(root)?;
    let total: usize = args[2].parse()?;
    let tenant = TenantId::from("correction01-scale");
    let hub = Arc::new(Database::open(root.join("hub.db"))?);
    let endpoint = IrohServer::bind(&format!(
        "iroh:?identity={}",
        root.join("hub.key").display()
    ))
    .await?;
    let server = Arc::new(SyncServer::new(hub.clone(), &endpoint, tenant.clone()));
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });
    let clauses = "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST DELIVERY MANIFEST OVER parts";
    let p = HashMap::new();
    hub.execute(
        &format!("DECLARE TENANT TABLE POLICY records {clauses}"),
        &p,
    )?;
    hub.execute(
        "DECLARE TENANT TABLE POLICY parts SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST",
        &p,
    )?;
    let edge = Arc::new(Database::open(root.join("edge.db"))?);
    let identity_path = root.join("edge.key");
    let identity = Arc::new(FabricIdentity::load_or_generate(&identity_path)?);
    let spec = contextdb_engine::transport::peer_dial_spec(&endpoint.ticket(), &identity_path);
    let counts = Arc::new(Counts::default());
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        Arc::new(Observed {
            inner: client_transport(&spec),
            counts: counts.clone(),
        }),
        tenant,
        identity,
    );
    client
        .bind_application_table_policy(
            ApplicationTablePolicyExpectation::new()
                .expect_table("records", clauses)?
                .expect_table("parts", "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST")?,
        )
        .await?;
    edge.execute(
        &format!("CREATE TABLE records (id UUID PRIMARY KEY, body TEXT) {clauses}"),
        &p,
    )?;
    edge.execute("CREATE TABLE parts (id UUID PRIMARY KEY, record_id UUID REFERENCES records(id), body TEXT) SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST",&p)?;
    client.push().await?;
    let start = Instant::now();
    let mut block = Instant::now();
    let mut edge_before = edge.__custody_metadata_work_for_test()?;
    let mut hub_before = hub.__custody_metadata_work_for_test()?;
    let mut network_before = counts.read();
    let mut block_start = 0;
    for i in 1..=total {
        let id = uuid::Uuid::new_v4();
        let tx = edge.begin()?;
        edge.insert_row(
            tx,
            "records",
            HashMap::from([
                ("id".into(), Value::Uuid(id)),
                ("body".into(), Value::Text(format!("unit {i}"))),
            ]),
        )?;
        let members = if i % 2 == 0 {
            let member = uuid::Uuid::new_v4();
            edge.insert_row(
                tx,
                "parts",
                HashMap::from([
                    ("id".into(), Value::Uuid(member)),
                    ("record_id".into(), Value::Uuid(id)),
                    ("body".into(), Value::Text(format!("member {i}"))),
                ]),
            )?;
            vec![(
                "parts",
                NaturalKey::single("id".into(), Value::Uuid(member)),
            )]
        } else {
            vec![]
        };
        edge.register_delivery_manifest(
            tx,
            DeliveryManifest {
                root_table: "records",
                root_key: NaturalKey::single("id".into(), Value::Uuid(id)),
                members,
            },
        )?;
        edge.commit(tx)?;
        let applied = client.push().await?;
        assert_eq!(applied.applied_rows, if i % 2 == 0 { 2 } else { 1 });
        assert!(applied.conflicts.is_empty());
        if i == 20 || i % 500 == 0 || i == total {
            let e = edge.__custody_metadata_work_for_test()?;
            let h = hub.__custody_metadata_work_for_test()?;
            let n = counts.read();
            let units = i - block_start;
            println!(
                "{}",
                serde_json::json!({"statement":"9/11/14/15","phase":"ordinary_pushes","from":block_start+1,"through":i,"units":units,"elapsed_ms":block.elapsed().as_millis(),"edge_work":delta(e,edge_before),"hub_work":delta(h,hub_before),"requests":n.0-network_before.0,"status_requests":n.1-network_before.1,"request_bytes":n.2-network_before.2})
            );
            for operation in ["push", "pull"] {
                let e0 = edge.__custody_metadata_work_for_test()?;
                let h0 = hub.__custody_metadata_work_for_test()?;
                let n0 = counts.read();
                let t = Instant::now();
                for _ in 0..3 {
                    if operation == "push" {
                        let r = client.push().await?;
                        assert_eq!(r.applied_rows, 0);
                    } else {
                        client.pull_default().await?;
                    }
                }
                let n = counts.read();
                println!(
                    "{}",
                    serde_json::json!({"statement":"15","phase":"steady","after_units":i,"operation":operation,"repetitions":3,"elapsed_us":t.elapsed().as_micros(),"requests":n.0-n0.0,"status_requests":n.1-n0.1,"request_bytes":n.2-n0.2,"edge_work":delta(edge.__custody_metadata_work_for_test()?,e0),"hub_work":delta(hub.__custody_metadata_work_for_test()?,h0)})
                );
            }
            block_start = i;
            block = Instant::now();
            edge_before = edge.__custody_metadata_work_for_test()?;
            hub_before = hub.__custody_metadata_work_for_test()?;
            network_before = counts.read();
        }
    }
    let status = edge.delivery_status("records")?;
    assert_eq!(status.accepted, total as u64);
    assert_eq!(status.pending, 0);
    let rows = hub.execute("SELECT COUNT(*) FROM records", &p)?;
    assert_eq!(rows.rows, vec![vec![Value::Int64(total as i64)]]);
    println!(
        "{}",
        serde_json::json!({"statement":"11/14","phase":"complete","units":total,"edge_accepted":status.accepted,"edge_pending":status.pending,"hub_count":format!("{:?}",rows.rows),"elapsed_ms":start.elapsed().as_millis()})
    );
    let before_status = edge.__custody_metadata_work_for_test()?;
    let repeated = edge.delivery_status("records")?;
    let status_work = delta(edge.__custody_metadata_work_for_test()?, before_status);
    assert_eq!(repeated, status);
    assert!(status_work.1 <= (total as u64).saturating_mul(16).saturating_add(32));
    println!(
        "{}",
        serde_json::json!({"statement":"14","phase":"indexed_status","units":total,"work":status_work})
    );
    client.shutdown().await;
    stop.store(true, Ordering::SeqCst);
    task.await?;
    drop(server);
    endpoint.close().await;
    Ok(())
}
