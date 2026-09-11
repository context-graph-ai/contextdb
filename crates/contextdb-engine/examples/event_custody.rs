//! Runnable declare/bind/push/read recipe (see skills/sync).
use contextdb_core::{TenantId, Value};
use contextdb_engine::sync_types::NaturalKey;
use contextdb_engine::{ApplicationTablePolicyExpectation, Database, DeliveryManifest, SyncClient};
use std::{collections::HashMap, sync::Arc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 3 {
        return Err("usage: event_custody EDGE_DB HUB_TICKET_FILE".into());
    }
    let db = Arc::new(Database::open(&args[1])?);
    let ticket = std::fs::read_to_string(&args[2])?;
    let client = SyncClient::new(db.clone(), ticket.trim(), TenantId::from("custody-demo"));
    let expectation = ApplicationTablePolicyExpectation::new()
        .expect_table(
            "records",
            "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST DELIVERY MANIFEST OVER record_parts",
        )?
        .expect_table("record_parts", "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST")?;
    client.bind_application_table_policy(expectation).await?;
    let params = HashMap::new();
    db.execute("CREATE TABLE records (id UUID PRIMARY KEY, body TEXT) SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST DELIVERY MANIFEST OVER record_parts", &params)?;
    db.execute("CREATE TABLE record_parts (id UUID PRIMARY KEY, record_id UUID REFERENCES records(id), body TEXT) SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST", &params)?;
    let root = uuid::Uuid::new_v4();
    let tx = db.begin()?;
    db.insert_row(
        tx,
        "records",
        HashMap::from([
            ("id".into(), Value::Uuid(root)),
            ("body".into(), Value::Text("one complete record".into())),
        ]),
    )?;
    db.register_delivery_manifest(
        tx,
        DeliveryManifest {
            root_table: "records",
            root_key: NaturalKey::single("id".into(), Value::Uuid(root)),
            members: vec![],
        },
    )?;
    db.commit(tx)?;
    client.push().await.map_err(std::io::Error::other)?;
    let outcomes = client.fetch_delivery_outcomes(None).await?;
    let status = db.delivery_status("records")?;
    println!(
        "accepted={} pending={} read_back={}",
        status.accepted,
        status.pending,
        outcomes.len()
    );
    client.shutdown().await;
    Ok(())
}
