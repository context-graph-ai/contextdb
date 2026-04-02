use contextdb_engine::Database;
use contextdb_engine::sync_types::ConflictPolicies;
use contextdb_server::SyncClient;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use crate::common::process::workspace_root;

pub struct NatsFixture {
    _container: ContainerAsync<GenericImage>,
    pub nats_url: String,
    pub ws_url: String,
}

pub async fn start_nats() -> NatsFixture {
    let conf = workspace_root()
        .join("crates/contextdb-engine/tests/nats.conf")
        .to_string_lossy()
        .into_owned();
    let image = GenericImage::new("nats", "latest")
        .with_exposed_port(4222.tcp())
        .with_exposed_port(9222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"));
    let request = image
        .with_mount(Mount::bind_mount(conf, "/etc/nats/nats.conf"))
        .with_cmd(["--js", "--config", "/etc/nats/nats.conf"]);
    let container: ContainerAsync<GenericImage> =
        request.start().await.expect("NATS container should start");
    let nats_port = container
        .get_host_port_ipv4(4222.tcp())
        .await
        .expect("NATS port should be mapped");
    let ws_port = container
        .get_host_port_ipv4(9222.tcp())
        .await
        .expect("NATS websocket port should be mapped");
    NatsFixture {
        _container: container,
        nats_url: format!("nats://127.0.0.1:{nats_port}"),
        ws_url: format!("ws://127.0.0.1:{ws_port}"),
    }
}

pub async fn wait_for_server_ready(edge_url: &str, tenant: &str, policies: &ConflictPolicies) {
    let probe_db = Arc::new(Database::open_memory());
    let probe_client = SyncClient::new(probe_db, edge_url, tenant);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match probe_client.pull(policies).await {
            Ok(_) => return,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(err) => panic!("server did not become ready for tenant {tenant}: {err}"),
        }
    }
}
