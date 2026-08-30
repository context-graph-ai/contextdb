//! Server resource policy is explicit configuration, never an endpoint-string
//! side channel.

#![cfg(feature = "iroh")]

use std::collections::HashSet;
use std::net::UdpSocket;
use std::path::Path;

use contextdb_engine::transport::{
    PeerEndpoint, PeerEndpointSpec as EndpointSpec, ServerResourcePolicy, peer_bind_spec,
    peer_dial_spec, server_transport_with_resource_policy,
};

const FORMER_ENDPOINT_POLICY_KEYS: [&str; 4] = [
    "response-staging-bytes",
    "pre-admission-connections",
    "pre-admission-bytes",
    "request-read-idle-ms",
];

const VALID_ENDPOINT_TICKET: &str =
    "endpointacxfr74igmsbvsbnn73wcecg5vt3kbzncqwfrdiampuufwnhkublmaa";

#[test]
fn endpoint_specs_keep_transport_identity_and_routing() {
    let endpoint = format!(
        "{}&port=4433&relay=none&publish=n0&lookup=mdns",
        peer_bind_spec(Path::new("/tmp/contextdb-hub.key"))
    );
    let routed = EndpointSpec::parse_detailed(&endpoint)
        .expect("transport identity and routing remain a valid endpoint specification")
        .expect("peer endpoint");
    assert_eq!(routed.port(), Some(4433));
    assert!(routed.identity_path().is_some());
    assert!(routed.publishes_address_lookup());
}

fn assert_policy_key_is_refused(endpoint: String, key: &str) {
    let flag = format!("--{key}");
    let error = EndpointSpec::parse_detailed(&endpoint)
        .expect_err("endpoint specs must loudly refuse former resource-policy keys");
    assert_eq!(
        error,
        safe_policy_remedy(&flag),
        "{endpoint} must name exactly one safe top-level remedy"
    );
}

fn safe_policy_remedy(flag: &str) -> String {
    format!("server resource policy belongs in top-level `{flag}`, not sync endpoint spec")
}

#[test]
fn bind_endpoint_specs_refuse_every_policy_key() {
    for key in FORMER_ENDPOINT_POLICY_KEYS {
        assert_policy_key_is_refused(
            format!(
                "{}&{key}=1",
                peer_bind_spec(Path::new("/tmp/contextdb-hub.key"))
            ),
            key,
        );
    }
}

#[test]
fn dial_endpoint_specs_refuse_every_policy_key() {
    EndpointSpec::parse_detailed(VALID_ENDPOINT_TICKET)
        .expect("ticket fixture parses")
        .expect("ticket fixture is a peer endpoint");
    for key in FORMER_ENDPOINT_POLICY_KEYS {
        assert_policy_key_is_refused(
            format!(
                "{}&{key}=1",
                peer_dial_spec(VALID_ENDPOINT_TICKET, Path::new("/tmp/contextdb-edge.key"))
            ),
            key,
        );
    }
}

#[test]
fn shipped_resource_policy_defaults_are_stable() {
    assert_eq!(
        ServerResourcePolicy::default(),
        ServerResourcePolicy {
            response_staging_bytes: None,
            pre_admission_connections: 128,
            pre_admission_bytes: 64 * 1024 * 1024,
            request_read_idle_ms: 30_000,
        }
    );
}

#[test]
fn lazy_factory_accepts_an_explicit_top_level_policy_without_binding() {
    let policy = ServerResourcePolicy {
        response_staging_bytes: Some(1),
        pre_admission_connections: 1,
        pre_admission_bytes: 1,
        request_read_idle_ms: 1,
    };
    let endpoint = peer_bind_spec(Path::new("/tmp/hub.key"));
    let _ = server_transport_with_resource_policy(&endpoint, policy);
}

fn reserve_loopback_udp_socket() -> UdpSocket {
    UdpSocket::bind(("127.0.0.1", 0)).expect("reserve a local UDP port")
}

fn runtime_marker(root: &Path) -> String {
    let tempfile_component = root
        .file_name()
        .expect("temporary directory has a final path component")
        .to_string_lossy();
    format!("runtime-resource-policy-{tempfile_component}")
}

fn runtime_nonnumeric_policy_value(marker: &str, key: &str) -> String {
    format!("resource-value-{key}-{marker}")
}

#[allow(clippy::too_many_arguments)]
fn assert_redacted_eager_bind_refusal(
    error: impl std::fmt::Display,
    endpoint: &str,
    ticket: Option<&str>,
    identity: &Path,
    flag: &str,
    secret: &str,
    marker: &str,
    reservation: &UdpSocket,
    port: u16,
) {
    let error = error.to_string();
    assert_eq!(
        error,
        format!("transport unreachable: {}", safe_policy_remedy(flag)),
        "eager bind must preserve exactly one safe top-level {flag} remedy"
    );
    assert!(
        !error.contains(endpoint),
        "eager bind error must not render the endpoint: {error}"
    );
    if let Some(ticket) = ticket {
        assert!(
            !error.contains(ticket),
            "eager bind error must not render the enrollment ticket: {error}"
        );
    }
    assert!(
        !error.contains(secret),
        "eager bind error must not render the policy value: {error}"
    );
    assert!(
        !error.contains(marker),
        "eager bind error must not render a recognizable runtime value marker: {error}"
    );
    assert!(
        !error.contains(identity.to_string_lossy().as_ref()),
        "eager bind error must not render the identity path: {error}"
    );
    assert!(
        !identity.exists(),
        "eager bind must refuse {flag} before generating {identity:?}"
    );
    assert!(
        reservation
            .local_addr()
            .expect("read held local UDP reservation")
            .port()
            == port,
        "eager bind must refuse {flag} while the local UDP reservation remains held"
    );
}

#[tokio::test]
async fn every_public_eager_binder_redacts_former_policy_values_before_identity_or_socket_effects()
{
    let mut generated_values = HashSet::new();
    for (form, ticket) in [("bind", None), ("dial", Some(VALID_ENDPOINT_TICKET))] {
        for key in FORMER_ENDPOINT_POLICY_KEYS {
            let flag = format!("--{key}");
            for binder in ["bind", "bind_with_resource_policy"] {
                let root = tempfile::tempdir().expect("temporary endpoint directory");
                let marker = runtime_marker(root.path());
                let secret = runtime_nonnumeric_policy_value(&marker, key);
                assert!(
                    generated_values.insert(secret.clone()),
                    "every eager-bind policy value must be materially distinct"
                );
                let identity = root.path().join(format!("{form}-{key}-{binder}.key"));
                let reservation = reserve_loopback_udp_socket();
                let port = reservation
                    .local_addr()
                    .expect("read held local UDP reservation")
                    .port();
                let base = match ticket {
                    Some(ticket) => peer_dial_spec(ticket, &identity),
                    None => peer_bind_spec(&identity),
                };
                let endpoint = format!("{base}&port={port}&{key}={secret}");

                match binder {
                    "bind" => {
                        let error = PeerEndpoint::bind(&endpoint)
                            .await
                            .err()
                            .expect("former endpoint policy keys must refuse eager bind");
                        assert_redacted_eager_bind_refusal(
                            error,
                            &endpoint,
                            ticket,
                            &identity,
                            &flag,
                            &secret,
                            &marker,
                            &reservation,
                            port,
                        );
                    }
                    "bind_with_resource_policy" => {
                        let error = PeerEndpoint::bind_with_resource_policy(
                            &endpoint,
                            ServerResourcePolicy::default(),
                        )
                        .await
                        .err()
                        .expect("former endpoint policy keys must refuse eager policy bind");
                        assert_redacted_eager_bind_refusal(
                            error,
                            &endpoint,
                            ticket,
                            &identity,
                            &flag,
                            &secret,
                            &marker,
                            &reservation,
                            port,
                        );
                    }
                    _ => unreachable!("the eager binder matrix is fixed"),
                }
            }
        }
    }
}
