//! Server resource policy is supplied by flags, not endpoint strings.
//! Invalid policy is a usage error before database open.

use std::collections::HashSet;
use std::net::UdpSocket;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use contextdb_server::transport::{ServerResourcePolicy, server_transport_with_resource_policy};
use contextdb_server::{PeerEndpoint, peer_bind_spec, peer_dial_spec};

const POLICY_FLAGS: [(&str, &str); 4] = [
    ("--response-staging-bytes", "1048576"),
    ("--pre-admission-connections", "3"),
    ("--pre-admission-bytes", "2097152"),
    ("--request-read-idle-ms", "1234"),
];

const POLICY_FLAG_NAMES: [&str; 4] = [
    "--response-staging-bytes",
    "--pre-admission-connections",
    "--pre-admission-bytes",
    "--request-read-idle-ms",
];

const FORMER_ENDPOINT_POLICY_KEYS: [&str; 4] = [
    "response-staging-bytes",
    "pre-admission-connections",
    "pre-admission-bytes",
    "request-read-idle-ms",
];

const VALID_ENDPOINT_TICKET: &str =
    "endpointacxfr74igmsbvsbnn73wcecg5vt3kbzncqwfrdiampuufwnhkublmaa";

#[test]
fn resource_policy_is_exposed_as_four_top_level_server_flags() {
    let output = Command::new(env!("CARGO_BIN_EXE_contextdb-server"))
        .arg("--help")
        .output()
        .expect("run server help");
    assert!(output.status.success());
    let help = String::from_utf8_lossy(&output.stdout);
    for (flag, _) in POLICY_FLAGS {
        assert!(
            help.contains(flag),
            "missing top-level resource flag {flag}"
        );
    }
}

#[test]
fn distinct_valid_resource_flags_reach_eager_binding() {
    for (index, values) in [
        ["7340032", "3", "2097152", "1234"],
        ["9437184", "5", "3145728", "2345"],
    ]
    .into_iter()
    .enumerate()
    {
        let root = tempfile::tempdir().expect("temporary server directory");
        let identity = root.path().join(format!("hub-{index}.key"));
        let endpoint = peer_bind_spec(&identity);
        let output = Command::new(env!("CARGO_BIN_EXE_contextdb-server"))
            .args([
                "--db-path",
                ":memory:",
                "--tenant-id",
                "acme",
                "--sync-endpoint",
                &endpoint,
                "--response-staging-bytes",
                values[0],
                "--pre-admission-connections",
                values[1],
                "--pre-admission-bytes",
                values[2],
                "--request-read-idle-ms",
                values[3],
                "--show-ticket",
            ])
            .env("RUST_LOG", "off")
            .output()
            .expect("spawn server");
        assert!(
            output.status.success(),
            "valid resource flags failed eager binding: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(identity.exists(), "eager bind did not create its identity");
    }
}

#[test]
fn server_lazy_factory_accepts_an_explicit_top_level_policy_without_binding() {
    let policy = ServerResourcePolicy {
        response_staging_bytes: Some(7 * 1024 * 1024),
        pre_admission_connections: 3,
        pre_admission_bytes: 2 * 1024 * 1024,
        request_read_idle_ms: 1_234,
    };
    let endpoint = peer_bind_spec(std::path::Path::new("/tmp/hub.key"));
    let _ = server_transport_with_resource_policy(&endpoint, policy);
}

#[test]
fn every_invalid_resource_value_is_a_usage_error_before_database_open() {
    let too_many_connections = tokio::sync::Semaphore::MAX_PERMITS
        .checked_add(1)
        .expect("semaphore maximum leaves a representable invalid value")
        .to_string();
    let invalids = [
        ("--response-staging-bytes", Some("0".to_string())),
        ("--response-staging-bytes", None),
        ("--pre-admission-connections", Some("0".to_string())),
        ("--pre-admission-connections", Some(too_many_connections)),
        ("--pre-admission-connections", None),
        ("--pre-admission-bytes", Some("0".to_string())),
        ("--pre-admission-bytes", Some("4294967296".to_string())),
        ("--pre-admission-bytes", None),
        ("--request-read-idle-ms", Some("0".to_string())),
        ("--request-read-idle-ms", Some(u64::MAX.to_string())),
        ("--request-read-idle-ms", None),
    ];
    let mut generated_values = HashSet::new();

    for (index, (flag, fixed_value)) in invalids.into_iter().enumerate() {
        let root = tempfile::tempdir().expect("temporary database directory");
        let marker = runtime_marker(root.path());
        let value = fixed_value.unwrap_or_else(|| {
            let value = runtime_nonnumeric_policy_value(&marker, flag);
            assert!(
                generated_values.insert(value.clone()),
                "every numeric flag must receive a materially distinct runtime value"
            );
            value
        });
        let db_path = root
            .path()
            .join(format!("{marker}-never-opened-{index}.db"));
        let identity_path =
            std::path::PathBuf::from(format!("{}.fabric-identity.key", db_path.display()));
        let output = Command::new(env!("CARGO_BIN_EXE_contextdb-server"))
            .args(["--db-path"])
            .arg(&db_path)
            .args(["--tenant-id", "acme", flag, value.as_str(), "--show-ticket"])
            .output()
            .expect("spawn server");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(2), "{flag}={value}: {stderr}");
        assert!(
            !stderr.contains("unexpected argument"),
            "{flag} must be recognized then rejected for its value: {stderr}"
        );
        assert!(output.stdout.is_empty(), "{flag} wrote stdout: {stderr}");
        assert!(
            !stderr.contains(db_path.to_string_lossy().as_ref()),
            "{flag} must not render its supplied database path: {stderr}"
        );
        assert!(
            !stderr.contains(identity_path.to_string_lossy().as_ref()),
            "{flag} must not render its derived identity path: {stderr}"
        );
        assert!(
            !stderr.contains(&marker),
            "{flag} must not render a distinctive runtime path marker: {stderr}"
        );
        assert!(
            !db_path.exists(),
            "{flag}={value} must fail before opening {db_path:?}"
        );
        assert!(
            !identity_path.exists(),
            "{flag}={value} must fail before creating {identity_path:?}"
        );
        assert!(
            root.path()
                .read_dir()
                .expect("read untouched temporary directory")
                .next()
                .is_none(),
            "{flag}={value} must not create any database or identity side effect"
        );
    }
}

#[test]
fn nonnumeric_resource_flags_are_redacted_usage_errors_before_database_or_identity_creation() {
    let mut generated_values = HashSet::new();
    for (index, flag) in POLICY_FLAG_NAMES.into_iter().enumerate() {
        let root = tempfile::tempdir().expect("temporary server directory");
        let marker = runtime_marker(root.path());
        let secret = runtime_nonnumeric_policy_value(&marker, flag);
        assert!(
            generated_values.insert(secret.clone()),
            "every numeric flag must receive a materially distinct runtime value"
        );
        let db_path = root
            .path()
            .join(format!("{marker}-never-opened-{index}.db"));
        let identity_path =
            std::path::PathBuf::from(format!("{}.fabric-identity.key", db_path.display()));
        let output = Command::new(env!("CARGO_BIN_EXE_contextdb-server"))
            .args(["--db-path"])
            .arg(&db_path)
            .args(["--tenant-id", "acme", flag])
            .arg(&secret)
            .arg("--show-ticket")
            .output()
            .expect("spawn server");
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr_lowercase = stderr.to_ascii_lowercase();

        assert_eq!(output.status.code(), Some(2), "{flag}: {stderr}");
        assert!(output.stdout.is_empty(), "{flag} wrote stdout: {stderr}");
        assert!(
            stderr.contains(flag),
            "{flag} must name the bad flag: {stderr}"
        );
        assert!(
            stderr_lowercase.contains("invalid value") && stderr_lowercase.contains("whole number"),
            "{flag} must give a generic invalid-value remedy: {stderr}"
        );
        assert!(
            !stderr.contains(&secret),
            "{flag} must not echo its supplied value: {stderr}"
        );
        assert!(
            !stderr.contains(&marker),
            "{flag} must not echo a recognizable runtime value marker: {stderr}"
        );
        assert!(
            !stderr.contains(db_path.to_string_lossy().as_ref()),
            "{flag} must not echo its supplied database path: {stderr}"
        );
        assert!(
            !stderr.contains(identity_path.to_string_lossy().as_ref()),
            "{flag} must not echo its derived identity path: {stderr}"
        );
        assert!(
            !db_path.exists(),
            "{flag} must fail before opening {db_path:?}"
        );
        assert!(
            !identity_path.exists(),
            "{flag} must fail before creating {identity_path:?}"
        );
        assert!(
            root.path()
                .read_dir()
                .expect("read untouched temporary directory")
                .next()
                .is_none(),
            "{flag} must not create any database or identity side effect"
        );
    }
}

#[test]
fn hyphen_prefixed_resource_values_are_safe_flag_specific_usage_refusals_before_startup() {
    let mut generated_values = HashSet::new();

    for flag in POLICY_FLAG_NAMES {
        for prefix in ["-", "--"] {
            let invocation = invalid_resource_policy_cli_invocation(flag, prefix, None, true);
            assert!(
                generated_values.insert(invocation.secret.clone()),
                "every hyphen-prefixed resource value must be materially distinct"
            );
            assert_safe_resource_policy_cli_refusal(invocation, flag);
        }
    }
}

#[test]
fn later_invalid_resource_values_are_attributed_to_their_own_flag_in_every_order_and_syntax() {
    let mut generated_values = HashSet::new();

    for (earlier_flag, earlier_value) in POLICY_FLAGS {
        for (invalid_flag, _) in POLICY_FLAGS {
            if earlier_flag == invalid_flag {
                continue;
            }

            for separate_value in [false, true] {
                let invocation = invalid_resource_policy_cli_invocation(
                    invalid_flag,
                    "",
                    Some((earlier_flag, earlier_value)),
                    separate_value,
                );
                assert!(
                    generated_values.insert(invocation.secret.clone()),
                    "every ordered resource-flag pair and value syntax must receive a distinct value"
                );
                assert_safe_resource_policy_cli_refusal(invocation, invalid_flag);
            }
        }
    }
}

#[test]
fn endpoint_policy_keys_are_loudly_refused_before_database_open() {
    for (flag, value) in POLICY_FLAGS {
        let root = tempfile::tempdir().expect("temporary database directory");
        let db_path = root.path().join("never-opened.db");
        let endpoint = format!(
            "{}&{}={}",
            peer_bind_spec(&root.path().join("hub.key")),
            flag.trim_start_matches("--"),
            value
        );
        let output = Command::new(env!("CARGO_BIN_EXE_contextdb-server"))
            .args(["--db-path"])
            .arg(&db_path)
            .args([
                "--tenant-id",
                "acme",
                "--sync-endpoint",
                &endpoint,
                "--show-ticket",
            ])
            .output()
            .expect("spawn server");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(2), "{endpoint}: {stderr}");
        assert!(
            stderr.contains("top-level") && stderr.contains(flag),
            "{endpoint}: {stderr}"
        );
        assert!(
            !db_path.exists(),
            "endpoint policy must fail before database open"
        );
    }
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

struct InvalidResourcePolicyCliInvocation {
    root: tempfile::TempDir,
    db_path: PathBuf,
    endpoint: String,
    identity_path: PathBuf,
    secret: String,
    output: Output,
    reservation: UdpSocket,
    port: u16,
}

fn invalid_resource_policy_cli_invocation(
    invalid_flag: &str,
    secret_prefix: &str,
    earlier_valid_flag: Option<(&str, &str)>,
    separate_value: bool,
) -> InvalidResourcePolicyCliInvocation {
    let root = tempfile::tempdir().expect("temporary server directory");
    let marker = runtime_marker(root.path());
    let secret = format!(
        "{secret_prefix}{}",
        runtime_nonnumeric_policy_value(&marker, invalid_flag)
    );
    let db_path = root.path().join(format!("{marker}-never-opened.db"));
    let identity_path = root.path().join(format!("{marker}-never-created.key"));
    let reservation = reserve_loopback_udp_socket();
    let port = reservation
        .local_addr()
        .expect("read held local UDP reservation")
        .port();
    let endpoint = format!("{}&port={port}", peer_bind_spec(&identity_path));
    let mut command = Command::new(env!("CARGO_BIN_EXE_contextdb-server"));
    command
        .args(["--db-path"])
        .arg(&db_path)
        .args(["--tenant-id", "acme", "--sync-endpoint"])
        .arg(&endpoint);
    if let Some((flag, value)) = earlier_valid_flag {
        command.args([flag, value]);
    }
    if separate_value {
        command.arg(invalid_flag).arg(&secret);
    } else {
        command.arg(format!("{invalid_flag}={secret}"));
    }
    command.arg("--show-ticket");
    let output = command.output().expect("spawn server");

    InvalidResourcePolicyCliInvocation {
        root,
        db_path,
        endpoint,
        identity_path,
        secret,
        output,
        reservation,
        port,
    }
}

fn assert_safe_resource_policy_cli_refusal(
    invocation: InvalidResourcePolicyCliInvocation,
    invalid_flag: &str,
) {
    let stderr = String::from_utf8_lossy(&invocation.output.stderr);
    let expected =
        format!("Error: invalid value for {invalid_flag}: expected a positive whole number\n");

    assert_eq!(
        invocation.output.status.code(),
        Some(2),
        "{invalid_flag} must be a usage error"
    );
    assert!(
        invocation.output.stdout.is_empty(),
        "{invalid_flag} refusal must not write stdout"
    );
    assert!(
        stderr == expected,
        "{invalid_flag} must emit exactly its flag-specific positive-whole-number remedy"
    );
    assert!(
        !stderr.contains(&invocation.secret),
        "{invalid_flag} refusal must not render its supplied value"
    );
    assert!(
        !stderr.contains(invocation.db_path.to_string_lossy().as_ref()),
        "{invalid_flag} refusal must not render its database path"
    );
    assert!(
        !stderr.contains(invocation.identity_path.to_string_lossy().as_ref()),
        "{invalid_flag} refusal must not render its identity path"
    );
    assert!(
        !stderr.contains(&invocation.endpoint),
        "{invalid_flag} refusal must not render its endpoint"
    );
    for other_flag in POLICY_FLAG_NAMES {
        if other_flag != invalid_flag {
            assert!(
                !stderr.contains(other_flag),
                "{invalid_flag} refusal must not offer another resource flag's remedy"
            );
        }
    }
    assert!(
        !invocation.db_path.exists(),
        "{invalid_flag} must fail before opening its database"
    );
    assert!(
        !invocation.identity_path.exists(),
        "{invalid_flag} must fail before creating its identity"
    );
    assert!(
        invocation
            .root
            .path()
            .read_dir()
            .expect("read untouched temporary directory")
            .next()
            .is_none(),
        "{invalid_flag} must fail before creating database or identity effects"
    );
    assert_eq!(
        invocation
            .reservation
            .local_addr()
            .expect("read held local UDP reservation")
            .port(),
        invocation.port,
        "{invalid_flag} must refuse while the requested socket remains reserved"
    );
}

fn safe_policy_remedy(flag: &str) -> String {
    format!("server resource policy belongs in top-level `{flag}`, not sync endpoint spec")
}

#[allow(clippy::too_many_arguments)]
fn assert_redacted_eager_bind_refusal(
    error: impl std::fmt::Display,
    endpoint: &str,
    ticket: Option<&str>,
    identity: &std::path::Path,
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
