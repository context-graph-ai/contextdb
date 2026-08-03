use std::path::{Path, PathBuf};

fn from_manifest(rel: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(rel)
}

fn collect_rs(dir: &Path, out: &mut Vec<PathBuf>) {
    if !dir.exists() {
        return;
    }
    for entry in std::fs::read_dir(dir).expect("read_dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            collect_rs(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

fn allowed_transport_types(file: &Path) -> &'static [&'static str] {
    if file == from_manifest("../contextdb-engine/src/blob_store.rs") {
        &["IrohServer"]
    } else if file == from_manifest("src/main.rs") {
        &["EndpointSpec", "IrohServer"]
    } else if file == from_manifest("src/smoke_driver.rs") {
        &[
            "ProductionSmokeCheckpoint",
            "ProductionSmokeGateKind",
            "arm_production_smoke_gate",
        ]
    } else {
        &[]
    }
}

fn assert_no_unsanctioned_transport_type(file: &Path, text: &str) {
    for statement in text.split(';') {
        let compact: String = statement.split_whitespace().collect();
        if compact.starts_with("pubuse") && compact.contains("transport") {
            assert_eq!(
                compact, "pubusetransport::in_process::InProcessBroker",
                "{file:?} must not re-export an adapter type behind a neutral product name"
            );
        }
        let imports_transport = compact.starts_with("use") && compact.contains("transport");
        if imports_transport {
            let imports_adapter_module = compact.ends_with("::iroh")
                || compact.contains("::irohas")
                || compact.ends_with("::transport")
                || compact.contains("::transportas")
                || (compact.contains("transport::{")
                    && (compact.contains("self") || compact.contains("iroh")));
            assert!(
                !imports_adapter_module,
                "{file:?} must import sanctioned adapter types, never the transport/iroh module: {statement}"
            );
        }
    }
    assert!(
        !text.contains("use crate::transport;")
            && !text.contains("use contextdb_server::transport;")
            && !text.contains("use crate::transport as ")
            && !text.contains("use contextdb_server::transport as ")
            && !text.contains("use crate::{transport")
            && !text.contains("use contextdb_server::{transport")
            && !text.contains("transport as ")
            && !text.contains("use crate as ")
            && !text.contains("use contextdb_server as "),
        "{file:?} must not import the transport module for an alias-based backend-type escape"
    );
    assert!(
        !text.contains("transport::iroh as "),
        "{file:?} must not alias the transport adapter module"
    );
    assert!(
        !text.contains("transport::{iroh") && !text.contains("transport::{ iroh"),
        "{file:?} must not import the whole transport adapter module"
    );

    const NEEDLES: [&str; 1] = ["transport::iroh::"];
    let allow = allowed_transport_types(file);

    for needle in NEEDLES {
        let mut rest = text;
        while let Some(pos) = rest.find(needle) {
            let after = &rest[pos + needle.len()..];
            rest = after;
            if let Some(braced) = after.strip_prefix('{') {
                let close = braced.find('}').unwrap_or(braced.len());
                for raw in braced[..close].split(',') {
                    let name = raw.split_whitespace().next().unwrap_or("").trim();
                    if !name.is_empty() {
                        assert!(
                            allow.contains(&name),
                            "unexpected transport type `{name}` in {file:?}"
                        );
                    }
                }
            } else {
                let ident: String = after
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                let token = if ident.is_empty() {
                    after
                        .chars()
                        .next()
                        .map(|c| c.to_string())
                        .unwrap_or_default()
                } else {
                    ident
                };
                assert!(
                    allow.contains(&token.as_str()),
                    "unexpected transport type `{token}` in {file:?}"
                );
            }
        }
    }
}

#[test]
fn fetch_backend_type_is_contained_to_the_transport_adapter() {
    for escape in [
        "use crate::transport::iroh; type Leak = iroh::PeerConnection;",
        "use crate::transport::{self as t}; type Leak = t::iroh::PeerConnection;",
        "use crate::transport::{self, iroh}; type Leak = iroh::PeerConnection;",
        "use crate::transport as t; type Leak = t::iroh::PeerConnection;",
        "extern crate self as c; use c::transport::iroh::PeerConnection;",
        "use super::transport::iroh::PeerConnection;",
        "pub use crate::transport::NeutralStore; use crate::NeutralStore;",
    ] {
        assert!(
            std::panic::catch_unwind(|| {
                assert_no_unsanctioned_transport_type(Path::new("synthetic_escape.rs"), escape);
            })
            .is_err(),
            "containment scanner accepted an adapter escape: {escape}"
        );
    }

    let manifest = std::fs::read_to_string(from_manifest("Cargo.toml")).expect("server Cargo.toml");
    for line in manifest.lines() {
        assert!(
            !(line.contains("package") && line.contains("iroh-blobs")),
            "iroh-blobs must not be hidden behind a renamed dependency: {line}"
        );
    }

    let adapter = from_manifest("../contextdb-engine/src/transport");
    let mut files = Vec::new();
    collect_rs(&from_manifest("src"), &mut files);
    collect_rs(&from_manifest("../contextdb-engine/src"), &mut files);

    for file in files {
        if file.starts_with(&adapter) {
            continue;
        }
        let text = std::fs::read_to_string(&file).unwrap_or_default();
        for needle in ["iroh_blobs", "iroh-blobs", "iroh::blobs"] {
            assert!(
                !text.contains(needle),
                "the fetch backend `{needle}` leaked outside the transport adapter, in {file:?}"
            );
        }
        assert_no_unsanctioned_transport_type(&file, &text);
    }

    let file = from_manifest("../contextdb-engine/src/blob_store.rs");
    let text = std::fs::read_to_string(&file).expect("engine blob_store.rs");
    let transport_lines: Vec<_> = text
        .lines()
        .filter(|line| line.contains("transport"))
        .collect();
    assert_eq!(
        transport_lines,
        [
            "use crate::transport::adapter::{self, BlobStoreHandle, FetchFailure, FetchVerdict};",
            "use crate::transport::iroh::IrohServer;",
        ],
        "the resolver may name only IrohServer and the transport-neutral blob adapter facade"
    );
    assert!(
        !text.lines().any(|line| {
            line.contains("type") && line.contains("IrohServer") && line.contains('=')
        }),
        "the resolver must not re-export IrohServer behind a neutral type alias"
    );
    assert!(
        text.contains("crate::work_ledger") && text.contains("BlobHash"),
        "the resolver must carry contextdb's own work_ledger::BlobHash"
    );
}

#[test]
fn resolver_surface_carries_no_media_or_detector_type() {
    let adapter = from_manifest("../contextdb-engine/src/transport");
    let mut files = Vec::new();
    collect_rs(&from_manifest("src"), &mut files);
    files.push(from_manifest("../contextdb-engine/src/blob_store.rs"));
    for file in files {
        if file.starts_with(&adapter) {
            continue;
        }
        let text = std::fs::read_to_string(&file).unwrap_or_default();
        for banned in [
            "Clip",
            "Crop",
            "VideoFrame",
            "Transcript",
            "Detector",
            "Detection",
            "MediaType",
        ] {
            assert!(
                !text.contains(banned),
                "the resolver surface names `{banned}` in {file:?}"
            );
        }
    }
}
