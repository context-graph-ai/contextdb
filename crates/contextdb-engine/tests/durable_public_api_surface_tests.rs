//! External callers may inspect durable state but cannot forge it.

use contextdb_engine::Database;
use std::collections::BTreeSet;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::{Mutex, OnceLock};
use syn::punctuated::Punctuated;
use syn::visit::Visit;
use syn::{ImplItem, ImplItemFn, Item, ItemImpl, Meta, Type, Visibility};

fn engine_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn fixture(name: &str) -> PathBuf {
    engine_root().join("tests/fixtures").join(name)
}

fn cargo_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn cargo_fixture(path: &Path, command: &str, feature: Option<&str>, args: &[&str]) -> Output {
    let _serial = cargo_lock().lock().expect("fixture cargo lock");
    let target = engine_root()
        .ancestors()
        .nth(2)
        .expect("contextdb root")
        .join("target/public-api-surface-fixture");
    let mut cargo = Command::new("cargo");
    cargo
        .arg(command)
        .args(["--offline", "--locked", "--quiet", "--target-dir"])
        .arg(target)
        .arg("--manifest-path")
        .arg(path.join("Cargo.toml"));
    if let Some(feature) = feature {
        cargo.args(["--features", feature]);
    }
    if command == "run" {
        cargo.arg("--").args(args);
    }
    bounded_command_output(cargo, "fixture cargo command")
}

fn bounded_command_output(mut command: Command, name: &str) -> Output {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("spawn {name}: {error}"));
    let stdout = child.stdout.take().expect("command stdout");
    let stderr = child.stderr.take().expect("command stderr");
    let stdout_reader = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = std::io::BufReader::new(stdout).read_to_end(&mut bytes);
        bytes
    });
    let stderr_reader = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = std::io::BufReader::new(stderr).read_to_end(&mut bytes);
        bytes
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
    let status = loop {
        match child.try_wait().expect("poll fixture cargo") {
            Some(status) => break status,
            None if std::time::Instant::now() < deadline => std::thread::yield_now(),
            None => {
                child.kill().expect("stop overdue fixture cargo");
                let _ = child.wait();
                panic!("{name} did not exit within 300 seconds");
            }
        }
    };
    Output {
        status,
        stdout: stdout_reader.join().expect("fixture stdout reader"),
        stderr: stderr_reader.join().expect("fixture stderr reader"),
    }
}

fn diagnostic(output: &Output) -> String {
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn database_limit_setters_survive_reopen_like_sql_limits() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("durable-limit.db");
    let path = path.to_string_lossy().into_owned();
    let output = cargo_fixture(
        &fixture("public_api_allowed"),
        "run",
        Some("durable-limits"),
        &[&path],
    );
    assert!(
        output.status.success(),
        "Database-owned memory and disk setters must survive reopen exactly like SQL; {}",
        diagnostic(&output)
    );
}

fn forbidden_features(probes: &[(&str, &str)]) {
    let mut compiled = Vec::new();
    let mut unrelated = Vec::new();
    for (feature, symbol) in probes {
        let output = cargo_fixture(
            &fixture("public_api_forbidden"),
            "check",
            Some(feature),
            &[],
        );
        if output.status.success() {
            compiled.push((*feature, diagnostic(&output)));
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();
            let named = format!("`{}`", symbol.to_ascii_lowercase());
            let signature_refusal = *symbol == "new"
                && stderr.contains("syncserver::new")
                && stderr.contains("this function takes")
                && stderr.contains("argument");
            let refusal = stderr.contains("no method named")
                || stderr.contains("no function or associated item named")
                || stderr.contains("is private")
                || stderr.contains("cannot find")
                || signature_refusal;
            if !(stderr.contains(&symbol.to_ascii_lowercase())
                && (stderr.contains(&named) || stderr.contains("private"))
                && refusal)
                && !signature_refusal
            {
                unrelated.push((feature, symbol, diagnostic(&output)));
            }
        }
    }
    assert!(
        compiled.is_empty(),
        "external callers still compile forbidden probes: {compiled:#?}"
    );
    assert!(
        unrelated.is_empty(),
        "forbidden probes failed before their named door: {unrelated:#?}"
    );
}

#[test]
fn external_callers_cannot_mutate_sync_progress_or_raw_accounting() {
    forbidden_features(&[
        ("accountant_accessor", "accountant"),
        ("raw_accountant_mutation", "set_budget"),
        ("raw_accountant_allocate", "try_allocate"),
        ("raw_accountant_allocate_for", "try_allocate_for"),
        ("raw_accountant_release", "release"),
        ("accountant_constructor", "open_memory_with_accountant"),
        (
            "plugin_accountant_constructor",
            "open_memory_with_plugin_and_accountant",
        ),
        ("file_accountant_constructor", "open_with_config"),
        (
            "file_disk_accountant_constructor",
            "open_with_config_and_disk_limit",
        ),
        ("sync_watermark_setter", "set_sync_watermark"),
        ("sync_incarnation_writer", "sync_incarnation"),
        ("push_watermark_writer", "persist_sync_push_watermark"),
        (
            "pending_confirmation_writer",
            "persist_sync_pending_push_confirmation",
        ),
        ("pull_watermark_writer", "persist_sync_pull_watermark"),
        ("pull_cursor_writer", "persist_sync_pull_cursor"),
        ("applied_push_writer", "persist_sync_applied_push_watermark"),
        ("outbound_pending_writer", "mark_outbound_rows_pending"),
        ("confirmed_pending_writer", "refresh_confirmed_pending_rows"),
        ("record_hub_acceptance_writer", "record_hub_accepted_rows"),
        (
            "invalidate_hub_regression_writer",
            "invalidate_accepted_local_ordering_after_hub_regression",
        ),
        (
            "register_retention_peer_writer",
            "register_retention_sync_peer",
        ),
        ("change_retention_peer_writer", "change_retention_sync_peer"),
        ("sync_relay_mode_writer", "enable_sync_relay_mode"),
        ("database_policy_accessor", "conflict_policies"),
        (
            "callerless_applied_push_writer",
            "persist_sync_applied_push_watermark_for_node",
        ),
        ("client_pull_policy_map", "pull"),
        ("client_initial_sync_policy_map", "initial_sync"),
        ("client_direction_setter", "set_table_direction"),
        ("client_policy_setter", "set_conflict_policy"),
        (
            "client_default_policy_setter",
            "set_default_conflict_policy",
        ),
        ("client_arbitrary_transport", "with_transport"),
        ("server_arbitrary_transport", "with_transport"),
        ("server_policy_constructor", "new"),
        ("raw_apply_changes", "apply_changes"),
        ("raw_policy_apply", "apply_synced_changes"),
        (
            "raw_apply_changes_with_receipt",
            "apply_synced_changes_with_receipt",
        ),
        ("role_relative_policy_types", "conflictpolicy"),
        ("legacy_source_mutation", "execute"),
        ("raw_blob_repository_type", "blob_repository"),
        ("raw_blob_repository_accessor", "blob_repository"),
    ]);

    let allowed = cargo_fixture(&fixture("public_api_allowed"), "check", None, &[]);
    assert!(
        allowed.status.success(),
        "read-only durable inspection must remain public; {}",
        diagnostic(&allowed)
    );
}

#[test]
fn external_callers_cannot_inject_an_accountant_durability_bypass() {
    forbidden_features(&[
        ("accountant_constructor", "open_memory_with_accountant"),
        (
            "plugin_accountant_constructor",
            "open_memory_with_plugin_and_accountant",
        ),
        ("file_accountant_constructor", "open_with_config"),
        (
            "file_disk_accountant_constructor",
            "open_with_config_and_disk_limit",
        ),
    ]);
}

#[test]
fn callerless_applied_push_writer_is_absent() {
    let database = method_names(&database_public_methods(&engine_root()));
    assert!(
        !database.contains("persist_sync_applied_push_watermark_for_node"),
        "the callerless per-node applied-push writer must be removed from production Database"
    );
    forbidden_features(&[(
        "callerless_applied_push_writer",
        "persist_sync_applied_push_watermark_for_node",
    )]);
}

#[test]
fn legacy_migration_source_is_read_only_and_current_roots_are_refused_unchanged() {
    let legacy_methods = method_names(&public_impl_methods(
        &engine_root().join("src/database.rs"),
        "LegacyMigrationSource",
    ));
    assert_eq!(
        legacy_methods,
        BTreeSet::from([
            "close".to_string(),
            "copy_locked_source_to".to_string(),
            "keyless_table_rows".to_string(),
            "migrate_in_place".to_string(),
        ]),
        "the validated legacy source may expose only its fixed read-only snapshot, its untouched \
         backup copy, its close operation, and the one door that performs a migration whole -- \
         never the durability-mutating operations that door alone may drive, and never a handle \
         onto the store it is building"
    );

    let dir = tempfile::tempdir().expect("current-format tempdir");
    let path = dir.path().join("current.db");
    let db = Database::open(&path).expect("create current-format database");
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT)",
        &Default::default(),
    )
    .expect("seed current-format schema");
    db.close().expect("close current-format database");
    let before = std::fs::read(&path).expect("read current-format bytes before refusal");

    let err = match Database::open_legacy_for_migration(&path) {
        Ok(_) => panic!("a current-format root must not become a privileged migration source"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("already current-format"),
        "the typed migration boundary must explain the current-format refusal: {err}"
    );
    assert_eq!(
        std::fs::read(&path).expect("read current-format bytes after refusal"),
        before,
        "validating a rejected current-format source must be read-only"
    );
}

#[test]
fn visibility_changing_macro_definitions_match_the_audited_contract() {
    const TEST_SEAM_PUBLIC: &str = r#"#[cfg(feature = "test-seams")]
macro_rules! sync_test_seam {
    ($(#[$attr:meta])* fn $($item:tt)*) => {
        $(#[$attr])*
        pub fn $($item)*
    };
}"#;
    const TEST_SEAM_CRATE_PRIVATE: &str = r#"#[cfg(not(feature = "test-seams"))]
macro_rules! sync_test_seam {
    ($(#[$attr:meta])* fn $($item:tt)*) => {
        $(#[$attr])*
        pub(crate) fn $($item)*
    };
}"#;
    const IROH_GATE: &str = r#"macro_rules! peer_endpoint_available {
    ($item:item) => {
        #[cfg(feature = "iroh")]
        $item
    };
}"#;

    let database = std::fs::read_to_string(engine_root().join("src/database.rs"))
        .expect("read Database macro definitions");
    assert_eq!(
        database.matches("macro_rules! sync_test_seam").count(),
        2,
        "the test-seam visibility macro definition set changed"
    );
    assert_eq!(database.matches(TEST_SEAM_PUBLIC).count(), 1);
    assert_eq!(database.matches(TEST_SEAM_CRATE_PRIVATE).count(), 1);

    let transport = std::fs::read_to_string(engine_root().join("src/transport/mod.rs"))
        .expect("read transport macro definitions");
    assert_eq!(
        transport
            .matches("macro_rules! peer_endpoint_available")
            .count(),
        1,
        "the Iroh item gate definition set changed"
    );
    assert_eq!(transport.matches(IROH_GATE).count(), 1);
}

fn public_impl_methods(path: &Path, target: &str) -> Vec<ImplItemFn> {
    let source = std::fs::read_to_string(path).expect("read public surface");
    let file = syn::parse_file(&source).expect("parse public surface");
    file.items
        .into_iter()
        .filter_map(|item| match item {
            Item::Impl(item) => Some(item),
            _ => None,
        })
        .filter(|item| !item.attrs.iter().any(cfg_is_test_only))
        .filter(|item| {
            matches!(
                item.self_ty.as_ref(),
                Type::Path(path) if path.path.segments.last().is_some_and(|segment| segment.ident == target)
            )
        })
        .flat_map(|item| item.items)
        .filter_map(|item| match item {
            ImplItem::Fn(method) => Some(method),
            ImplItem::Macro(item) => {
                let path = item
                    .mac
                    .path
                    .segments
                    .iter()
                    .map(|segment| segment.ident.to_string())
                    .collect::<Vec<_>>()
                    .join("::");
                match path.as_str() {
                    // This macro only adds `#[cfg(feature = "iroh")]`; the
                    // wrapped method's visibility is therefore authoritative.
                    "crate::transport::peer_endpoint_available" => Some(
                        syn::parse2::<ImplItemFn>(item.mac.tokens).expect(
                            "peer_endpoint_available must wrap exactly one auditable method",
                        ),
                    ),
                    // In ordinary builds this macro deliberately changes the
                    // wrapped private `fn` to `pub(crate) fn`; neither form is
                    // a downstream public door. Parsing still proves the macro
                    // contains only audited methods rather than hiding arbitrary
                    // impl items. Some invocations deliberately carry private
                    // helper methods after the first visibility-injected method.
                    "sync_test_seam" => {
                        let tokens = item.mac.tokens;
                        let wrapper = format!("impl __PublicSurfaceAudit {{ {tokens} }}");
                        let methods = syn::parse_str::<ItemImpl>(&wrapper)
                            .unwrap_or_else(|err| {
                                panic!("sync_test_seam must contain auditable impl items; {err}")
                            })
                            .items;
                        assert!(!methods.is_empty(), "sync_test_seam must not be empty");
                        for method in methods {
                            let ImplItem::Fn(method) = method else {
                                panic!(
                                    "sync_test_seam may contain methods only; another impl item could hide a public {target} door"
                                )
                            };
                            assert!(
                                !matches!(method.vis, Visibility::Public(_)),
                                "sync_test_seam input must not declare its own public {target} method"
                            );
                        }
                        None
                    }
                    _ => panic!("unaudited impl-item macro `{path}` can hide a public {target} method"),
                }
            }
            _ => None,
        })
        .filter(|method| {
            matches!(method.vis, Visibility::Public(_))
                && !method.attrs.iter().any(cfg_is_test_only)
        })
        .collect()
}

fn cfg_is_test_only(attribute: &syn::Attribute) -> bool {
    let Meta::List(list) = &attribute.meta else {
        return false;
    };
    attribute.path().is_ident("cfg")
        && list
            .parse_args::<Meta>()
            .is_ok_and(|condition| cfg_condition_is_test_only(&condition))
}

fn cfg_condition_is_test_only(condition: &Meta) -> bool {
    match condition {
        Meta::Path(path) => path.is_ident("test"),
        Meta::NameValue(value) => {
            value.path.is_ident("feature")
                && matches!(&value.value, syn::Expr::Lit(literal) if matches!(&literal.lit, syn::Lit::Str(name) if matches!(name.value().as_str(), "test-seams" | "production-smoke-driver")))
        }
        Meta::List(list) if list.path.is_ident("any") || list.path.is_ident("all") => {
            let branches = list
                .parse_args_with(Punctuated::<Meta, syn::Token![,]>::parse_terminated)
                .expect("parse cfg branches");
            if list.path.is_ident("any") {
                branches.iter().all(cfg_condition_is_test_only)
            } else {
                branches.iter().any(cfg_condition_is_test_only)
            }
        }
        Meta::List(_) => false,
    }
}

fn database_public_methods(engine: &Path) -> Vec<ImplItemFn> {
    let mut paths = vec![engine.join("src/database.rs")];
    collect_rust_files(&engine.join("src/database"), &mut paths);
    paths
        .iter()
        .flat_map(|path| public_impl_methods(path, "Database"))
        .collect()
}

fn collect_rust_files(directory: &Path, paths: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(directory).expect("read Database implementation directory") {
        let path = entry.expect("Database implementation entry").path();
        if path.is_dir() {
            collect_rust_files(&path, paths);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            paths.push(path);
        }
    }
}

fn frozen_names(source: &str) -> BTreeSet<String> {
    source
        .lines()
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .collect()
}

fn method_names(methods: &[ImplItemFn]) -> BTreeSet<String> {
    methods
        .iter()
        .map(|method| method.sig.ident.to_string())
        .collect()
}

fn is_test_door(name: &str) -> bool {
    name.starts_with("__") || name.ends_with("_for_test")
}

fn method_mentions(method: &ImplItemFn, wanted: &str) -> bool {
    struct Mention<'a> {
        wanted: &'a str,
        found: bool,
    }
    impl Visit<'_> for Mention<'_> {
        fn visit_path(&mut self, path: &syn::Path) {
            self.found |= path
                .segments
                .iter()
                .any(|segment| segment.ident == self.wanted);
            syn::visit::visit_path(self, path);
        }
    }
    let mut visitor = Mention {
        wanted,
        found: false,
    };
    visitor.visit_signature(&method.sig);
    visitor.found
}

#[test]
fn durable_public_mutation_surface_matches_the_frozen_allowlist() {
    let engine = engine_root();
    let root = engine.ancestors().nth(2).expect("contextdb root");
    let database = database_public_methods(&engine);
    let client = public_impl_methods(&engine.join("src/sync_client.rs"), "SyncClient");
    let server = public_impl_methods(&engine.join("src/sync_server.rs"), "SyncServer");
    let engine_exports =
        std::fs::read_to_string(engine_root().join("src/lib.rs")).expect("read engine reexports");
    let server_exports = std::fs::read_to_string(root.join("crates/contextdb-server/src/lib.rs"))
        .expect("read server reexports");

    let database_names = method_names(&database);
    let client_names = method_names(&client);
    let server_names = method_names(&server);
    let accountant = public_impl_methods(
        &root.join("crates/contextdb-core/src/memory.rs"),
        "MemoryAccountant",
    );
    let accountant_names = method_names(&accountant);
    let accountant_reachability = database
        .iter()
        .filter(|method| !is_test_door(&method.sig.ident.to_string()))
        .filter(|method| method_mentions(method, "MemoryAccountant"))
        .map(|method| method.sig.ident.to_string())
        .collect::<BTreeSet<_>>();
    let policy_reachability = database
        .iter()
        .filter(|method| !is_test_door(&method.sig.ident.to_string()))
        .filter(|method| {
            method_mentions(method, "ConflictPolicies") || method_mentions(method, "ConflictPolicy")
        })
        .map(|method| method.sig.ident.to_string())
        .collect::<BTreeSet<_>>();
    let client_sensitive_reachability = client
        .iter()
        .filter(|method| !is_test_door(&method.sig.ident.to_string()))
        .filter(|method| {
            method_mentions(method, "ConflictPolicies")
                || method_mentions(method, "ConflictPolicy")
                || method_mentions(method, "SyncDirection")
                || method_mentions(method, "ServerTransport")
        })
        .map(|method| method.sig.ident.to_string())
        .collect::<BTreeSet<_>>();
    let safe_server_new = server
        .iter()
        .find(|method| method.sig.ident == "new")
        .expect("safe SyncServer::new constructor remains public");
    assert_eq!(
        database_names,
        frozen_names(include_str!("fixtures/public_api_database_names.txt")),
        "Database public impl-name inventory drifted from the frozen durable surface"
    );
    assert_eq!(
        client_names,
        frozen_names(include_str!("fixtures/public_api_client_names.txt")),
        "SyncClient public impl-name inventory drifted from the frozen typed surface"
    );
    assert_eq!(
        server_names,
        frozen_names(include_str!("fixtures/public_api_server_names.txt")),
        "SyncServer public impl-name inventory drifted from the frozen typed surface"
    );
    assert_eq!(
        accountant_names,
        frozen_names(include_str!(
            "fixtures/public_api_memory_accountant_names.txt"
        )),
        "MemoryAccountant public impl-name inventory drifted from its read-only surface"
    );
    assert!(
        accountant_reachability.is_empty(),
        "Database methods still expose raw mutable accountant types: {accountant_reachability:?}"
    );
    assert!(
        policy_reachability.is_empty(),
        "Database methods still expose role-relative policy types: {policy_reachability:?}"
    );
    assert!(
        client_sensitive_reachability.is_empty(),
        "SyncClient methods still expose policy, direction, or arbitrary transport types: {client_sensitive_reachability:?}"
    );
    for (surface, names) in [
        ("Database", &database_names),
        ("SyncClient", &client_names),
        ("SyncServer", &server_names),
        ("MemoryAccountant", &accountant_names),
    ] {
        let test_doors = names
            .iter()
            .filter(|name| is_test_door(name))
            .collect::<Vec<_>>();
        assert!(
            test_doors.is_empty(),
            "{surface} still exposes test/debug injection doors in the normal build: {test_doors:?}"
        );
    }
    assert!(
        !method_mentions(safe_server_new, "ConflictPolicies")
            && !method_mentions(safe_server_new, "ConflictPolicy")
            && !method_mentions(safe_server_new, "ServerTransport")
            && safe_server_new.sig.inputs.len() == 3,
        "SyncServer::new must remain the typed production constructor without policy or arbitrary transport arguments"
    );
    for forbidden_reexport in [
        "pub use sync_types::*",
        "pub use sync_types::{ConflictPolicy",
        "pub use sync_types::{ConflictPolicies",
    ] {
        assert!(
            !engine_exports.contains(forbidden_reexport)
                && !server_exports.contains(forbidden_reexport),
            "role-relative arbitration types must not remain reachable through public reexports: {forbidden_reexport}"
        );
    }
}
