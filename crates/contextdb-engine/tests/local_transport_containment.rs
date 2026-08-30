use contextdb_engine::local_transport::{LocalPlatformAvailability, local_platform_availability};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use syn::visit::{self, Visit};

#[cfg(windows)]
const WINDOWS_LOCAL_CHANNEL_AVAILABILITY: LocalPlatformAvailability = local_platform_availability();
#[cfg(windows)]
const _: () = match WINDOWS_LOCAL_CHANNEL_AVAILABILITY {
    LocalPlatformAvailability::PlatformUnsupported => (),
    LocalPlatformAvailability::Available => panic!("Windows local channels must stay unsupported"),
};

fn engine_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn collect_rust_files(root: &Path, files: &mut Vec<PathBuf>) {
    let mut entries: Vec<_> = fs::read_dir(root)
        .expect("read local transport source directory")
        .map(|entry| entry.expect("source directory entry").path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            collect_rust_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

fn source_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_rust_files(root, &mut files);
    let mut discovered: BTreeSet<_> = files.into_iter().collect();
    let mut pending: Vec<_> = discovered.iter().cloned().collect();
    while let Some(source) = pending.pop() {
        let contents = fs::read_to_string(&source).expect("read containment source");
        let Ok(syntax) = syn::parse_file(&contents) else {
            continue;
        };
        for referenced in referenced_source_files(&source, &syntax) {
            if referenced.is_file() && discovered.insert(referenced.clone()) {
                pending.push(referenced);
            }
        }
    }
    discovered.into_iter().collect()
}

#[derive(Default)]
struct StructuralDependencies {
    paths: BTreeSet<Vec<String>>,
    aliases: BTreeMap<String, Vec<String>>,
    modules: BTreeSet<String>,
}

fn collect_macro_tokens(
    tokens: proc_macro2::TokenStream,
    dependencies: &mut StructuralDependencies,
) {
    for token in tokens {
        match token {
            proc_macro2::TokenTree::Ident(ident) => {
                dependencies.paths.insert(vec![ident.to_string()]);
            }
            proc_macro2::TokenTree::Group(group) => {
                collect_macro_tokens(group.stream(), dependencies);
            }
            proc_macro2::TokenTree::Punct(_) | proc_macro2::TokenTree::Literal(_) => {}
        }
    }
}

fn module_path_attribute(module: &syn::ItemMod) -> Option<PathBuf> {
    module.attrs.iter().find_map(|attribute| {
        if !attribute.path().is_ident("path") {
            return None;
        }
        let syn::Meta::NameValue(value) = &attribute.meta else {
            return None;
        };
        let syn::Expr::Lit(expression) = &value.value else {
            return None;
        };
        let syn::Lit::Str(path) = &expression.lit else {
            return None;
        };
        Some(PathBuf::from(path.value()))
    })
}

fn macro_file_argument(item: &syn::Macro, names: &[&str]) -> Option<PathBuf> {
    let name = item.path.segments.last()?.ident.to_string();
    if !names.contains(&name.as_str()) {
        return None;
    }
    syn::parse2::<syn::LitStr>(item.tokens.clone())
        .ok()
        .map(|literal| PathBuf::from(literal.value()))
}

struct ReferencedSourceFiles<'a> {
    source: &'a Path,
    referenced: BTreeSet<PathBuf>,
}

impl ReferencedSourceFiles<'_> {
    fn resolve(&self, relative: PathBuf) -> PathBuf {
        self.source
            .parent()
            .expect("source file has a parent")
            .join(relative)
    }

    fn module_directory(&self) -> PathBuf {
        let parent = self.source.parent().expect("source file has a parent");
        if self.source.file_name().is_some_and(|name| name == "mod.rs") {
            parent.to_path_buf()
        } else {
            parent.join(
                self.source
                    .file_stem()
                    .expect("Rust source has a file stem"),
            )
        }
    }
}

impl<'ast> Visit<'ast> for ReferencedSourceFiles<'_> {
    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        if item.content.is_none() {
            if let Some(path) = module_path_attribute(item) {
                self.referenced.insert(self.resolve(path));
            } else {
                let name = item.ident.to_string();
                self.referenced
                    .insert(self.module_directory().join(format!("{name}.rs")));
                self.referenced
                    .insert(self.module_directory().join(name).join("mod.rs"));
            }
        }
        visit::visit_item_mod(self, item);
    }

    fn visit_macro(&mut self, item: &'ast syn::Macro) {
        let name = item
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string());
        if name
            .as_deref()
            .is_some_and(|name| ["include", "include_str"].contains(&name))
        {
            let path = macro_file_argument(item, &["include", "include_str"])
                .expect("containment requires a statically resolvable include path");
            self.referenced.insert(self.resolve(path));
        }
        visit::visit_macro(self, item);
    }
}

fn referenced_source_files(source: &Path, syntax: &syn::File) -> BTreeSet<PathBuf> {
    let mut references = ReferencedSourceFiles {
        source,
        referenced: BTreeSet::new(),
    };
    references.visit_file(syntax);
    references.referenced
}

fn flatten_use_tree(
    tree: &syn::UseTree,
    prefix: &mut Vec<String>,
    dependencies: &mut StructuralDependencies,
) {
    match tree {
        syn::UseTree::Path(path) => {
            prefix.push(path.ident.to_string());
            flatten_use_tree(&path.tree, prefix, dependencies);
            prefix.pop();
        }
        syn::UseTree::Name(name) => {
            let mut imported = prefix.clone();
            imported.push(name.ident.to_string());
            dependencies.paths.insert(imported.clone());
            dependencies
                .aliases
                .insert(name.ident.to_string(), imported);
        }
        syn::UseTree::Rename(rename) => {
            let mut imported = prefix.clone();
            imported.push(rename.ident.to_string());
            dependencies.paths.insert(imported.clone());
            dependencies
                .aliases
                .insert(rename.rename.to_string(), imported);
        }
        syn::UseTree::Glob(_) => {
            dependencies.paths.insert(prefix.clone());
        }
        syn::UseTree::Group(group) => {
            for item in &group.items {
                flatten_use_tree(item, prefix, dependencies);
            }
        }
    }
}

impl<'ast> Visit<'ast> for StructuralDependencies {
    fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
        flatten_use_tree(&item.tree, &mut Vec::new(), self);
        visit::visit_item_use(self, item);
    }

    fn visit_item_extern_crate(&mut self, item: &'ast syn::ItemExternCrate) {
        let original = vec![item.ident.to_string()];
        self.paths.insert(original.clone());
        let local_name = item
            .rename
            .as_ref()
            .map_or_else(|| item.ident.to_string(), |(_, rename)| rename.to_string());
        self.aliases.insert(local_name, original);
        visit::visit_item_extern_crate(self, item);
    }

    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        self.modules.insert(item.ident.to_string());
        visit::visit_item_mod(self, item);
    }

    fn visit_path(&mut self, path: &'ast syn::Path) {
        self.paths.insert(
            path.segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect(),
        );
        visit::visit_path(self, path);
    }

    fn visit_macro(&mut self, item: &'ast syn::Macro) {
        collect_macro_tokens(item.tokens.clone(), self);
        visit::visit_macro(self, item);
    }
}

fn structural_dependencies(source: &Path) -> StructuralDependencies {
    structural_dependencies_with_aliases(source, &BTreeMap::new())
}

fn structural_dependencies_with_aliases(
    source: &Path,
    cargo_aliases: &BTreeMap<String, Vec<String>>,
) -> StructuralDependencies {
    let contents = fs::read_to_string(source).expect("read transport source");
    let mut dependencies = StructuralDependencies {
        aliases: cargo_aliases.clone(),
        ..StructuralDependencies::default()
    };
    if let Ok(syntax) = syn::parse_file(&contents) {
        dependencies.visit_file(&syntax);
    } else if let Ok(tokens) = contents.parse::<proc_macro2::TokenStream>() {
        collect_macro_tokens(tokens, &mut dependencies);
    }
    dependencies
}

fn forbidden_paths(dependencies: &StructuralDependencies) -> Vec<Vec<String>> {
    dependencies
        .paths
        .iter()
        .map(|path| resolve_aliases(path, &dependencies.aliases))
        .filter(|path| forbidden_dependency(path))
        .collect()
}

fn resolve_aliases(path: &[String], aliases: &BTreeMap<String, Vec<String>>) -> Vec<String> {
    let mut resolved = path.to_vec();
    let mut seen = BTreeSet::new();
    while let Some(first) = resolved.first().cloned() {
        let Some(alias) = aliases.get(&first) else {
            break;
        };
        if !seen.insert(first) {
            break;
        }
        let mut expanded = alias.clone();
        expanded.extend_from_slice(&resolved[1..]);
        resolved = expanded;
    }
    resolved
}

fn forbidden_dependency(path: &[String]) -> bool {
    let forbidden_modules = [
        "companion",
        "companion_lock",
        "database",
        "persistence",
        "persistent_store",
        "remote_sync",
        "sync",
        "sync_client",
        "sync_server",
        "sync_types",
        "transport",
    ];
    let forbidden_crates = [
        "bao_tree",
        "contextdb_server",
        "futures_util",
        "iroh",
        "iroh_blobs",
        "irpc",
        "libc",
        "range_collections",
        "remote_sync",
        "remote_sync_client",
        "remote_sync_server",
        "tokio",
    ];
    let Some(first) = path.first().map(String::as_str) else {
        return false;
    };
    if forbidden_crates.contains(&first) || forbidden_modules.contains(&first) {
        return true;
    }
    if first == "crate" {
        return path
            .get(1)
            .is_some_and(|module| forbidden_modules.contains(&module.as_str()));
    }
    if first == "self" || first == "super" {
        return path
            .iter()
            .skip(1)
            .any(|segment| forbidden_modules.contains(&segment.as_str()));
    }
    false
}

fn rust_crate_name(package: &str) -> String {
    package.replace('-', "_")
}

fn collect_dependency_aliases(
    value: &toml::Value,
    in_dependency_table: bool,
    aliases: &mut BTreeMap<String, Vec<String>>,
) {
    let Some(table) = value.as_table() else {
        return;
    };
    if in_dependency_table {
        for (alias, specification) in table {
            let package = specification
                .as_table()
                .and_then(|fields| fields.get("package"))
                .and_then(toml::Value::as_str)
                .unwrap_or(alias);
            aliases.insert(rust_crate_name(alias), vec![rust_crate_name(package)]);
        }
        return;
    }
    for (key, nested) in table {
        collect_dependency_aliases(
            nested,
            matches!(
                key.as_str(),
                "dependencies" | "dev-dependencies" | "build-dependencies"
            ),
            aliases,
        );
    }
}

fn cargo_dependency_aliases(manifest: &toml::Value) -> BTreeMap<String, Vec<String>> {
    let mut aliases = BTreeMap::new();
    collect_dependency_aliases(manifest, false, &mut aliases);
    aliases
}

#[derive(Debug)]
struct CargoDependencyEntry {
    table_path: Vec<String>,
    alias: String,
    package: String,
    optional: bool,
}

fn collect_dependency_entries(
    value: &toml::Value,
    path: &mut Vec<String>,
    entries: &mut Vec<CargoDependencyEntry>,
) {
    let Some(table) = value.as_table() else {
        return;
    };
    if path.last().is_some_and(|name| {
        matches!(
            name.as_str(),
            "dependencies" | "dev-dependencies" | "build-dependencies"
        )
    }) {
        for (alias, specification) in table {
            let fields = specification.as_table();
            let package = fields
                .and_then(|fields| fields.get("package"))
                .and_then(toml::Value::as_str)
                .unwrap_or(alias);
            entries.push(CargoDependencyEntry {
                table_path: path.clone(),
                alias: rust_crate_name(alias),
                package: rust_crate_name(package),
                optional: fields
                    .and_then(|fields| fields.get("optional"))
                    .and_then(toml::Value::as_bool)
                    .unwrap_or(false),
            });
        }
        return;
    }
    for (key, nested) in table {
        path.push(key.clone());
        collect_dependency_entries(nested, path, entries);
        path.pop();
    }
}

fn cargo_dependency_entries(manifest: &toml::Value) -> Vec<CargoDependencyEntry> {
    let mut entries = Vec::new();
    collect_dependency_entries(manifest, &mut Vec::new(), &mut entries);
    entries
}

#[test]
fn local_channel_source_recursively_excludes_storage_companion_remote_and_async_dependencies() {
    let source_root = engine_root().join("src/local_transport");
    let manifest: toml::Value = fs::read_to_string(engine_root().join("Cargo.toml"))
        .expect("read engine manifest")
        .parse()
        .expect("parse engine manifest");
    let cargo_aliases = cargo_dependency_aliases(&manifest);
    let files = source_files(&source_root);
    let required: BTreeSet<_> = [
        "address.rs",
        "authentication.rs",
        "carrier.rs",
        "deadlines.rs",
        "framing.rs",
        "mod.rs",
        "runtime.rs",
        "stale.rs",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    let present: BTreeSet<_> = files
        .iter()
        .filter_map(|path| path.file_name().and_then(|name| name.to_str()))
        .map(str::to_owned)
        .collect();
    assert!(required.is_subset(&present));

    for source in files {
        let dependencies = structural_dependencies_with_aliases(&source, &cargo_aliases);
        for module in &dependencies.modules {
            assert!(
                !forbidden_dependency(std::slice::from_ref(module)),
                "{} declares forbidden nested module {module}",
                source.display()
            );
        }
        for path in &dependencies.paths {
            let resolved = resolve_aliases(path, &dependencies.aliases);
            assert!(
                !forbidden_dependency(&resolved),
                "{} structurally references forbidden path {}",
                source.display(),
                resolved.join("::")
            );
        }
    }
}

#[test]
fn owner_channel_routes_cannot_bypass_the_typed_protocol_boundary() {
    let manifest: toml::Value = fs::read_to_string(engine_root().join("Cargo.toml"))
        .expect("read engine manifest")
        .parse()
        .expect("parse engine manifest");
    let aliases = cargo_dependency_aliases(&manifest);
    let bypasses = [
        "decode_message_exact",
        "encode_message",
        "encode_payload_frame",
        "read_payload_with_admission",
        "receive_framed_ordinary_result",
    ];
    let required_boundary_calls = [
        "receive_message",
        "receive_response",
        "send_message",
        "write_request",
    ];
    let mut observed_boundary_calls = BTreeSet::new();

    for source in source_files(&engine_root().join("src/owner_read")) {
        let dependencies = structural_dependencies_with_aliases(&source, &aliases);
        for path in &dependencies.paths {
            let resolved = resolve_aliases(path, &dependencies.aliases);
            let Some(symbol) = resolved.last().map(String::as_str) else {
                continue;
            };
            assert!(
                !bypasses.contains(&symbol),
                "{} bypasses LocalProtocolBoundary through {}",
                source.display(),
                resolved.join("::")
            );
            if required_boundary_calls.contains(&symbol) {
                observed_boundary_calls.insert(symbol.to_owned());
            }
        }
    }
    assert_eq!(
        observed_boundary_calls,
        required_boundary_calls
            .into_iter()
            .map(str::to_owned)
            .collect(),
        "owner client and service must enter the carrier only through the typed boundary"
    );
}

fn assert_snippet_is_caught(source: &str) {
    let syntax = syn::parse_file(source).expect("parse adversarial containment source");
    let mut dependencies = StructuralDependencies::default();
    dependencies.visit_file(&syntax);
    assert!(
        !forbidden_paths(&dependencies).is_empty(),
        "containment missed adversarial source: {source}"
    );
}

#[test]
fn structural_containment_positive_controls_cover_every_supported_rust_indirection() {
    assert_snippet_is_caught("mod nested { use crate::persistence::Store; }");
    assert_snippet_is_caught("use crate::persistence as p; fn f() { p::open(); }");
    assert_snippet_is_caught("extern crate tokio as runtime; fn f() { runtime::spawn(); }");
    assert_snippet_is_caught(
        "use crate::{local_transport, persistence::{Store as DurableStore}}; fn f(_: DurableStore) {}",
    );
    assert_snippet_is_caught("fn f() { route!(tokio::spawn(async {})); }");
    assert_snippet_is_caught("fn f() { unsafe { libc::access(core::ptr::null(), 0); } }");

    let aliased_manifest: toml::Value = r#"
        [target.'cfg(unix)'.dependencies]
        async_runtime = { package = "tokio", version = "1" }
        [target.'cfg(unix)'.build-dependencies]
        durable = { package = "remote-sync", version = "1" }
    "#
    .parse()
    .expect("parse adversarial dependency aliases");
    let aliases = cargo_dependency_aliases(&aliased_manifest);
    for source in [
        "fn f() { async_runtime::spawn(async {}); }",
        "fn f() { durable::connect(); }",
    ] {
        let syntax = syn::parse_file(source).expect("parse aliased adversarial source");
        let mut dependencies = StructuralDependencies {
            aliases: aliases.clone(),
            ..StructuralDependencies::default()
        };
        dependencies.visit_file(&syntax);
        assert!(
            !forbidden_paths(&dependencies).is_empty(),
            "Cargo package alias escaped structural containment: {source}"
        );
    }

    let temporary = tempfile::tempdir().expect("temporary containment graph");
    let root = temporary.path().join("root");
    fs::create_dir(&root).expect("create graph root");

    let path_root = root.join("path_root.rs");
    fs::write(&path_root, "#[path = \"../path_hidden.rs\"] mod hidden;")
        .expect("write path control root");
    fs::write(
        temporary.path().join("path_hidden.rs"),
        "use crate::persistence::Store;",
    )
    .expect("write path control target");

    let include_root = root.join("include_root.rs");
    fs::write(&include_root, "include!(\"../included.rs\");").expect("write include control root");
    fs::write(temporary.path().join("included.rs"), "extern crate tokio;")
        .expect("write include control target");

    let include_str_root = root.join("include_str_root.rs");
    fs::write(
        &include_str_root,
        "const SOURCE: &str = include_str!(\"../included_tokens.txt\");",
    )
    .expect("write include_str control root");
    fs::write(
        temporary.path().join("included_tokens.txt"),
        "route!(crate::persistence::open());",
    )
    .expect("write include_str control target");

    for (entry, target_name) in [
        (path_root, "path_hidden.rs"),
        (include_root, "included.rs"),
        (include_str_root, "included_tokens.txt"),
    ] {
        let graph_root = tempfile::tempdir().expect("isolated graph entry");
        let scan_root = graph_root.path().join("src");
        fs::create_dir(&scan_root).expect("create isolated source root");
        let graph_entry = scan_root.join("mod.rs");
        let entry_text = fs::read_to_string(&entry).expect("read graph entry");
        fs::write(&graph_entry, entry_text).expect("write isolated graph entry");
        fs::copy(
            temporary.path().join(target_name),
            graph_root.path().join(target_name),
        )
        .expect("copy referenced graph target");
        let files = source_files(&scan_root);
        assert!(files.len() >= 2, "reference was not recursively resolved");
        assert!(
            files
                .iter()
                .map(|source| structural_dependencies(source))
                .any(|dependencies| !forbidden_paths(&dependencies).is_empty())
        );
    }
}

fn use_tree_contains_local_transport(tree: &syn::UseTree) -> bool {
    match tree {
        syn::UseTree::Path(path) => {
            path.ident == "local_transport" || use_tree_contains_local_transport(&path.tree)
        }
        syn::UseTree::Name(name) => name.ident == "local_transport",
        syn::UseTree::Rename(rename) => rename.ident == "local_transport",
        syn::UseTree::Group(group) => group.items.iter().any(use_tree_contains_local_transport),
        syn::UseTree::Glob(_) => false,
    }
}

#[test]
fn local_channel_test_seam_is_not_reexported_as_engine_product_api() {
    let library = syn::parse_file(
        &fs::read_to_string(engine_root().join("src/lib.rs")).expect("read engine library"),
    )
    .expect("parse engine library");
    let local_transport_modules: Vec<_> = library
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Mod(module) if module.ident == "local_transport" => Some(module),
            _ => None,
        })
        .collect();
    assert_eq!(local_transport_modules.len(), 2);
    assert!(
        local_transport_modules
            .iter()
            .any(|module| matches!(&module.vis, syn::Visibility::Inherited))
    );
    assert!(
        local_transport_modules
            .iter()
            .any(|module| matches!(&module.vis, syn::Visibility::Public(_)))
    );

    let public_local_transport_reexport = library.items.iter().any(|item| match item {
        syn::Item::Use(import) => {
            matches!(&import.vis, syn::Visibility::Public(_))
                && use_tree_contains_local_transport(&import.tree)
        }
        _ => false,
    });
    assert!(!public_local_transport_reexport);
}

fn table<'a>(value: &'a toml::Value, key: &str) -> &'a toml::value::Table {
    value
        .get(key)
        .and_then(toml::Value::as_table)
        .unwrap_or_else(|| panic!("manifest table {key} is required"))
}

#[test]
fn manifest_keeps_tokio_optional_and_unix_credentials_out_of_windows_dependencies() {
    let manifest: toml::Value = fs::read_to_string(engine_root().join("Cargo.toml"))
        .expect("read engine manifest")
        .parse()
        .expect("parse engine manifest structurally");
    let dependencies = table(&manifest, "dependencies");
    let tokio = dependencies
        .get("tokio")
        .and_then(toml::Value::as_table)
        .expect("Tokio dependency is represented as a table");
    assert_eq!(
        tokio.get("optional").and_then(toml::Value::as_bool),
        Some(true)
    );
    assert!(!dependencies.contains_key("nix"));

    let entries = cargo_dependency_entries(&manifest);
    for entry in &entries {
        if entry.package == "tokio"
            && !entry
                .table_path
                .iter()
                .any(|part| part == "dev-dependencies")
        {
            assert!(
                entry.optional,
                "Tokio package alias {} in {} must remain optional",
                entry.alias,
                entry.table_path.join(".")
            );
        }
        if entry.package == "nix" {
            assert_eq!(
                entry.table_path,
                vec![
                    "target".to_owned(),
                    "cfg(unix)".to_owned(),
                    "dependencies".to_owned(),
                ],
                "every nix package alias must remain Unix-only"
            );
        }
    }
    let aliases = cargo_dependency_aliases(&manifest);
    assert_eq!(aliases.get("nix"), Some(&vec!["nix".to_owned()]));

    let default_features = manifest
        .get("features")
        .and_then(|features| features.get("default"))
        .and_then(toml::Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        default_features.is_empty(),
        "default features must activate no runtime"
    );

    let targets = table(&manifest, "target");
    let unix = table(
        targets
            .get("cfg(unix)")
            .expect("Unix target dependency table exists"),
        "dependencies",
    );
    let nix = unix
        .get("nix")
        .and_then(toml::Value::as_table)
        .expect("nix is a structured Unix-only dependency");
    let nix_features: BTreeSet<_> = nix
        .get("features")
        .and_then(toml::Value::as_array)
        .expect("nix feature list")
        .iter()
        .filter_map(toml::Value::as_str)
        .collect();
    // `poll` is the audited safe Unix facility for cancellable readiness.
    assert_eq!(
        nix_features,
        BTreeSet::from(["fs", "mount", "poll", "socket", "user"])
    );
    for (target, target_value) in targets {
        if target.contains("windows") {
            assert!(
                !table(target_value, "dependencies").contains_key("nix"),
                "Windows target {target} must not activate Unix credentials"
            );
        }
    }
}

#[test]
fn windows_build_contract_is_explicitly_unsupported() {
    #[cfg(windows)]
    {
        assert_eq!(
            local_platform_availability(),
            LocalPlatformAvailability::PlatformUnsupported
        );
        let credential_function: fn(&()) -> Result<_, _> =
            contextdb_engine::local_transport::peer_user_from_stream::<()>;
        assert!(credential_function(&()).is_err());
    }

    #[cfg(not(windows))]
    assert!(matches!(
        local_platform_availability(),
        LocalPlatformAvailability::Available | LocalPlatformAvailability::PlatformUnsupported
    ));
}
