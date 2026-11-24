use super::*;
use contextdb::{Create, Value, serde_json};
use serde::Deserialize;
use std::collections::HashMap;
#[derive(Deserialize)]
struct Key {
    column: String,
    value: Value,
    #[serde(default)]
    rest: Vec<(String, Value)>,
}
/// One quoted identifier, folded to lower case as a bare name is, so a name never reads as SQL.
fn ident(name: &str) -> String {
    format!("\"{}\"", name.to_lowercase().replace('"', "\"\""))
}

pub fn run(command: &Command, args: &Args, output: Output) -> contextdb::Result<bool> {
    let reader = |p: &Path| ReadSession::open(p, args.limits());
    let printed = match command {
        Command::Snapshot {
            command: Snapshot::Export { path, artifact },
        } => output.status("snapshot", reader(path)?.export_snapshot(artifact)?),
        Command::Inspect {
            command:
                Inspect::Key {
                    path,
                    table,
                    key_json,
                    column,
                },
        } => {
            let key: Key = serde_json::from_str(key_json)
                .map_err(|e| contextdb::Error::ParseError(e.to_string()))?;
            let mut params = HashMap::new();
            let mut predicates = Vec::new();
            // Each name folded once, as a bare name is, so the row read and its history agree.
            let table = table.to_lowercase();
            let given: Vec<_> = std::iter::once((key.column, key.value))
                .chain(key.rest)
                .map(|(name, value)| (name.to_lowercase(), value))
                .collect();
            for (i, (name, value)) in given.iter().enumerate() {
                params.insert(format!("key{i}"), value.clone());
                predicates.push(format!("{} = $key{i}", ident(name)));
            }
            let fields = Some(column.join(", "))
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "*".into());
            let session = reader(path)?.into_one_moment()?;
            let rows = session.execute(
                &format!(
                    "SELECT {fields} FROM {} WHERE {}",
                    ident(&table),
                    predicates.join(" AND ")
                ),
                &params,
            )?;
            let meta = session.table_meta(&table)?;
            let pk = meta.indexes.iter().find(|i| i.name.starts_with("__pk_"));
            let named = |(column, _): &(String, _)| given.iter().find(|g| g.0 == *column);
            let pk = pk.into_iter().flat_map(|pk| pk.columns.iter().map(named));
            let key: Vec<Value> = pk.map(|g| g.map_or(Value::Null, |g| g.1.clone())).collect();
            let versions = session.row_versions(&table, &key, 128)?;
            let more = Some(versions.more);
            output
                .result(&rows)
                .and(output.report("row_versions", &versions.rows, more))
        }
        Command::Inspect { command } => {
            output.refusal(
                "not_supported",
                match command {
                    Inspect::Blob { .. } => "inspect blob: media arrives in step 8",
                    _ => "inspect sync-apply-state: sync arrives in step 6",
                },
            );
            return Ok(false);
        }
        Command::Diagnose { path } => {
            let answer =
                output.result(&reader(path)?.execute("SHOW CONTENT_SIZE", &HashMap::new())?);
            output.notice(
                "diagnose",
                &format!("{} reads cleanly; nothing to correct", path.display()),
            );
            answer
        }
        Command::Purge { path, .. } | Command::Reset { path, .. } => {
            let mut options = DatabaseOpenOptions::default();
            options.maintenance = Maintenance::Caller;
            options.memory_limit = args.memory_limit;
            options.disk_limit = args.disk_limit.map(|n| n as u64);
            options.create = if matches!(command, Command::Reset { .. }) {
                Create::Fresh
            } else {
                Create::Never
            };
            let db = open(path, options, output)?;
            let answer = if let Command::Purge { table, .. } = command {
                output
                    .result(&db.execute(&format!("PURGE FROM {}", ident(table)), &HashMap::new())?)
            } else {
                output.notice(
                    "reset",
                    &format!("reset: a fresh, empty store at {}", path.display()),
                );
                Ok(())
            };
            db.close()?;
            answer
        }
    };
    Ok(printed.is_ok())
}
