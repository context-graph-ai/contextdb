//! The declarations: CREATE TABLE with every clause of a column and of a table, and CREATE INDEX.

use super::{
    Pair, Rule, child, direction, expr, first_ident, has, ident, idents, number, refuse,
    search_mode, signed, string, two_idents,
};
use crate::sql::ast::*;
use crate::types::Result;

/// A clause written twice is refused.
fn once(written: bool, clause: &str) -> Result<()> {
    match written {
        true => Err(refuse(format!("duplicate {clause} clause"))),
        false => Ok(()),
    }
}

/// A list that names a column twice is refused.
fn distinct(what: &str, columns: &[String]) -> Result<()> {
    let twice = (columns.iter().enumerate())
        .find_map(|(at, column)| columns[..at].contains(column).then_some(column));
    match twice {
        Some(twice) => Err(refuse(format!("{what} names '{twice}' twice"))),
        None => Ok(()),
    }
}

/// The first part of what the grammar read as `pair`.
fn first(pair: Pair<'_>) -> Pair<'_> {
    pair.into_inner()
        .next()
        .expect("the grammar reads a part here")
}

/// The first word of a clause, in capitals.
fn keyword(pair: &Pair<'_>) -> String {
    let letters = pair.as_str().chars().take_while(char::is_ascii_alphabetic);
    letters.map(|letter| letter.to_ascii_uppercase()).collect()
}

/// Every quoted text directly inside `pair`, in order.
fn strings(pair: Pair<'_>) -> Vec<String> {
    let texts = pair
        .into_inner()
        .filter(|part| part.as_rule() == Rule::string);
    texts.map(|text| string(&text)).collect()
}

/// Every state and edge type directly inside `pair`, in order.
fn values(pair: Pair<'_>) -> Vec<String> {
    let values = pair
        .into_inner()
        .filter(|part| part.as_rule() == Rule::as_written);
    values.map(|value| ident(&value)).collect()
}

fn indexed_column(pair: Pair<'_>) -> (String, SortDirection) {
    let mut parts = pair.into_inner();
    let name = ident(&parts.next().expect("a column"));
    (name, direction(parts.next()))
}

/// The rule of an `ON STATE` and of a `PROPAGATE ON EDGE`: the last two values written are the
/// state that starts it and the state it sets.
fn propagation(pair: Pair<'_>) -> Result<Propagation> {
    let mut states = values(pair.clone());
    let set = states.pop().expect("a state to set");
    let on_state = states.pop().expect("a state to start from");
    Ok(Propagation {
        on_state,
        set,
        max_depth: child(pair.clone(), Rule::max_depth)
            .map(|depth| number(&first(depth), "MAX DEPTH"))
            .transpose()?,
        abort_on_failure: has(&pair, Rule::abort_on_failure),
    })
}

fn kept_counts(pair: Pair<'_>) -> Result<KeptCounts> {
    let mut lists = Vec::new();
    for part in pair.into_inner() {
        if part.as_rule() == Rule::count_word {
            return match keyword(&part).as_str() {
                "ALL" => Ok(KeptCounts::AllPrefixes),
                "NONE" => Ok(KeptCounts::None),
                _ => Err(refuse(
                    "MAINTAIN COUNT DEFAULT takes a declared list away, and a new table has none",
                )),
            };
        }
        // A list or a column written twice is read as written; the table's check refuses it.
        lists.push(part.into_inner().map(indexed_column).collect());
    }
    Ok(KeptCounts::Lists(lists))
}

pub(super) fn create_table(pair: Pair<'_>) -> Result<CreateTable> {
    let mut table = CreateTable {
        name: first_ident(pair.clone()),
        columns: Vec::new(),
        unique_constraints: Vec::new(),
        primary_key: Vec::new(),
        foreign_keys: Vec::new(),
        if_not_exists: has(&pair, Rule::if_not_exists),
        options: TableOptions::default(),
    };
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::column_def => table.columns.push(column_def(part)?),
            Rule::unique_table_constraint => {
                let columns = idents(part);
                distinct("UNIQUE", &columns)?;
                table.unique_constraints.push(columns);
            }
            Rule::primary_key_columns => {
                once(!table.primary_key.is_empty(), "PRIMARY KEY")?;
                table.primary_key = idents(first(part));
                distinct("PRIMARY KEY", &table.primary_key)?;
            }
            Rule::foreign_key => {
                let follows_parent_purge = has(&part, Rule::follows_parent_purge);
                let parent = first_ident(part.clone());
                let mut lists = part
                    .into_inner()
                    .filter(|list| list.as_rule() == Rule::columns);
                let [own, columns] = [(); 2].map(|()| idents(lists.next().expect("a list")));
                distinct("FOREIGN KEY", &own)?;
                if own.len() != columns.len() {
                    return Err(refuse(format!(
                        "FOREIGN KEY names {} columns and REFERENCES {}",
                        own.len(),
                        columns.len()
                    )));
                }
                let reference = Reference {
                    follows_parent_purge,
                    table: parent,
                    columns,
                    on_state: None,
                };
                table.foreign_keys.push((own, reference));
            }
            // IF NOT EXISTS and the table's name are read above; the rest is an option.
            _ => table_option(part, &mut table.options)?,
        }
    }
    let unique = table.unique_constraints.iter();
    let foreign = table.foreign_keys.iter();
    let lists = (unique.map(|columns| ("UNIQUE", columns)))
        .chain([("PRIMARY KEY", &table.primary_key)])
        .chain(foreign.map(|(own, _)| ("FOREIGN KEY", own)));
    for (what, columns) in lists {
        let declared = |name: &&String| table.columns.iter().any(|column| column.name == **name);
        if let Some(unknown) = columns.iter().find(|name| !declared(name)) {
            return Err(refuse(format!(
                "{what} constraint references unknown column '{unknown}'"
            )));
        }
    }
    Ok(table)
}

/// Reads one option of a table into `options`, refusing one written twice.
pub(super) fn table_option(part: Pair<'_>, options: &mut TableOptions) -> Result<()> {
    match part.as_rule() {
        Rule::immutable => {
            once(options.immutable, "IMMUTABLE")?;
            options.immutable = true;
        }
        Rule::state_machine => {
            once(options.state_machine.is_some(), "STATE MACHINE")?;
            let transitions = part.clone().into_inner().skip(1).map(|transition| {
                let mut states = values(transition);
                (states.remove(0), states)
            });
            options.state_machine = Some(StateMachine {
                transitions: transitions.collect(),
                column: first_ident(part),
            });
        }
        Rule::dag => {
            once(!options.dag.is_empty(), "DAG")?;
            options.dag = strings(part);
        }
        Rule::propagate_edge => options.propagate_edges.push(EdgePropagation {
            edge_type: values(part.clone()).swap_remove(0),
            direction: keyword(&child(part.clone(), Rule::edge_direction).expect("a direction")),
            rule: propagation(part)?,
        }),
        Rule::exclude_vector => options.exclude_vector_states.extend(values(part)),
        Rule::retain => {
            once(options.retain.is_some(), "RETAIN")?;
            let mut parts = part.into_inner();
            let count = number(&parts.next().expect("a count"), "RETAIN")?;
            let unit = match keyword(&parts.next().expect("a unit")).as_str() {
                "SECONDS" => RetainUnit::Seconds,
                "MINUTES" => RetainUnit::Minutes,
                "HOURS" => RetainUnit::Hours,
                _ => RetainUnit::Days,
            };
            let sync_safe = parts.next();
            if let Some(direction) = sync_safe.clone().and_then(|safe| safe.into_inner().next()) {
                let clause = match keyword(&direction).as_str() {
                    "PUSH" => "SYNC PUSH ONLY",
                    _ => "SYNC TWO WAY",
                };
                return Err(refuse(format!(
                    "RETAIN names no direction: write {clause} as a clause of its own"
                )));
            }
            options.retain = Some(Retain {
                count,
                unit,
                sync_safe: sync_safe.is_some(),
            });
        }
        Rule::history => {
            once(options.history.is_some(), "HISTORY")?;
            options.history = Some(match keyword(&first(part)).as_str() {
                "ALL" => History::All,
                _ => History::CurrentOnly,
            });
        }
        Rule::sync => {
            once(options.sync.is_some(), "SYNC")?;
            options.sync = Some(match keyword(&first(part)).as_str() {
                "OFF" => SyncDirection::Off,
                "PUSH" => SyncDirection::PushOnly,
                "PULL" => SyncDirection::PullOnly,
                _ => SyncDirection::TwoWay,
            });
        }
        Rule::sync_conflict => {
            once(options.conflict.is_some(), "SYNC CONFLICT")?;
            options.conflict = Some(match keyword(&first(part)).as_str() {
                "FIRST" => ConflictPolicy::KeepFirst,
                _ => ConflictPolicy::KeepLatest,
            });
        }
        Rule::maintain_count => {
            once(options.kept_counts.is_some(), "MAINTAIN COUNT")?;
            options.kept_counts = Some(kept_counts(part)?);
        }
        Rule::delivery_manifest => {
            once(!options.delivery_manifest.is_empty(), "DELIVERY MANIFEST")?;
            options.delivery_manifest = idents(part);
        }
        Rule::edge_discard => {
            once(options.edge_discard.is_some(), "EDGE DISCARD")?;
            options.edge_discard = Some(match keyword(&first(part)).as_str() {
                "NEVER" => EdgeDiscard::Never,
                "AFTER" => EdgeDiscard::AfterOutcome,
                _ => EdgeDiscard::Always,
            });
        }
        _ => {}
    }
    Ok(())
}

/// `COMPACTION NONE` or `COMPACTION (...)`, which is NONE when it has no member.
pub(super) fn compaction(clause: Pair<'_>) -> CompactionClause {
    let member = |rule| child(clause.clone(), rule).and_then(signed);
    let [change, tombstone] = [Rule::change_percent, Rule::tombstone_percent].map(member);
    match (change, tombstone) {
        (None, None) => CompactionClause::None,
        (change, tombstone) => CompactionClause::Percent { change, tombstone },
    }
}

pub(super) fn column_def(pair: Pair<'_>) -> Result<ColumnDef> {
    let mut parts = pair.into_inner();
    let name = ident(&parts.next().expect("a column name"));
    let type_pair = parts.next().expect("a column type");
    let type_name = type_pair.as_str().to_ascii_uppercase();
    let mut vector = VectorClauses::default();
    let data_type = match (type_pair.into_inner().next(), type_name.as_str()) {
        (Some(declared), _) => {
            let mut parts = declared.into_inner();
            let dimension = parts.next().expect("a dimension");
            for clause in parts {
                match clause.as_rule() {
                    Rule::quantization => {
                        let text = clause.into_inner().next().expect("a quoted text");
                        vector.quantization = Some(string(&text));
                    }
                    Rule::partition_key => vector.partition_key = idents(clause),
                    Rule::max_partitions => vector.max_partitions = signed(clause),
                    Rule::search_mode => vector.search_mode = Some(search_mode(&clause)),
                    Rule::indexing_threshold => vector.indexing_threshold = signed(clause),
                    Rule::probes => vector.probes = signed(clause),
                    // The one clause left: COMPACTION.
                    _ => vector.compaction = Some(compaction(clause)),
                }
            }
            DataType::Vector(number::<u32>(&dimension, "the VECTOR dimension")? as usize)
        }
        (None, "UUID") => DataType::Uuid,
        (None, "TEXT") => DataType::Text,
        (None, "INTEGER" | "INT") => DataType::Integer,
        (None, "REAL" | "FLOAT") => DataType::Real,
        (None, "BOOLEAN" | "BOOL") => DataType::Boolean,
        (None, "TIMESTAMP") => DataType::Timestamp,
        (None, "TXID") => DataType::TxId,
        (None, _) => DataType::Json,
    };
    let mut column = ColumnDef {
        name,
        data_type,
        nullable: true,
        primary_key: false,
        unique: false,
        default: None,
        vector,
        references: None,
        immutable: false,
        context_id: false,
        scope_labels: None,
        acl: None,
        rank_policy: None,
        expires: false,
    };
    for constraint in parts {
        match constraint.as_rule() {
            Rule::expires => {
                once(column.expires, "EXPIRES")?;
                column.expires = true;
            }
            Rule::not_null => {
                once(!column.nullable, "NOT NULL")?;
                column.nullable = false;
            }
            Rule::nullable_marker => {}
            Rule::primary_key => {
                once(column.primary_key, "PRIMARY KEY")?;
                column.primary_key = true;
            }
            Rule::unique => {
                once(column.unique, "UNIQUE")?;
                column.unique = true;
            }
            Rule::references => {
                once(column.references.is_some(), "REFERENCES")?;
                let [table, parent] = two_idents(constraint.clone());
                column.references = Some(Reference {
                    follows_parent_purge: has(&constraint, Rule::follows_parent_purge),
                    table,
                    columns: vec![parent],
                    on_state: child(constraint, Rule::propagation)
                        .map(propagation)
                        .transpose()?,
                });
            }
            Rule::immutable => {
                once(column.immutable, "IMMUTABLE")?;
                column.immutable = true;
            }
            Rule::context_id => {
                once(column.context_id, "CONTEXT_ID")?;
                column.context_id = true;
            }
            Rule::scope_labels => {
                once(column.scope_labels.is_some(), "SCOPE_LABEL")?;
                let mut lists = constraint.into_inner().map(strings);
                column.scope_labels = Some(ScopeLabels {
                    read: lists.next().expect("a list of labels"),
                    write: lists.next(),
                });
            }
            Rule::acl => {
                once(column.acl.is_some(), "ACL")?;
                let [table, granted] = two_idents(constraint);
                column.acl = Some((table, granted));
            }
            Rule::rank_policy => {
                once(column.rank_policy.is_some(), "RANK_POLICY")?;
                let mut names = idents(constraint.clone()).into_iter();
                let mut name = || names.next().expect("the grammar reads three names here");
                column.rank_policy = Some(RankPolicy {
                    table: name(),
                    column: name(),
                    sort_key: name(),
                    formula: strings(constraint).swap_remove(0),
                });
            }
            // The one constraint left: DEFAULT.
            _ => {
                once(column.default.is_some(), "DEFAULT")?;
                column.default = Some(expr::expr(first(constraint))?);
            }
        }
    }
    Ok(column)
}

pub(super) fn create_index(pair: Pair<'_>) -> CreateIndex {
    let [name, table] = two_idents(pair.clone());
    let mut index = CreateIndex {
        name,
        table,
        columns: Vec::new(),
        json_path: Vec::new(),
    };
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::indexed_column => index.columns.push(indexed_column(part)),
            Rule::json_path => {
                index.json_path = strings(part.clone());
                index.columns.push((first_ident(part), SortDirection::Asc));
            }
            // The index's name and its table's, read above.
            _ => {}
        }
    }
    index
}

/// The carried event forms use the same decoded identifiers and strings as tables.
pub(super) fn event_declaration(pair: Pair<'_>) -> Result<Statement> {
    use crate::types::*;
    use EventDeclaration::*;
    let mut names = idents(pair.clone()).into_iter();
    let name = names.next().expect("a declaration name");
    let get = |rule| child(pair.clone(), rule).expect("a grammar clause");
    let event = || match get(Rule::event_trigger)
        .as_str()
        .to_ascii_uppercase()
        .as_str()
    {
        "INSERT" => EventTypeTrigger::Insert,
        "UPDATE" => EventTypeTrigger::Update,
        _ => EventTypeTrigger::Delete,
    };
    let declaration = match pair.as_rule() {
        Rule::create_trigger_stmt => Trigger(TriggerDecl {
            name,
            table: names.next().expect("a table"),
            including_sync: has(&pair, Rule::including_sync_clause),
            event: match event() {
                EventTypeTrigger::Insert => TriggerEvent::Insert,
                EventTypeTrigger::Update => TriggerEvent::Update,
                _ => return Err(Error::TriggerEventUnsupported),
            },
        }),
        Rule::create_event_type_stmt => EventType(EventTypeDecl {
            name,
            table: names.next().expect("a table"),
            event: event(),
        }),
        Rule::create_sink_stmt => Sink(SinkDecl {
            name,
            sink_type: if get(Rule::sink_type)
                .as_str()
                .eq_ignore_ascii_case("callback")
            {
                SinkType::Callback
            } else {
                SinkType::Webhook
            },
            url: child(pair.clone(), Rule::sink_url_clause).map(|p| string(&first(p))),
        }),
        Rule::create_route_stmt => Route(RouteDecl {
            name,
            event_type: names.next().expect("an event type"),
            sink: names.next().expect("a sink"),
            where_in: child(pair.clone(), Rule::route_where_clause).map(|p| RouteWhereIn {
                column: first_ident(p.clone()),
                values: strings(child(p, Rule::labels).expect("labels")),
            }),
        }),
        Rule::create_schedule_stmt => {
            let text = string(&get(Rule::string));
            let words: Vec<_> = text.split_whitespace().collect();
            let multiplier = match words.get(1).map(|s| s.to_ascii_lowercase()).as_deref() {
                Some("millisecond" | "milliseconds") => 1,
                Some("second" | "seconds") => 1000,
                Some("minute" | "minutes") => 60_000,
                Some("hour" | "hours") => 3_600_000,
                _ => 0,
            };
            let every_ms = words
                .first()
                .and_then(|s| s.parse::<u64>().ok())
                .and_then(|n| n.checked_mul(multiplier))
                .filter(|n| *n > 0 && words.len() == 2)
                .ok_or_else(|| {
                    Error::PlanError(format!(
                        "invalid schedule interval: {}",
                        crate::types::excerpt(&text)
                    ))
                })?;
            let policy = child(pair.clone(), Rule::missed_tick_clause);
            let missed_tick_policy = policy
                .clone()
                .map(|p| string(&first(p)).trim().to_ascii_lowercase())
                .unwrap_or_else(|| "skip-and-audit".into());
            let missed_tick_policy = match missed_tick_policy.as_str() {
                "skip-and-audit" => MissedTickPolicy::SkipAndAudit,
                "catch-up" => MissedTickPolicy::CatchUp,
                "fail-loud" => MissedTickPolicy::FailLoud,
                _ => {
                    return Err(Error::PlanError(format!(
                        "invalid missed tick policy: {}",
                        crate::types::excerpt(&missed_tick_policy)
                    )));
                }
            };
            let catch_up_within_seconds = policy
                .and_then(|p| child(p, Rule::integer))
                .map(|p| number(&p, "catch-up seconds"))
                .transpose()?;
            Schedule(ScheduleDecl {
                name,
                every_ms,
                callback: names.next().expect("a callback"),
                missed_tick_policy,
                catch_up_within_seconds,
            })
        }
        rule => {
            return Ok(Statement::DropEvent {
                name,
                if_exists: has(&pair, Rule::if_exists),
                table: child(pair.clone(), Rule::on_table).map(first_ident),
                kind: match rule {
                    Rule::drop_trigger_stmt => Kind::Trigger,
                    Rule::drop_route_stmt => Kind::Route,
                    _ => Kind::Schedule,
                },
            });
        }
    };
    Ok(Statement::DeclareEvent(declaration))
}
