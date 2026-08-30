/// The store or session effect of a recognized REPL command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommandEffect {
    StoreRead,
    StoreWrite,
    OwnerStatus,
    SessionOnly,
    Invalid,
}

/// One canonical command spelling and its effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CommandDeclaration {
    pub spelling: &'static str,
    pub effect: CommandEffect,
}

/// A conventional spelling that delegates to a canonical command declaration.
///
/// Aliases intentionally carry no effect. Their effect is always resolved from
/// the command they name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CommandAlias {
    pub spelling: &'static str,
    pub canonical_spelling: &'static str,
}

/// The canonical dispatch identity for a top-level store operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OperationalCommandKind {
    Migrate,
    Reset,
    Diagnose,
    Snapshot,
    Inspect,
    Purge,
}

/// A top-level operation's typed identity and spelling for one future command
/// tree. It intentionally has no effect field: its user-visible access rule
/// belongs to the operation's established contract, not this classifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OperationalCommand {
    pub kind: OperationalCommandKind,
    pub spelling: &'static str,
}

/// The one discovery surface that human help and the documentation may expose.
///
/// This deliberately has no rendered wording or internal enum labels.  A
/// consumer may add syntax and prose after a spelling, but it must obtain the
/// spelling from this list rather than keeping a second command/help table.
/// Invalid parent forms are parser refusals, not discoverable commands.
/// What `.help` SHOWS for a command an operator cannot type on its own.
///
/// A command's spelling is what dispatch matches, and for a toggle that is the
/// bare word -- but an operator reading help needs the form they actually
/// type. `.trace` alone does nothing; `.trace on` and `.trace off` are the
/// invocations, and the ratified command table spells them that way. The
/// canonical list stays the registry's own spellings; only the rendering
/// carries the argument.
pub(crate) fn help_signature(spelling: &str) -> &str {
    match spelling {
        ".trace" => ".trace on|off",
        other => other,
    }
}

pub fn canonical_help_signatures() -> Vec<&'static str> {
    META_COMMANDS
        .iter()
        .filter(|declaration| declaration.effect != CommandEffect::Invalid)
        .map(|declaration| declaration.spelling)
        .chain(CONVENTIONAL_ALIASES.iter().map(|alias| alias.spelling))
        .chain(OPERATIONAL_COMMANDS.iter().map(|command| command.spelling))
        .collect()
}

/// The result required at the authorization boundary immediately before REPL
/// dispatch.  A StoreWrite row is refused here when the session was not opened
/// for writing; dispatch must never see that row in such a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetaCommandAuthorization {
    Dispatch(&'static CommandDeclaration),
    RefuseStoreWrite(&'static CommandDeclaration),
}

/// The sole canonical dot-command table from which classification and
/// discovery derive.
pub(crate) static META_COMMANDS: &[CommandDeclaration] = &[
    CommandDeclaration {
        spelling: ".tables",
        effect: CommandEffect::StoreRead,
    },
    CommandDeclaration {
        spelling: ".schema",
        effect: CommandEffect::StoreRead,
    },
    CommandDeclaration {
        spelling: ".explain",
        effect: CommandEffect::StoreRead,
    },
    CommandDeclaration {
        spelling: ".events status",
        effect: CommandEffect::StoreRead,
    },
    CommandDeclaration {
        spelling: ".maintenance status",
        effect: CommandEffect::StoreRead,
    },
    CommandDeclaration {
        spelling: ".cursor open",
        effect: CommandEffect::StoreRead,
    },
    CommandDeclaration {
        spelling: ".cursor fetch",
        effect: CommandEffect::StoreRead,
    },
    CommandDeclaration {
        spelling: ".cursor close",
        effect: CommandEffect::StoreRead,
    },
    CommandDeclaration {
        spelling: ".maintenance run",
        effect: CommandEffect::StoreWrite,
    },
    CommandDeclaration {
        spelling: ".maintenance compact",
        effect: CommandEffect::StoreWrite,
    },
    CommandDeclaration {
        spelling: ".sync push",
        effect: CommandEffect::StoreWrite,
    },
    CommandDeclaration {
        spelling: ".sync pull",
        effect: CommandEffect::StoreWrite,
    },
    CommandDeclaration {
        spelling: ".sync reconnect",
        effect: CommandEffect::StoreWrite,
    },
    CommandDeclaration {
        spelling: ".sync destination",
        effect: CommandEffect::StoreWrite,
    },
    CommandDeclaration {
        spelling: ".sync auto",
        effect: CommandEffect::StoreWrite,
    },
    CommandDeclaration {
        spelling: ".owner status",
        effect: CommandEffect::OwnerStatus,
    },
    CommandDeclaration {
        spelling: ".help",
        effect: CommandEffect::SessionOnly,
    },
    CommandDeclaration {
        spelling: ".trace",
        effect: CommandEffect::SessionOnly,
    },
    CommandDeclaration {
        spelling: ".quit",
        effect: CommandEffect::SessionOnly,
    },
    CommandDeclaration {
        spelling: ".exit",
        effect: CommandEffect::SessionOnly,
    },
    CommandDeclaration {
        spelling: ".sync status",
        effect: CommandEffect::SessionOnly,
    },
    CommandDeclaration {
        spelling: ".events",
        effect: CommandEffect::Invalid,
    },
    CommandDeclaration {
        spelling: ".maintenance",
        effect: CommandEffect::Invalid,
    },
    CommandDeclaration {
        spelling: ".cursor",
        effect: CommandEffect::Invalid,
    },
    CommandDeclaration {
        spelling: ".sync",
        effect: CommandEffect::Invalid,
    },
    CommandDeclaration {
        spelling: ".owner",
        effect: CommandEffect::Invalid,
    },
];

/// Conventional aliases point at a declaration in [`META_COMMANDS`], never at
/// a second effect table.
pub(crate) static CONVENTIONAL_ALIASES: &[CommandAlias] = &[
    CommandAlias {
        spelling: "\\dt",
        canonical_spelling: ".tables",
    },
    CommandAlias {
        spelling: "\\d",
        canonical_spelling: ".schema",
    },
    CommandAlias {
        spelling: "\\q",
        canonical_spelling: ".quit",
    },
    CommandAlias {
        spelling: "\\?",
        canonical_spelling: ".help",
    },
];

/// The typed operational rows a later one-tree dispatch can consume.
pub(crate) static OPERATIONAL_COMMANDS: &[OperationalCommand] = &[
    OperationalCommand {
        kind: OperationalCommandKind::Migrate,
        spelling: "migrate",
    },
    OperationalCommand {
        kind: OperationalCommandKind::Reset,
        spelling: "reset",
    },
    OperationalCommand {
        kind: OperationalCommandKind::Diagnose,
        spelling: "diagnose",
    },
    OperationalCommand {
        kind: OperationalCommandKind::Snapshot,
        spelling: "snapshot",
    },
    OperationalCommand {
        kind: OperationalCommandKind::Inspect,
        spelling: "inspect",
    },
    OperationalCommand {
        kind: OperationalCommandKind::Purge,
        spelling: "purge",
    },
];

/// Classify a command without reconstructing an alias effect.
#[allow(dead_code)]
pub(crate) fn classify_meta_command(line: &str) -> CommandEffect {
    resolve_meta_command(line)
        .map(|declaration| declaration.effect)
        .unwrap_or(CommandEffect::Invalid)
}

/// Resolve a spelling to the declaration that owns its effect.
///
/// This is deliberately an internal seam: alias dispatch borrows the exact
/// canonical row rather than reconstructing an equivalent effect.
pub(crate) fn resolve_meta_command(line: &str) -> Option<&'static CommandDeclaration> {
    let line = line.trim();

    let direct = META_COMMANDS
        .iter()
        .filter_map(|declaration| {
            invocation_arguments(line, declaration.spelling)
                .map(|arguments| (declaration, arguments))
        })
        .filter(|(declaration, arguments)| invocation_is_valid(declaration, arguments))
        .max_by_key(|(declaration, _)| declaration.spelling.len())
        .map(|(declaration, _)| declaration);

    if let Some(declaration) = direct {
        return (declaration.effect != CommandEffect::Invalid).then_some(declaration);
    }

    CONVENTIONAL_ALIASES.iter().find_map(|alias| {
        let arguments = invocation_arguments(line, alias.spelling)?;
        let declaration = META_COMMANDS
            .iter()
            .find(|candidate| candidate.spelling == alias.canonical_spelling)?;
        invocation_is_valid(declaration, arguments).then_some(declaration)
    })
}

/// Classify and authorize a meta-command before any command-specific dispatch.
///
/// The later REPL integration must call this seam before dispatch, so a
/// refusal is observable even for commands whose current dispatch would
/// otherwise be a no-op or only mutate session state.
pub(crate) fn authorize_meta_command_before_dispatch(
    line: &str,
    store_writes_permitted: bool,
) -> Option<MetaCommandAuthorization> {
    let declaration = resolve_meta_command(line)?;
    match declaration.effect {
        CommandEffect::StoreWrite if !store_writes_permitted => {
            Some(MetaCommandAuthorization::RefuseStoreWrite(declaration))
        }
        _ => Some(MetaCommandAuthorization::Dispatch(declaration)),
    }
}

/// The generated read-command discovery list from the canonical table.
#[allow(dead_code)]
pub(crate) fn read_command_discovery() -> Vec<&'static str> {
    META_COMMANDS
        .iter()
        .filter(|declaration| declaration.effect == CommandEffect::StoreRead)
        .map(|declaration| declaration.spelling)
        .collect()
}

/// The generated requires-write discovery list from the canonical table.
#[allow(dead_code)]
pub(crate) fn requires_write_command_discovery() -> Vec<&'static str> {
    META_COMMANDS
        .iter()
        .filter(|declaration| declaration.effect == CommandEffect::StoreWrite)
        .map(|declaration| declaration.spelling)
        .collect()
}

/// Return the trimmed argument tail only when `spelling` ends at a command
/// boundary. This keeps `.schema` from accepting `.schemaful` and `\\d` from
/// accepting `\\dtables`.
fn invocation_arguments<'a>(line: &'a str, spelling: &str) -> Option<&'a str> {
    let arguments = line.strip_prefix(spelling)?;
    if arguments.is_empty() {
        return Some(arguments);
    }
    arguments
        .chars()
        .next()
        .filter(|character| character.is_whitespace())?;
    Some(arguments.trim())
}

/// The registry recognizes command grammar, not just a prefix. Full argument
/// semantics remain with the later command dispatcher.
fn invocation_is_valid(declaration: &CommandDeclaration, arguments: &str) -> bool {
    match declaration.spelling {
        ".schema" | ".explain" | ".cursor open" => !arguments.is_empty(),
        ".tables" | ".events status" => continuation_arguments_are_valid(arguments),
        ".cursor fetch" => {
            arguments.is_empty()
                || (arguments.bytes().all(|byte| byte.is_ascii_digit())
                    && arguments.parse::<usize>().is_ok())
        }
        ".sync destination" => true,
        ".sync auto" => arguments.is_empty() || matches!(arguments, "on" | "off"),
        ".trace" => arguments.is_empty() || matches!(arguments, "on" | "off"),
        ".maintenance status"
        | ".cursor close"
        | ".maintenance run"
        | ".maintenance compact"
        | ".sync push"
        | ".sync pull"
        | ".sync reconnect"
        | ".owner status"
        | ".quit"
        | ".exit"
        | ".sync status"
        | ".events"
        | ".maintenance"
        | ".cursor"
        | ".sync"
        | ".owner" => arguments.is_empty(),
        ".help" => true,
        _ => false,
    }
}

/// `.tables` and `.events status` may resume only with the continuation token
/// issued by that same command. The token itself is opaque to this registry.
fn continuation_arguments_are_valid(arguments: &str) -> bool {
    if arguments.is_empty() {
        return true;
    }

    invocation_arguments(arguments, "--continue").is_some_and(|token| !token.is_empty())
}

/// The top-level operation spellings for ordinary `--help` discovery.
pub fn operational_command_discovery() -> Vec<&'static str> {
    OPERATIONAL_COMMANDS
        .iter()
        .map(|declaration| declaration.spelling)
        .collect()
}

#[cfg(test)]
mod contract_tests {
    use super::*;
    use std::collections::HashSet;

    const EXPECTED_META_COMMANDS: &[CommandDeclaration] = &[
        CommandDeclaration {
            spelling: ".tables",
            effect: CommandEffect::StoreRead,
        },
        CommandDeclaration {
            spelling: ".schema",
            effect: CommandEffect::StoreRead,
        },
        CommandDeclaration {
            spelling: ".explain",
            effect: CommandEffect::StoreRead,
        },
        CommandDeclaration {
            spelling: ".events status",
            effect: CommandEffect::StoreRead,
        },
        CommandDeclaration {
            spelling: ".maintenance status",
            effect: CommandEffect::StoreRead,
        },
        CommandDeclaration {
            spelling: ".cursor open",
            effect: CommandEffect::StoreRead,
        },
        CommandDeclaration {
            spelling: ".cursor fetch",
            effect: CommandEffect::StoreRead,
        },
        CommandDeclaration {
            spelling: ".cursor close",
            effect: CommandEffect::StoreRead,
        },
        CommandDeclaration {
            spelling: ".maintenance run",
            effect: CommandEffect::StoreWrite,
        },
        CommandDeclaration {
            spelling: ".maintenance compact",
            effect: CommandEffect::StoreWrite,
        },
        CommandDeclaration {
            spelling: ".sync push",
            effect: CommandEffect::StoreWrite,
        },
        CommandDeclaration {
            spelling: ".sync pull",
            effect: CommandEffect::StoreWrite,
        },
        CommandDeclaration {
            spelling: ".sync reconnect",
            effect: CommandEffect::StoreWrite,
        },
        CommandDeclaration {
            spelling: ".sync destination",
            effect: CommandEffect::StoreWrite,
        },
        CommandDeclaration {
            spelling: ".sync auto",
            effect: CommandEffect::StoreWrite,
        },
        CommandDeclaration {
            spelling: ".owner status",
            effect: CommandEffect::OwnerStatus,
        },
        CommandDeclaration {
            spelling: ".help",
            effect: CommandEffect::SessionOnly,
        },
        CommandDeclaration {
            spelling: ".trace",
            effect: CommandEffect::SessionOnly,
        },
        CommandDeclaration {
            spelling: ".quit",
            effect: CommandEffect::SessionOnly,
        },
        CommandDeclaration {
            spelling: ".exit",
            effect: CommandEffect::SessionOnly,
        },
        CommandDeclaration {
            spelling: ".sync status",
            effect: CommandEffect::SessionOnly,
        },
        CommandDeclaration {
            spelling: ".events",
            effect: CommandEffect::Invalid,
        },
        CommandDeclaration {
            spelling: ".maintenance",
            effect: CommandEffect::Invalid,
        },
        CommandDeclaration {
            spelling: ".cursor",
            effect: CommandEffect::Invalid,
        },
        CommandDeclaration {
            spelling: ".sync",
            effect: CommandEffect::Invalid,
        },
        CommandDeclaration {
            spelling: ".owner",
            effect: CommandEffect::Invalid,
        },
    ];

    const EXPECTED_ALIASES: &[CommandAlias] = &[
        CommandAlias {
            spelling: "\\dt",
            canonical_spelling: ".tables",
        },
        CommandAlias {
            spelling: "\\d",
            canonical_spelling: ".schema",
        },
        CommandAlias {
            spelling: "\\q",
            canonical_spelling: ".quit",
        },
        CommandAlias {
            spelling: "\\?",
            canonical_spelling: ".help",
        },
    ];

    const EXPECTED_OPERATIONAL_COMMANDS: &[OperationalCommand] = &[
        OperationalCommand {
            kind: OperationalCommandKind::Migrate,
            spelling: "migrate",
        },
        OperationalCommand {
            kind: OperationalCommandKind::Reset,
            spelling: "reset",
        },
        OperationalCommand {
            kind: OperationalCommandKind::Diagnose,
            spelling: "diagnose",
        },
        OperationalCommand {
            kind: OperationalCommandKind::Snapshot,
            spelling: "snapshot",
        },
        OperationalCommand {
            kind: OperationalCommandKind::Inspect,
            spelling: "inspect",
        },
        OperationalCommand {
            kind: OperationalCommandKind::Purge,
            spelling: "purge",
        },
    ];

    #[test]
    fn canonical_rows_aliases_and_operational_rows_are_exact_and_unique() {
        assert_eq!(META_COMMANDS, EXPECTED_META_COMMANDS);
        assert_eq!(CONVENTIONAL_ALIASES, EXPECTED_ALIASES);
        assert_eq!(OPERATIONAL_COMMANDS, EXPECTED_OPERATIONAL_COMMANDS);

        let mut spellings = HashSet::new();
        for spelling in META_COMMANDS
            .iter()
            .map(|row| row.spelling)
            .chain(CONVENTIONAL_ALIASES.iter().map(|row| row.spelling))
            .chain(OPERATIONAL_COMMANDS.iter().map(|row| row.spelling))
        {
            assert!(
                spellings.insert(spelling),
                "registered spelling {spelling:?} is duplicated"
            );
        }
        assert_eq!(
            spellings.len(),
            META_COMMANDS.len() + CONVENTIONAL_ALIASES.len() + OPERATIONAL_COMMANDS.len()
        );
        for alias in CONVENTIONAL_ALIASES {
            assert!(
                META_COMMANDS
                    .iter()
                    .any(|row| row.spelling == alias.canonical_spelling)
            );
        }
    }

    #[test]
    fn classifier_accepts_only_declared_grammar_and_removed_aliases_stay_invalid() {
        for (line, effect) in [
            (".tables", CommandEffect::StoreRead),
            (".schema entries", CommandEffect::StoreRead),
            (".explain DELETE FROM entries", CommandEffect::StoreRead),
            (".events status", CommandEffect::StoreRead),
            (".maintenance status", CommandEffect::StoreRead),
            (
                ".cursor open SELECT * FROM entries",
                CommandEffect::StoreRead,
            ),
            (".cursor fetch 25", CommandEffect::StoreRead),
            (".cursor close", CommandEffect::StoreRead),
            (".maintenance run", CommandEffect::StoreWrite),
            (".maintenance compact", CommandEffect::StoreWrite),
            (".sync push", CommandEffect::StoreWrite),
            (".sync pull", CommandEffect::StoreWrite),
            (".sync reconnect", CommandEffect::StoreWrite),
            (".sync destination hub", CommandEffect::StoreWrite),
            (".sync auto on", CommandEffect::StoreWrite),
            (".owner status", CommandEffect::OwnerStatus),
            (".help vector", CommandEffect::SessionOnly),
            (".trace on", CommandEffect::SessionOnly),
            (".quit", CommandEffect::SessionOnly),
            (".exit", CommandEffect::SessionOnly),
            (".sync status", CommandEffect::SessionOnly),
            (".events", CommandEffect::Invalid),
            (".maintenance", CommandEffect::Invalid),
            (".cursor", CommandEffect::Invalid),
            (".cursor open", CommandEffect::Invalid),
            (".cursor fetch every", CommandEffect::Invalid),
            (".cursor close again", CommandEffect::Invalid),
            (".sync", CommandEffect::Invalid),
            (".owner", CommandEffect::Invalid),
            (".unknown", CommandEffect::Invalid),
            ("\\trace", CommandEffect::Invalid),
            ("\\sync", CommandEffect::Invalid),
            ("\\dtables", CommandEffect::Invalid),
            ("\\dentries", CommandEffect::Invalid),
            ("\\quitnow", CommandEffect::Invalid),
            ("\\?vector", CommandEffect::Invalid),
        ] {
            assert_eq!(
                classify_meta_command(line),
                effect,
                "{line} must retain its declared effect"
            );
        }
    }

    #[test]
    fn aliases_borrow_the_same_canonical_declaration_identity() {
        for (alias, canonical) in [
            ("\\dt", ".tables"),
            ("\\d entries", ".schema entries"),
            ("\\q", ".quit"),
            ("\\? vector", ".help vector"),
        ] {
            let alias_row = resolve_meta_command(alias).expect("alias must resolve");
            let canonical_row =
                resolve_meta_command(canonical).expect("canonical spelling must resolve");
            assert!(
                std::ptr::eq(alias_row, canonical_row),
                "{alias} must borrow {canonical}'s canonical declaration, not reconstruct an equal effect"
            );
            assert_eq!(
                classify_meta_command(alias),
                alias_row.effect,
                "{alias} classification must come from its resolved canonical declaration"
            );
            assert_eq!(
                classify_meta_command(canonical),
                canonical_row.effect,
                "{canonical} classification must come from its resolved canonical declaration"
            );
        }

        for line in [
            ".tables",
            ".schema entries",
            ".explain DELETE FROM entries",
            ".events status",
            ".maintenance status",
            ".cursor open SELECT * FROM entries",
            ".cursor fetch 25",
            ".cursor close",
            ".maintenance run",
            ".maintenance compact",
            ".sync push",
            ".sync pull",
            ".sync reconnect",
            ".sync destination hub",
            ".sync auto on",
            ".owner status",
            ".help vector",
            ".trace on",
            ".quit",
            ".exit",
            ".sync status",
        ] {
            let resolved = resolve_meta_command(line).expect("declared command must resolve");
            assert_eq!(
                classify_meta_command(line),
                resolved.effect,
                "{line} classification must match its canonical registry row"
            );
        }
    }

    #[test]
    fn generated_discovery_is_the_exact_filtered_canonical_registry() {
        assert_eq!(
            META_COMMANDS, EXPECTED_META_COMMANDS,
            "a missing registry must not make discovery vacuous"
        );
        let reads: Vec<_> = META_COMMANDS
            .iter()
            .filter(|row| row.effect == CommandEffect::StoreRead)
            .map(|row| row.spelling)
            .collect();
        let writes: Vec<_> = META_COMMANDS
            .iter()
            .filter(|row| row.effect == CommandEffect::StoreWrite)
            .map(|row| row.spelling)
            .collect();
        let operational: Vec<_> = OPERATIONAL_COMMANDS
            .iter()
            .map(|row| row.spelling)
            .collect();
        assert_eq!(read_command_discovery(), reads);
        assert_eq!(requires_write_command_discovery(), writes);
        assert_eq!(operational_command_discovery(), operational);
        assert!(!operational_command_discovery().contains(&"repair"));
    }

    #[test]
    fn help_signatures_and_the_pre_dispatch_authorization_seam_are_registry_derived() {
        let expected_help: Vec<_> = EXPECTED_META_COMMANDS
            .iter()
            .filter(|row| row.effect != CommandEffect::Invalid)
            .map(|row| row.spelling)
            .chain(EXPECTED_ALIASES.iter().map(|row| row.spelling))
            .chain(EXPECTED_OPERATIONAL_COMMANDS.iter().map(|row| row.spelling))
            .collect();
        assert_eq!(canonical_help_signatures(), expected_help);

        for row in EXPECTED_META_COMMANDS {
            let canonical = META_COMMANDS
                .iter()
                .find(|candidate| candidate.spelling == row.spelling)
                .expect("the exact-registry assertion above guarantees this row");
            let invocation = match row.spelling {
                ".schema" => ".schema entries",
                ".explain" => ".explain DELETE FROM entries",
                ".cursor open" => ".cursor open SELECT * FROM entries",
                ".cursor fetch" => ".cursor fetch 25",
                ".sync destination" => ".sync destination hub",
                ".sync auto" => ".sync auto on",
                ".help" => ".help vector",
                ".trace" => ".trace on",
                _ => row.spelling,
            };
            let authorization = authorize_meta_command_before_dispatch(invocation, false);
            match row.effect {
                CommandEffect::StoreWrite => match authorization {
                    Some(MetaCommandAuthorization::RefuseStoreWrite(resolved)) => {
                        assert!(std::ptr::eq(resolved, canonical));
                    }
                    other => panic!(
                        "{} must be refused by the pre-dispatch seam, got {other:?}",
                        row.spelling
                    ),
                },
                CommandEffect::Invalid => assert!(authorization.is_none()),
                _ => match authorization {
                    Some(MetaCommandAuthorization::Dispatch(resolved)) => {
                        assert!(std::ptr::eq(resolved, canonical));
                    }
                    other => panic!(
                        "{} must reach dispatch through its canonical row, got {other:?}",
                        row.spelling
                    ),
                },
            }
            if row.effect == CommandEffect::StoreWrite {
                match authorize_meta_command_before_dispatch(invocation, true) {
                    Some(MetaCommandAuthorization::Dispatch(resolved)) => {
                        assert!(std::ptr::eq(resolved, canonical));
                    }
                    other => panic!(
                        "{} must reach dispatch when store writes are permitted, got {other:?}",
                        row.spelling
                    ),
                }
            }
        }
    }
}
