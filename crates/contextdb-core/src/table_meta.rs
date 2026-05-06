use crate::Direction;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CompositeForeignKey {
    pub child_columns: Vec<String>,
    pub parent_table: String,
    pub parent_columns: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SingleColumnForeignKey {
    pub child_column: String,
    pub parent_table: String,
    pub parent_column: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct TableMeta {
    pub columns: Vec<ColumnDef>,
    pub immutable: bool,
    pub state_machine: Option<StateMachineConstraint>,
    #[serde(default)]
    pub dag_edge_types: Vec<String>,
    #[serde(default)]
    pub unique_constraints: Vec<Vec<String>>,
    pub natural_key_column: Option<String>,
    #[serde(default)]
    pub propagation_rules: Vec<PropagationRule>,
    #[serde(default)]
    pub default_ttl_seconds: Option<u64>,
    #[serde(default)]
    pub sync_safe: bool,
    #[serde(default)]
    pub expires_column: Option<String>,
    #[serde(default)]
    pub indexes: Vec<IndexDecl>,
    #[serde(default)]
    pub composite_foreign_keys: Vec<CompositeForeignKey>,
}

// Custom `Deserialize` that tolerates prior on-disk `TableMeta` encoded
// without the trailing `indexes` field (backward-compat).
impl<'de> serde::Deserialize<'de> for TableMeta {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{MapAccess, SeqAccess, Visitor};
        use std::fmt;

        struct TableMetaVisitor;

        impl<'de> Visitor<'de> for TableMetaVisitor {
            type Value = TableMeta;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a TableMeta")
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<TableMeta, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let columns = seq
                    .next_element::<Vec<ColumnDef>>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let immutable = seq
                    .next_element::<bool>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                let state_machine = seq
                    .next_element::<Option<StateMachineConstraint>>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(2, &self))?;
                let dag_edge_types = seq.next_element::<Vec<String>>()?.unwrap_or_default();
                let unique_constraints =
                    seq.next_element::<Vec<Vec<String>>>()?.unwrap_or_default();
                let natural_key_column = seq.next_element::<Option<String>>()?.unwrap_or_default();
                let propagation_rules = seq
                    .next_element::<Vec<PropagationRule>>()?
                    .unwrap_or_default();
                let default_ttl_seconds = seq.next_element::<Option<u64>>()?.unwrap_or_default();
                let sync_safe = seq.next_element::<bool>()?.unwrap_or_default();
                // Ok(None) at a declared-length tail is legitimate serde
                // behavior (declared-length sequence exhausted). Decode
                // errors, in contrast, indicate corruption or incompatible
                // on-disk payloads and must propagate rather than silently
                // default.
                let expires_column = seq.next_element::<Option<String>>()?.unwrap_or_default();
                let indexes = seq.next_element::<Vec<IndexDecl>>()?.unwrap_or_default();
                let composite_foreign_keys = seq
                    .next_element::<Vec<CompositeForeignKey>>()?
                    .unwrap_or_default();
                Ok(TableMeta {
                    columns,
                    immutable,
                    state_machine,
                    dag_edge_types,
                    unique_constraints,
                    natural_key_column,
                    propagation_rules,
                    default_ttl_seconds,
                    sync_safe,
                    expires_column,
                    indexes,
                    composite_foreign_keys,
                })
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<TableMeta, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut columns: Option<Vec<ColumnDef>> = None;
                let mut immutable: Option<bool> = None;
                let mut state_machine: Option<Option<StateMachineConstraint>> = None;
                let mut dag_edge_types: Option<Vec<String>> = None;
                let mut unique_constraints: Option<Vec<Vec<String>>> = None;
                let mut natural_key_column: Option<Option<String>> = None;
                let mut propagation_rules: Option<Vec<PropagationRule>> = None;
                let mut default_ttl_seconds: Option<Option<u64>> = None;
                let mut sync_safe: Option<bool> = None;
                let mut expires_column: Option<Option<String>> = None;
                let mut indexes: Option<Vec<IndexDecl>> = None;
                let mut composite_foreign_keys: Option<Vec<CompositeForeignKey>> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "columns" => columns = Some(map.next_value()?),
                        "immutable" => immutable = Some(map.next_value()?),
                        "state_machine" => state_machine = Some(map.next_value()?),
                        "dag_edge_types" => dag_edge_types = Some(map.next_value()?),
                        "unique_constraints" => unique_constraints = Some(map.next_value()?),
                        "natural_key_column" => natural_key_column = Some(map.next_value()?),
                        "propagation_rules" => propagation_rules = Some(map.next_value()?),
                        "default_ttl_seconds" => default_ttl_seconds = Some(map.next_value()?),
                        "sync_safe" => sync_safe = Some(map.next_value()?),
                        "expires_column" => expires_column = Some(map.next_value()?),
                        "indexes" => indexes = Some(map.next_value()?),
                        "composite_foreign_keys" => {
                            composite_foreign_keys = Some(map.next_value()?)
                        }
                        _ => {
                            let _: serde::de::IgnoredAny = map.next_value()?;
                        }
                    }
                }

                Ok(TableMeta {
                    columns: columns.ok_or_else(|| serde::de::Error::missing_field("columns"))?,
                    immutable: immutable
                        .ok_or_else(|| serde::de::Error::missing_field("immutable"))?,
                    state_machine: state_machine.unwrap_or_default(),
                    dag_edge_types: dag_edge_types.unwrap_or_default(),
                    unique_constraints: unique_constraints.unwrap_or_default(),
                    natural_key_column: natural_key_column.unwrap_or_default(),
                    propagation_rules: propagation_rules.unwrap_or_default(),
                    default_ttl_seconds: default_ttl_seconds.unwrap_or_default(),
                    sync_safe: sync_safe.unwrap_or_default(),
                    expires_column: expires_column.unwrap_or_default(),
                    indexes: indexes.unwrap_or_default(),
                    composite_foreign_keys: composite_foreign_keys.unwrap_or_default(),
                })
            }
        }

        const FIELDS: &[&str] = &[
            "columns",
            "immutable",
            "state_machine",
            "dag_edge_types",
            "unique_constraints",
            "natural_key_column",
            "propagation_rules",
            "default_ttl_seconds",
            "sync_safe",
            "expires_column",
            "indexes",
            "composite_foreign_keys",
        ];
        deserializer.deserialize_struct("TableMeta", FIELDS, TableMetaVisitor)
    }
}

/// Direction for a column within an engine-local index declaration.
/// Distinct from `contextdb_parser::ast::SortDirection`, which carries a
/// `CosineDistance` variant that is meaningful only for vector ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SortDirection {
    #[default]
    Asc,
    Desc,
}

/// How an index entered `TableMeta.indexes`. `Auto` indexes are synthesized
/// at CREATE TABLE time from PRIMARY KEY / UNIQUE constraints. `UserDeclared`
/// indexes come from `CREATE INDEX` DDL. The distinction drives surface
/// rendering (auto-indexes omitted from `.schema`), schema-rendering verbose
/// flags, and sync DDL emission (auto-indexes are not re-emitted since they
/// are derived from the CreateTable payload).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum IndexKind {
    Auto,
    #[default]
    UserDeclared,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDecl {
    pub name: String,
    pub columns: Vec<(String, SortDirection)>,
    #[serde(default)]
    pub kind: IndexKind,
}

impl IndexDecl {
    pub fn estimated_bytes(&self) -> usize {
        32 + self.name.len() * 16
            + self
                .columns
                .iter()
                .fold(0usize, |acc, (c, _)| acc.saturating_add(24 + c.len() * 16))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PropagationRule {
    ForeignKey {
        fk_column: String,
        referenced_table: String,
        referenced_column: String,
        trigger_state: String,
        target_state: String,
        max_depth: u32,
        abort_on_failure: bool,
    },
    Edge {
        edge_type: String,
        direction: Direction,
        trigger_state: String,
        target_state: String,
        max_depth: u32,
        abort_on_failure: bool,
    },
    VectorExclusion {
        trigger_state: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachineConstraint {
    pub column: String,
    pub transitions: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScopeLabelKind {
    Simple {
        write_labels: Vec<String>,
    },
    Split {
        read_labels: Vec<String>,
        write_labels: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AclRef {
    pub ref_table: String,
    pub ref_column: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ColumnDef {
    pub name: String,
    pub column_type: ColumnType,
    pub nullable: bool,
    pub primary_key: bool,
    #[serde(default)]
    pub unique: bool,
    #[serde(default)]
    pub default: Option<String>,
    #[serde(default)]
    pub references: Option<ForeignKeyReference>,
    #[serde(default)]
    pub expires: bool,
    #[serde(default)]
    pub immutable: bool,
    #[serde(default)]
    pub quantization: VectorQuantization,
    #[serde(default)]
    pub rank_policy: Option<RankPolicy>,
    #[serde(default)]
    pub context_id: bool,
    #[serde(default)]
    pub scope_label: Option<ScopeLabelKind>,
    #[serde(default)]
    pub acl_ref: Option<AclRef>,
}

// Custom `Deserialize` that tolerates prior on-disk schemas missing the
// trailing fields (backward-compat, I5). JSON / other formats that distinguish
// "missing field" from "required field" continue to work via `serde(default)`
// on the fields themselves.
impl<'de> serde::Deserialize<'de> for ColumnDef {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{MapAccess, SeqAccess, Visitor};
        use std::fmt;

        struct ColumnDefVisitor;

        impl<'de> Visitor<'de> for ColumnDefVisitor {
            type Value = ColumnDef;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a ColumnDef")
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<ColumnDef, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let name = seq
                    .next_element::<String>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let column_type = seq
                    .next_element::<ColumnType>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                let nullable = seq
                    .next_element::<bool>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(2, &self))?;
                let primary_key = seq
                    .next_element::<bool>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(3, &self))?;
                let unique = seq.next_element::<bool>()?.unwrap_or_default();
                let default = seq.next_element::<Option<String>>()?.unwrap_or_default();
                let references = seq
                    .next_element::<Option<ForeignKeyReference>>()?
                    .unwrap_or_default();
                let expires = seq.next_element::<bool>()?.unwrap_or_default();
                // Trailing field. `Ok(None)` means the declared-length seq
                // ended naturally (legitimate for JSON paths). Decode errors
                // propagate — silently defaulting to `false` would let a
                // corrupt payload pose as a non-immutable column.
                let immutable = seq.next_element::<bool>()?.unwrap_or_default();
                let quantization = seq
                    .next_element::<VectorQuantization>()?
                    .unwrap_or_default();
                let rank_policy = seq
                    .next_element::<Option<RankPolicy>>()?
                    .unwrap_or_default();
                let context_id = seq.next_element::<bool>()?.unwrap_or_default();
                let scope_label = seq
                    .next_element::<Option<ScopeLabelKind>>()?
                    .unwrap_or_default();
                let acl_ref = seq.next_element::<Option<AclRef>>()?.unwrap_or_default();
                Ok(ColumnDef {
                    name,
                    column_type,
                    nullable,
                    primary_key,
                    unique,
                    default,
                    references,
                    expires,
                    immutable,
                    quantization,
                    rank_policy,
                    context_id,
                    scope_label,
                    acl_ref,
                })
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<ColumnDef, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut name: Option<String> = None;
                let mut column_type: Option<ColumnType> = None;
                let mut nullable: Option<bool> = None;
                let mut primary_key: Option<bool> = None;
                let mut unique: Option<bool> = None;
                let mut default: Option<Option<String>> = None;
                let mut references: Option<Option<ForeignKeyReference>> = None;
                let mut expires: Option<bool> = None;
                let mut immutable: Option<bool> = None;
                let mut quantization: Option<VectorQuantization> = None;
                let mut rank_policy: Option<Option<RankPolicy>> = None;
                let mut context_id: Option<bool> = None;
                let mut scope_label: Option<Option<ScopeLabelKind>> = None;
                let mut acl_ref: Option<Option<AclRef>> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "name" => name = Some(map.next_value()?),
                        "column_type" => column_type = Some(map.next_value()?),
                        "nullable" => nullable = Some(map.next_value()?),
                        "primary_key" => primary_key = Some(map.next_value()?),
                        "unique" => unique = Some(map.next_value()?),
                        "default" => default = Some(map.next_value()?),
                        "references" => references = Some(map.next_value()?),
                        "expires" => expires = Some(map.next_value()?),
                        "immutable" => immutable = Some(map.next_value()?),
                        "quantization" => quantization = Some(map.next_value()?),
                        "rank_policy" => rank_policy = Some(map.next_value()?),
                        "context_id" => context_id = Some(map.next_value()?),
                        "scope_label" => scope_label = Some(map.next_value()?),
                        "acl_ref" => acl_ref = Some(map.next_value()?),
                        _ => {
                            let _: serde::de::IgnoredAny = map.next_value()?;
                        }
                    }
                }

                Ok(ColumnDef {
                    name: name.ok_or_else(|| serde::de::Error::missing_field("name"))?,
                    column_type: column_type
                        .ok_or_else(|| serde::de::Error::missing_field("column_type"))?,
                    nullable: nullable
                        .ok_or_else(|| serde::de::Error::missing_field("nullable"))?,
                    primary_key: primary_key
                        .ok_or_else(|| serde::de::Error::missing_field("primary_key"))?,
                    unique: unique.unwrap_or_default(),
                    default: default.unwrap_or_default(),
                    references: references.unwrap_or_default(),
                    expires: expires.unwrap_or_default(),
                    immutable: immutable.unwrap_or_default(),
                    quantization: quantization.unwrap_or_default(),
                    rank_policy: rank_policy.unwrap_or_default(),
                    context_id: context_id.unwrap_or_default(),
                    scope_label: scope_label.unwrap_or_default(),
                    acl_ref: acl_ref.unwrap_or_default(),
                })
            }
        }

        const FIELDS: &[&str] = &[
            "name",
            "column_type",
            "nullable",
            "primary_key",
            "unique",
            "default",
            "references",
            "expires",
            "immutable",
            "quantization",
            "rank_policy",
            "context_id",
            "scope_label",
            "acl_ref",
        ];
        deserializer.deserialize_struct("ColumnDef", FIELDS, ColumnDefVisitor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RankPolicy {
    pub joined_table: String,
    pub joined_column: String,
    #[serde(default)]
    pub anchor_column: String,
    pub sort_key: String,
    pub formula: String,
    pub protected_index: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignKeyReference {
    pub table: String,
    pub column: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnType {
    Integer,
    Real,
    Text,
    Boolean,
    Json,
    Uuid,
    Vector(usize),
    Timestamp,
    TxId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum VectorQuantization {
    #[default]
    F32,
    SQ8,
    SQ4,
}

impl VectorQuantization {
    pub fn as_str(self) -> &'static str {
        match self {
            VectorQuantization::F32 => "F32",
            VectorQuantization::SQ8 => "SQ8",
            VectorQuantization::SQ4 => "SQ4",
        }
    }

    pub fn storage_bytes(self, dimension: usize) -> usize {
        match self {
            VectorQuantization::F32 => dimension.saturating_mul(std::mem::size_of::<f32>()),
            VectorQuantization::SQ8 => dimension.saturating_add(8),
            VectorQuantization::SQ4 => dimension.div_ceil(2).saturating_add(8),
        }
    }
}

impl TableMeta {
    pub fn estimated_bytes(&self) -> usize {
        let columns_bytes = self.columns.iter().fold(0usize, |acc, column| {
            acc.saturating_add(column.estimated_bytes())
        });
        let state_machine_bytes = self
            .state_machine
            .as_ref()
            .map(StateMachineConstraint::estimated_bytes)
            .unwrap_or(0);
        let dag_bytes = self.dag_edge_types.iter().fold(0usize, |acc, edge_type| {
            acc.saturating_add(32 + edge_type.len() * 16)
        });
        let unique_constraint_bytes =
            self.unique_constraints.iter().fold(0usize, |acc, columns| {
                acc.saturating_add(
                    24 + columns
                        .iter()
                        .map(|column| 16 + column.len() * 16)
                        .sum::<usize>(),
                )
            });
        let natural_key_bytes = self
            .natural_key_column
            .as_ref()
            .map(|column| 32 + column.len() * 16)
            .unwrap_or(0);
        let propagation_bytes = self.propagation_rules.iter().fold(0usize, |acc, rule| {
            acc.saturating_add(rule.estimated_bytes())
        });
        let expires_bytes = self
            .expires_column
            .as_ref()
            .map(|column| 32 + column.len() * 16)
            .unwrap_or(0);
        let indexes_bytes = self
            .indexes
            .iter()
            .fold(0usize, |acc, i| acc.saturating_add(i.estimated_bytes()));
        let composite_foreign_key_bytes = self
            .composite_foreign_keys
            .iter()
            .fold(0usize, |acc, fk| acc.saturating_add(fk.estimated_bytes()));

        16 + columns_bytes
            + state_machine_bytes
            + dag_bytes
            + unique_constraint_bytes
            + natural_key_bytes
            + propagation_bytes
            + expires_bytes
            + indexes_bytes
            + composite_foreign_key_bytes
            + self.default_ttl_seconds.map(|_| 8).unwrap_or(0)
            + 8
    }
}

impl CompositeForeignKey {
    fn estimated_bytes(&self) -> usize {
        40 + self.parent_table.len() * 16
            + self.child_columns.iter().fold(0usize, |acc, column| {
                acc.saturating_add(16 + column.len() * 16)
            })
            + self.parent_columns.iter().fold(0usize, |acc, column| {
                acc.saturating_add(16 + column.len() * 16)
            })
    }
}

impl PropagationRule {
    fn estimated_bytes(&self) -> usize {
        match self {
            PropagationRule::ForeignKey {
                fk_column,
                referenced_table,
                referenced_column,
                trigger_state,
                target_state,
                ..
            } => {
                24 + fk_column.len() * 16
                    + referenced_table.len() * 16
                    + referenced_column.len() * 16
                    + trigger_state.len() * 16
                    + target_state.len() * 16
            }
            PropagationRule::Edge {
                edge_type,
                trigger_state,
                target_state,
                ..
            } => 24 + edge_type.len() * 16 + trigger_state.len() * 16 + target_state.len() * 16,
            PropagationRule::VectorExclusion { trigger_state } => 16 + trigger_state.len() * 16,
        }
    }
}

impl StateMachineConstraint {
    fn estimated_bytes(&self) -> usize {
        let transitions_bytes = self.transitions.iter().fold(0usize, |acc, (from, tos)| {
            acc.saturating_add(
                32 + from.len() * 16 + tos.iter().map(|to| 16 + to.len() * 16).sum::<usize>(),
            )
        });
        24 + self.column.len() * 16 + transitions_bytes
    }
}

impl ColumnDef {
    fn estimated_bytes(&self) -> usize {
        let default_bytes = self
            .default
            .as_ref()
            .map(|value| 32 + value.len() * 16)
            .unwrap_or(0);
        let reference_bytes = self
            .references
            .as_ref()
            .map(|reference| 32 + reference.table.len() * 16 + reference.column.len() * 16)
            .unwrap_or(0);
        let rank_policy_bytes = self
            .rank_policy
            .as_ref()
            .map(|policy| {
                40 + policy.joined_table.len() * 16
                    + policy.joined_column.len() * 16
                    + policy.anchor_column.len() * 16
                    + policy.sort_key.len() * 16
                    + policy.formula.len() * 16
                    + policy.protected_index.len() * 16
            })
            .unwrap_or(0);
        let scope_label_bytes = self
            .scope_label
            .as_ref()
            .map(|kind| match kind {
                ScopeLabelKind::Simple { write_labels } => {
                    24 + write_labels
                        .iter()
                        .map(|s| 16 + s.len() * 16)
                        .sum::<usize>()
                }
                ScopeLabelKind::Split {
                    read_labels,
                    write_labels,
                } => {
                    40 + read_labels.iter().map(|s| 16 + s.len() * 16).sum::<usize>()
                        + write_labels
                            .iter()
                            .map(|s| 16 + s.len() * 16)
                            .sum::<usize>()
                }
            })
            .unwrap_or(0);
        let acl_ref_bytes = self
            .acl_ref
            .as_ref()
            .map(|acl| 32 + acl.ref_table.len() * 16 + acl.ref_column.len() * 16)
            .unwrap_or(0);
        8 + self.name.len() * 16
            + self.column_type.estimated_bytes()
            + default_bytes
            + reference_bytes
            + rank_policy_bytes
            + scope_label_bytes
            + acl_ref_bytes
            + 8
    }
}

impl ColumnType {
    fn estimated_bytes(&self) -> usize {
        match self {
            ColumnType::Integer => 16,
            ColumnType::Real => 16,
            ColumnType::Text => 16,
            ColumnType::Boolean => 16,
            ColumnType::Json => 24,
            ColumnType::Uuid => 16,
            ColumnType::Vector(_) => 24,
            ColumnType::Timestamp => 16,
            ColumnType::TxId => 8,
        }
    }
}
