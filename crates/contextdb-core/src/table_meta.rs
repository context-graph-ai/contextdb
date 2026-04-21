use crate::Direction;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
}

// Custom `Deserialize` that tolerates prior on-disk schemas missing the
// `immutable` field (backward-compat, I5). Attempts to deserialize the full
// nine-field struct; on sequence truncation at the trailing `immutable`
// position, defaults it to `false`. JSON / other formats that distinguish
// "missing field" from "required field" continue to work via `serde(default)`
// on the field itself.
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
                // Trailing field: tolerate bincode-style premature EOF by defaulting
                // to `false` when reading the next element produces an error that
                // indicates the buffer ended short of the expected bool tag.
                let immutable = match seq.next_element::<bool>() {
                    Ok(Some(v)) => v,
                    Ok(None) => false,
                    Err(_) => false,
                };
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
        ];
        deserializer.deserialize_struct("ColumnDef", FIELDS, ColumnDefVisitor)
    }
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

        16 + columns_bytes
            + state_machine_bytes
            + dag_bytes
            + unique_constraint_bytes
            + natural_key_bytes
            + propagation_bytes
            + expires_bytes
            + self.default_ttl_seconds.map(|_| 8).unwrap_or(0)
            + 8
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
        8 + self.name.len() * 16
            + self.column_type.estimated_bytes()
            + default_bytes
            + reference_bytes
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
