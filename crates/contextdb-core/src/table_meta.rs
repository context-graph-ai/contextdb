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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
        }
    }
}
