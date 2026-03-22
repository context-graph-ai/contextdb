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
    pub natural_key_column: Option<String>,
    #[serde(default)]
    pub propagation_rules: Vec<PropagationRule>,
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
}
