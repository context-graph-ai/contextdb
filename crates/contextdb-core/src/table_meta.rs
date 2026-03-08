use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct TableMeta {
    pub columns: Vec<ColumnDef>,
    pub immutable: bool,
    pub state_machine: Option<StateMachineConstraint>,
}

#[derive(Debug, Clone)]
pub struct StateMachineConstraint {
    pub column: String,
    pub transitions: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub column_type: ColumnType,
    pub nullable: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    Integer,
    Real,
    Text,
    Boolean,
    Json,
    Uuid,
    Vector(usize),
}
