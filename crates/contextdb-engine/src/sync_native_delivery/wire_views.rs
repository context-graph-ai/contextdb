//! Original record wire fields with prepaid JSON text instead of Value's
//! allocating json_as_string serializer. Every other field borrows its record.
use super::{Counter, DecodeError};
use crate::custody::records::{DiagnosticBlock, RowRef, TerminalRecord};
use crate::read_image_memory::{NativeDeliveryBytes, NativeDeliveryImage};
use crate::sync_types::{Conflict, NaturalKey, RefusalCause};
use contextdb_core::Value;
use serde::{Serialize, ser::SerializeSeq};
use std::cell::Cell;

fn key_values(key: &NaturalKey) -> impl Iterator<Item = &Value> {
    std::iter::once(&key.value).chain(key.rest.iter().map(|(_, value)| value))
}
fn values<'a>(
    terminal: &'a TerminalRecord,
    block: &'a DiagnosticBlock,
) -> impl Iterator<Item = &'a Value> {
    key_values(&terminal.root.key)
        .chain(
            block
                .subjects
                .iter()
                .flat_map(|(row, _)| key_values(&row.key)),
        )
        .chain(block.conflicts.iter().flat_map(|conflict| {
            key_values(&conflict.natural_key).chain(
                conflict
                    .refusal_cause
                    .iter()
                    .flat_map(|cause| key_values(&cause.natural_key)),
            )
        }))
        .filter(|value| matches!(value, Value::Json(_)))
}
fn json_length(value: &serde_json::Value) -> Result<usize, DecodeError> {
    let mut count = Counter::default();
    // Direct JSON value serialization, never core Value::json_as_string.
    serde_json::to_writer(&mut count, value)
        .map_err(|_| DecodeError::Malformed("JSON text size failure"))?;
    Ok(count.0)
}
pub(super) fn prepare_json(
    memory: &NativeDeliveryImage,
    terminal: &TerminalRecord,
    block: &DiagnosticBlock,
) -> Result<NativeDeliveryBytes, DecodeError> {
    let mut length = 0usize;
    for value in values(terminal, block) {
        memory.checkpoint()?;
        let Value::Json(json) = value else {
            unreachable!()
        };
        length = length
            .checked_add(json_length(json)?)
            .ok_or(DecodeError::Malformed("JSON text size overflow"))?;
    }
    let mut bytes = memory.allocate(length)?;
    let mut destination = bytes.as_mut_slice();
    for value in values(terminal, block) {
        memory.checkpoint()?;
        let Value::Json(json) = value else {
            unreachable!()
        };
        serde_json::to_writer(&mut destination, json)
            .map_err(|_| DecodeError::Malformed("JSON text encode failure"))?;
    }
    if !destination.is_empty() {
        return Err(DecodeError::Malformed("JSON text size changed"));
    }
    Ok(bytes)
}
pub(super) struct Context<'a> {
    pub(super) terminal: &'a TerminalRecord,
    pub(super) block: &'a DiagnosticBlock,
    json: &'a [u8],
    json_at: Cell<usize>,
}
impl<'a> Context<'a> {
    pub(super) fn new(
        terminal: &'a TerminalRecord,
        block: &'a DiagnosticBlock,
        json: &'a [u8],
    ) -> Self {
        Self {
            terminal,
            block,
            json,
            json_at: Cell::new(0),
        }
    }
    pub(super) fn reset_json_pass(&self) {
        self.json_at.set(0);
    }
    pub(super) fn finish_json_pass(&self) -> Result<(), DecodeError> {
        if self.json_at.get() != self.json.len() {
            return Err(DecodeError::Malformed("JSON traversal changed"));
        }
        Ok(())
    }
    fn json_text(&self, requested: &Value) -> &str {
        let Value::Json(json) = requested else {
            unreachable!()
        };
        // ValueView visits root, subjects, then conflicts/refusal causes in
        // exactly values() order. Measure only this value, once per wire pass;
        // immutable inputs and prepare_json's checked total bound the cursor.
        let length = json_length(json).expect("precounted JSON text");
        let at = self.json_at.get();
        let end = at.checked_add(length).expect("precounted JSON text total");
        self.json_at.set(end);
        std::str::from_utf8(&self.json[at..end]).expect("JSON writer emits UTF-8")
    }
}
struct ValueView<'a> {
    value: &'a Value,
    context: &'a Context<'a>,
}
impl Serialize for ValueView<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.value {
            Value::Null => serializer.serialize_unit_variant("Value", 0, "Null"),
            Value::Bool(value) => serializer.serialize_newtype_variant("Value", 1, "Bool", value),
            Value::Int64(value) => serializer.serialize_newtype_variant("Value", 2, "Int64", value),
            Value::Float64(value) => {
                serializer.serialize_newtype_variant("Value", 3, "Float64", value)
            }
            Value::Text(value) => serializer.serialize_newtype_variant("Value", 4, "Text", value),
            Value::Uuid(value) => serializer.serialize_newtype_variant("Value", 5, "Uuid", value),
            Value::Timestamp(value) => {
                serializer.serialize_newtype_variant("Value", 6, "Timestamp", value)
            }
            Value::Json(_) => serializer.serialize_newtype_variant(
                "Value",
                7,
                "Json",
                self.context.json_text(self.value),
            ),
            Value::Vector(value) => {
                serializer.serialize_newtype_variant("Value", 8, "Vector", value)
            }
            Value::TxId(value) => serializer.serialize_newtype_variant("Value", 9, "TxId", value),
        }
    }
}
#[derive(Serialize)]
struct KeyView<'a> {
    column: &'a str,
    value: ValueView<'a>,
    rest: KeyRest<'a>,
}
struct KeyRest<'a> {
    key: &'a NaturalKey,
    context: &'a Context<'a>,
}
impl Serialize for KeyRest<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.key.rest.len()))?;
        for (column, value) in &self.key.rest {
            seq.serialize_element(&(
                column,
                ValueView {
                    value,
                    context: self.context,
                },
            ))?;
        }
        seq.end()
    }
}
fn key<'a>(key: &'a NaturalKey, context: &'a Context<'a>) -> KeyView<'a> {
    KeyView {
        column: &key.column,
        value: ValueView {
            value: &key.value,
            context,
        },
        rest: KeyRest { key, context },
    }
}
#[derive(Serialize)]
struct RowView<'a> {
    table: &'a str,
    key: KeyView<'a>,
}
fn row<'a>(row: &'a RowRef, context: &'a Context<'a>) -> RowView<'a> {
    RowView {
        table: &row.table,
        key: key(&row.key, context),
    }
}
#[derive(Serialize)]
struct CauseView<'a> {
    table: &'a str,
    natural_key: KeyView<'a>,
}
fn cause<'a>(cause: &'a RefusalCause, context: &'a Context<'a>) -> CauseView<'a> {
    CauseView {
        table: &cause.table,
        natural_key: key(&cause.natural_key, context),
    }
}
#[derive(Serialize)]
struct ConflictView<'a> {
    natural_key: KeyView<'a>,
    resolution: &'a crate::sync_types::ConflictPolicy,
    reason: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    table: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mutation_kind: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    winning_author_node_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hub_acceptance_position: Option<contextdb_core::Lsn>,
    #[serde(skip_serializing_if = "Option::is_none")]
    refusal_cause: Option<CauseView<'a>>,
}
fn conflict<'a>(conflict: &'a Conflict, context: &'a Context<'a>) -> ConflictView<'a> {
    ConflictView {
        natural_key: key(&conflict.natural_key, context),
        resolution: &conflict.resolution,
        reason: conflict.reason.as_deref(),
        table: conflict.table.as_deref(),
        mutation_kind: conflict.mutation_kind.as_deref(),
        winning_author_node_id: conflict.winning_author_node_id.as_deref(),
        hub_acceptance_position: conflict.hub_acceptance_position,
        refusal_cause: conflict
            .refusal_cause
            .as_ref()
            .map(|value| cause(value, context)),
    }
}
struct Subjects<'a>(&'a Context<'a>);
impl Serialize for Subjects<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.block.subjects.len()))?;
        for (subject, mode) in &self.0.block.subjects {
            seq.serialize_element(&(row(subject, self.0), mode))?;
        }
        seq.end()
    }
}
struct Conflicts<'a>(&'a Context<'a>);
impl Serialize for Conflicts<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.block.conflicts.len()))?;
        for value in &self.0.block.conflicts {
            seq.serialize_element(&conflict(value, self.0))?;
        }
        seq.end()
    }
}
#[derive(Serialize)]
pub(super) struct BlockView<'a> {
    salt: &'a [u8; 32],
    subjects: Subjects<'a>,
    conflicts: Conflicts<'a>,
}
pub(super) fn block<'a>(context: &'a Context<'a>) -> BlockView<'a> {
    BlockView {
        salt: &context.block.salt,
        subjects: Subjects(context),
        conflicts: Conflicts(context),
    }
}
#[derive(Serialize)]
pub(super) struct TerminalView<'a> {
    namespace: &'a crate::custody::records::Namespace,
    root: RowView<'a>,
    origin_life_digest: &'a [u8; 32],
    submission: &'a uuid::Uuid,
    seal_digest: &'a [u8; 32],
    unit_digest: &'a [u8; 32],
    source: &'a (contextdb_core::Lsn, u32),
    kind: u8,
    cause: Option<&'a str>,
    position: &'a (contextdb_core::Lsn, u32),
    state: &'a crate::custody::records::StateRef,
    diagnostics: &'a crate::custody::records::DiagnosticClaim,
    signature: &'a [u8],
}
pub(super) fn terminal<'a>(context: &'a Context<'a>) -> TerminalView<'a> {
    let terminal = context.terminal;
    TerminalView {
        namespace: &terminal.namespace,
        root: row(&terminal.root, context),
        origin_life_digest: &terminal.origin_life_digest,
        submission: &terminal.submission,
        seal_digest: &terminal.seal_digest,
        unit_digest: &terminal.unit_digest,
        source: &terminal.source,
        kind: terminal.kind,
        cause: terminal.cause.as_deref(),
        position: &terminal.position,
        state: &terminal.state,
        diagnostics: &terminal.diagnostics,
        signature: &terminal.signature,
    }
}
