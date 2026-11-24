//! Membership lists share the removal transaction's temporary records.
use super::expression::{EvalContext, among};
use crate::memory::{Budget, Reservation};
use crate::sql::Expr;
use crate::store::{ChangeSet, Key, PART_BYTES, SET, put_value, read_value, value_len};
use crate::types::{Error, Result, Value, heap_bytes};
use std::borrow::Cow;
use std::cell::{RefCell, RefMut};
use std::ops::Bound::{Excluded, Included};

// Planning can keep collect's value copy, search's key tuples and encoded keys beside
// the retained list (table::collect and search::Scope::keys): three more representations.
const PLANNING_COPIES: u64 = 3;

#[derive(Clone, Copy)]
pub(crate) struct SetContext<'a> {
    pub(crate) changes: &'a dyn SetSlot,
    pub(crate) budget: &'a Budget,
}

pub(crate) trait SetSlot {
    fn loan(&self) -> RefMut<'_, ChangeSet>;
}
impl SetSlot for RefCell<&mut ChangeSet> {
    fn loan(&self) -> RefMut<'_, ChangeSet> {
        RefMut::map(self.borrow_mut(), |changes| &mut **changes)
    }
}

pub(crate) enum List {
    Memory(Vec<Value>, u64),
    Stored(Stored),
}

#[derive(Clone, Copy)]
pub(crate) struct Stored {
    at: usize,
    kinds: u16,
    count: u64,
}

impl List {
    pub(super) fn small(&self) -> Result<&[Value]> {
        match self {
            Self::Memory(values, _) => Ok(values),
            Self::Stored(_) => Err(Error::PlanError("stored list needs a reader".into())),
        }
    }

    pub(super) fn push(
        &mut self,
        value: Value,
        at: usize,
        ctx: Option<SetContext<'_>>,
        held: &mut Reservation,
    ) -> Result<()> {
        if let Self::Memory(values, bytes) = self {
            let incoming = heap_bytes(&value);
            let over = held.bytes().saturating_mul(1 + PLANNING_COPIES) > PART_BYTES;
            if let Some(ctx) = ctx.filter(|_| over) {
                let mut stored = Stored {
                    at,
                    kinds: 0,
                    count: 0,
                };
                for old in values.iter() {
                    stored.put(old, ctx)?;
                }
                let freed = *bytes + (values.capacity() * size_of::<Value>()) as u64;
                *self = Self::Stored(stored);
                held.shrink(freed);
            } else {
                held.room(values, usize::MAX)?;
                values.push(value);
                *bytes += incoming;
                return Ok(());
            }
        }
        if let Self::Stored(stored) = self {
            stored.put(&value, ctx.expect("stored list context"))?;
            held.shrink(heap_bytes(&value));
        }
        Ok(())
    }
}

fn canonical_key(value: &Value) -> Result<Key> {
    match value {
        Value::Float64(n)
            if n.fract() == 0.0
                && (*n as i128) >= i128::from(i64::MIN)
                && (*n as i128) <= i128::from(i64::MAX) =>
        {
            Key::from_values(&[Value::Int64(*n as i64)], &[])
        }
        _ => Key::from_values(std::slice::from_ref(value), &[]),
    }
}

impl Stored {
    fn put(&mut self, value: &Value, ctx: SetContext<'_>) -> Result<()> {
        let _room = ctx
            .budget
            .reserve((value_len(value) * 4 + 64) as u64, "stored set")?;
        let mut key = self.at.to_be_bytes().to_vec();
        match value {
            Value::Null => key.push(0),
            Value::Json(_) | Value::Vector(_) => {
                key.push(if matches!(value, Value::Json(_)) {
                    8
                } else {
                    9
                });
                key.extend(self.count.to_be_bytes());
            }
            _ => key.extend(canonical_key(value)?.as_bytes()),
        }
        self.kinds |= 1 << key[size_of::<usize>()];
        self.count += 1;
        let mut bytes = Vec::with_capacity(value_len(value));
        put_value(&mut bytes, value);
        ctx.changes.loan().put_temp(SET, &key, &bytes)
    }

    pub(super) fn values<'a>(
        &self,
        tag: u8,
        ctx: SetContext<'a>,
    ) -> impl Iterator<Item = Result<Element<'a>>> + 'a {
        let prefix = [self.at.to_be_bytes().as_slice(), &[tag]].concat();
        let end = Key::from_bytes(&prefix)
            .prefix_end()
            .expect("set prefix end");
        let mut after: Option<(Vec<u8>, Reservation)> = None;
        std::iter::from_fn(move || {
            let from = after
                .as_ref()
                .map_or(Included(prefix.as_slice()), |(key, _)| {
                    Excluded(key.as_slice())
                });
            let record =
                ctx.changes
                    .loan()
                    .next_temp(SET, (from, Excluded(end.as_bytes())), ctx.budget);
            let next = (|| {
                let Some(((key, bytes), mut held)) = record? else {
                    return Ok(None);
                };
                let value = read_value(&mut bytes.as_slice(), &mut held)?
                    .ok_or_else(|| Error::PlanError("invalid temporary membership value".into()))?;
                let key_room = ctx.budget.reserve(key.len() as u64, "membership place")?;
                after = Some((key, key_room));
                Ok(Some(Element {
                    value: [Cow::Owned(value)],
                    _held: held,
                }))
            })();
            next.transpose()
        })
    }

    pub(super) fn tags(&self) -> impl Iterator<Item = u8> {
        let kinds = self.kinds;
        (0..=9).filter(move |tag| kinds & (1 << tag) != 0)
    }

    pub(super) fn member(
        &self,
        left: Cow<'_, Value>,
        negated: bool,
        expr: &[Expr],
        eval: &EvalContext<'_>,
    ) -> Result<Value> {
        let ctx = eval.sets.expect("set context");
        if *left == Value::Null {
            return Ok(Value::Null);
        }
        let keyed = !matches!(*left, Value::Json(_) | Value::Vector(_));
        let bytes = if keyed { value_len(&left) * 4 + 64 } else { 0 };
        let _room = ctx.budget.reserve(bytes as u64, "set key")?;
        let own = keyed.then(|| canonical_key(&left)).transpose()?;
        if let Some(own) = &own {
            let mut key = self.at.to_be_bytes().to_vec();
            key.extend(own.as_bytes());
            if ctx
                .changes
                .loan()
                .next_temp(SET, (Included(&key), Included(&key)), ctx.budget)?
                .is_some()
            {
                return Ok(Value::Bool(!negated));
            }
        }
        let skip = match own.as_ref().map(|key| key.as_bytes()[0]) {
            Some(2 | 3) => 12,
            Some(tag) => 1 << tag,
            None => 0,
        };
        let items = self
            .tags()
            .filter(|tag| skip & (1 << tag) == 0)
            .flat_map(|tag| self.values(tag, ctx));
        among(
            &[left],
            items.map(|e| e.map(|e| (None, e))),
            negated,
            expr,
            eval,
        )
    }
}

pub(super) struct Element<'a> {
    pub(super) value: [Cow<'a, Value>; 1],
    _held: Reservation,
}
impl<'a> AsRef<[Cow<'a, Value>]> for Element<'a> {
    fn as_ref(&self) -> &[Cow<'a, Value>] {
        &self.value
    }
}
