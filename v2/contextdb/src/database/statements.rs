//! What each form of statement cost, in memory and never in the store: the record
//! `SHOW STATEMENT_STATISTICS` lists, most work first.

use std::cmp::Reverse;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use super::locked;
use crate::memory::{Budget, Reservation};
use crate::query::{Push, Work};
use crate::sql::ast::Show;
use crate::types::{Result, Value};

/// The most bytes of a form the record keeps.
const KEPT_TEXT: usize = 2048;

/// The forms run, by a hash of each, and how many forms are kept.
pub(crate) struct Statements {
    lines: Mutex<HashMap<u64, Line>>,
    pub(crate) limit: AtomicU64,
    budget: Budget,
}

/// One form: its text; its calls, then its rows, rows examined and distance evaluations in all
/// and the most of each in one run; the index its latest run left unused, with why; and the
/// line's charge.
type Line = (
    Box<str>,
    [u64; 7],
    Option<(String, &'static str)>,
    Reservation,
);

/// Drops the least called forms until no more than `room` are left.
fn evict(lines: &mut HashMap<u64, Line>, room: usize) {
    while lines.len() > room
        && let Some((&fewest, _)) = lines.iter().min_by_key(|(_, line)| line.1[0])
    {
        lines.remove(&fewest);
    }
}

impl Statements {
    pub(crate) fn new(budget: &Budget, limit: Option<u64>) -> Self {
        let limit = AtomicU64::new(limit.unwrap_or(crate::settings::SETTINGS[6].2));
        let (lines, budget) = (Mutex::default(), budget.clone());
        Self {
            lines,
            limit,
            budget,
        }
    }

    /// Sets how many forms are kept, and drops the least called of those over it now.
    pub(crate) fn set_limit(&self, limit: u64) {
        self.limit.store(limit, Relaxed);
        evict(&mut locked(&self.lines), limit as usize);
    }

    /// Adds a statement's run as its own `Work` counted it, refused part-way or not, with the
    /// rows a write changed.
    pub(crate) fn ran(&self, form: Option<&str>, work: &Work, affected: u64) {
        let mut run = work.counts();
        run[0] += affected;
        self.add(form, true, run, run, work.unused.take());
    }

    /// Adds a run of `form` that is a call when `call`; `so_far` is the call's counts so far,
    /// which a cursor's pages add up to. A form the record has no room for is not kept.
    pub(crate) fn add(
        &self,
        form: Option<&str>,
        call: bool,
        run: [u64; 3],
        so_far: [u64; 3],
        unused: Option<(String, &'static str)>,
    ) {
        let (Some(form), limit) = (form, self.limit.load(Relaxed) as usize) else {
            return;
        };
        let mut hash = DefaultHasher::new();
        form.hash(&mut hash);
        let (hash, mut lines) = (hash.finish(), locked(&self.lines));
        if !lines.contains_key(&hash) {
            evict(&mut lines, limit.saturating_sub(1));
            let end = (0..=form.len().min(KEPT_TEXT)).rfind(|&at| form.is_char_boundary(at));
            let text: Box<str> = form[..end.unwrap_or(0)].into();
            let bytes = (size_of::<Line>() + text.len()) as u64;
            let held = self.budget.reserve(bytes, "statement statistics");
            let (Ok(held), true) = (held, limit > 0) else {
                return;
            };
            lines.insert(hash, (text, [0; 7], None, held));
        }
        let (_, numbers, kept, _) = lines.get_mut(&hash).expect("the line of the form");
        numbers[0] += u64::from(call);
        for at in 0..3 {
            numbers[1 + at] += run[at];
            numbers[4 + at] = numbers[4 + at].max(so_far[at]);
        }
        *kept = unused.or(kept.take().filter(|_| !call));
    }

    /// The forms as rows of the report, most work first, from its offset and at most its limit.
    pub(crate) fn rows(&self, show: &Show, push: Push<'_>) -> Result<()> {
        let lines = locked(&self.lines);
        let mut lines: Vec<&Line> = lines.values().collect();
        lines.sort_by_key(|line| (Reverse(line.1[2] + line.1[3]), &line.0));
        let shown = lines.into_iter().skip(show.offset as usize);
        let mut shown = shown.take(show.limit.map_or(usize::MAX, |limit| limit as usize));
        shown.try_for_each(|(text, numbers, unused, _)| {
            let unused = unused
                .as_ref()
                .map(|(index, why)| format!("{index}: {why}"));
            let numbers = numbers.map(|n| Value::Int64(n as i64));
            let row = [Value::Text(text.to_string())].into_iter().chain(numbers);
            push(
                row.chain([unused.map_or(Value::Null, Value::Text)])
                    .collect(),
            )
        })
    }

    pub(crate) fn reset(&self) {
        locked(&self.lines).clear();
    }
}
