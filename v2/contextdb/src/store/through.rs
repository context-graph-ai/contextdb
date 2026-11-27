//! Parts of one storage transaction and reads of its complete private view.
use super::*;

pub(super) const NOTES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("notes");
pub(super) const SETS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("sets");
/// A key's note (`N`) is empty when taken, `WRITTEN` once its row first leaves in a part (counted,
/// its version decided), `ERASED` when that removes a row whose removed versions are not kept.
pub(super) const WRITTEN: u8 = 1;
pub(super) const ERASED: u8 = 2;

impl Store {
    pub(crate) fn through(&self, changes: &mut ChangeSet) {
        let HeldFor::Writing { data, .. } = &self.held else {
            unreachable!()
        };
        (changes.data, changes.first) = (Some(data.clone()), Some(changes.base.clone()));
    }
}

impl ChangeSet {
    pub(crate) fn first(&self) -> &View {
        self.first.as_ref().unwrap_or(&self.base)
    }
    pub(crate) fn streaming(&self) -> bool {
        self.data.is_some()
    }
    /// Ends a part through `part`, when one is given, once the set holds `PART_BYTES`. A part is
    /// given only at a row boundary of a set written through, where every rewind point of the
    /// rows before has expired (a callback's statement has none), so their undo goes.
    pub(crate) fn part_full(
        &mut self,
        part: Option<impl FnOnce(&mut Self) -> Result<()>>,
    ) -> Result<()> {
        let Some(part) = part else { return Ok(()) };
        self.calls.clear();
        match self.reserved.bytes() >= PART_BYTES {
            true => part(self),
            false => Ok(()),
        }
    }
    pub(crate) fn changed_tables(&self) -> Box<dyn Iterator<Item = (TableId, u64)> + '_> {
        match self.data {
            Some(_) => Box::new(self.tally.iter().map(|(&id, &n)| (id, n))),
            None => Box::new(
                self.rows
                    .iter()
                    .filter(|(_, r)| !r.is_empty())
                    .map(|(&id, r)| (TableId(id), r.len() as u64)),
            ),
        }
    }
    pub(super) fn remember(&mut self, (undo, bytes): (Undo, u64)) -> Result<()> {
        if self.data.is_none() {
            self.calls.push((undo, bytes));
            return Ok(());
        }
        self.written |= !matches!(&undo, Undo::Vector(k, _) if k.0[0] >= NOTE);
        // A part's rows are undone as in memory; derived records leave with each full part.
        match undo {
            Undo::Vector(..) | Undo::Number(..) => self.derived += bytes,
            undo => {
                // Its rows are counted as they leave their notes (`disk::put`).
                if let Undo::Row(id, ..) = undo {
                    self.tallied(TableId(id))?;
                }
                self.calls.push((undo, bytes));
            }
        }
        if self.derived == 0 || self.reserved.bytes() < PART_BYTES {
            return Ok(());
        }
        self.put_part(false)
    }
    // Giving out the transaction drops the view of it first: nothing holds a page during a write.
    pub(super) fn writing(&mut self) -> Stored<WriteTransaction> {
        self.base = self.first().clone();
        match self.write.take() {
            Some(write) => Ok(write),
            None => disk::begin(
                self.data.as_ref().expect("a write-through change set"),
                self,
            ),
        }
    }
    pub(crate) fn put_part(&mut self, rows: bool) -> Result<()> {
        let put = (|| -> Stored<WriteTransaction> {
            let write = self.writing()?;
            match rows {
                true => disk::put(&write, self)?,
                false => disk::put_derived(&write, self)?,
            }
            Ok(write)
        })();
        let write = put.map_err(|e| disk::failed(self.first().folder(), "put", e))?;
        self.reserved.shrink(self.derived);
        self.derived = 0;
        self.vectors.clear();
        self.numbers.clear();
        if rows {
            self.rows.clear();
            self.index.clear();
            self.counts.clear();
            self.kept.clear();
            self.schema.clear();
            self.dropped = Vec::new();
            self.copied = Vec::new();
            self.calls.clear();
            let drain = u64::from(self.drain) * super::DRAIN_BYTES as u64;
            self.reserved
                .shrink(self.reserved.bytes().saturating_sub(drain));
        }
        let first = &self.first().moment;
        let moment = Moment::open(
            Tables::Write(&write, first),
            first.folder.clone(),
            None,
            first._pin.clone(),
        );
        let moment = moment.map_err(|e| disk::failed(self.first().folder(), "put", e))?;
        (self.base, self.write) = (
            View {
                moment: Arc::new(moment),
                read: None,
            },
            Some(write),
        );
        Ok(())
    }
    /// In memory, a note is the statement's workspace: no undo, one charge, given back at its end.
    pub(crate) fn put_temp(&mut self, kind: u8, key: &[u8], value: &[u8]) -> Result<()> {
        let entry = STAGED_BYTES + 1 + key.len() as u64;
        let key = Key([&[kind], key].concat());
        if self.data.is_none() {
            self.reserved.grow(entry + value.len() as u64)?;
            let before = self.vectors.insert(key, Some(value.to_vec())).flatten();
            self.reserved
                .shrink(before.map_or(0, |before| entry + before.len() as u64));
            return Ok(());
        }
        let bytes = self.counted(&key, value.len() as u64)?;
        self.stage_record(key, Some(value.to_vec()), bytes)
    }
    /// Gives back, as every statement ends, the notes a set held in memory keeps from [`NOTE`].
    pub(crate) fn end_statement(&mut self) {
        if self.data.is_none() {
            let done = self.vectors.split_off(&Key(vec![NOTE]));
            let bytes = done.iter().map(|(key, value)| {
                let value = value.as_ref().map_or(0, Vec::len);
                STAGED_BYTES + (key.0.len() + value) as u64
            });
            self.reserved.shrink(bytes.sum());
        }
    }
    pub(crate) fn next_temp(
        &self,
        kind: u8,
        (from, to): (Bound<&[u8]>, Bound<&[u8]>),
        budget: &Budget,
    ) -> Result<Option<SetRecord>> {
        let framed = |bound: Bound<&[u8]>, edge| match bound {
            Unbounded => edge,
            bound => bound.map(|k| Key([&[kind], k].concat())),
        };
        let from = framed(from, Included(Key(vec![kind])));
        let to = framed(to, Excluded(Key(vec![kind + 1])));
        let staged = self
            .vectors
            .range::<Key, _>((from.as_ref(), to.as_ref()))
            .find_map(|(k, v)| v.as_ref().map(|v| (k.0.as_slice(), v.as_slice())));
        let read = || -> Stored<_> {
            let table = self
                .write
                .as_ref()
                .map(|w| w.peek_table(if kind == SET { SETS } else { NOTES }))
                .transpose()?;
            let bounds = (
                from.as_ref().map(Key::as_bytes),
                to.as_ref().map(Key::as_bytes),
            );
            let mut rows = table
                .as_ref()
                .map(|t| t.range::<&[u8]>(bounds))
                .transpose()?;
            let stored = rows.as_mut().and_then(|r| r.next()).transpose()?;
            let stored = stored.as_ref().map(|(k, v)| (k.value(), v.value()));
            let picked = match (staged, stored) {
                (Some(a), Some(b)) => Some(if a.0 <= b.0 { a } else { b }),
                (a, b) => a.or(b),
            };
            Ok(picked
                .map(|(key, value)| {
                    let held = budget
                        .reserve((key.len() + value.len() + 48) as u64, "temporary record")?;
                    Ok(((key[1..].to_vec(), value.to_vec()), held))
                })
                .transpose())
        };
        read().map_err(|e| disk::failed(self.base.folder(), "read", e))?
    }
    /// The first key from `from` of `table` noted under `kind`: `N` a taken key, `V` a key
    /// whose vectors alone an erase takes.
    pub(crate) fn noted_from(
        &self,
        kind: u8,
        table: TableId,
        from: &Key,
        budget: &Budget,
    ) -> Result<Option<(Key, Reservation)>> {
        let _seek = budget.reserve((from.0.len() + 10) as u64, "note seek")?;
        let prefix = [&[kind][..], &table.0.to_be_bytes()].concat();
        let from = [prefix.as_slice(), &from.0].concat();
        Ok(self
            .next_temp(NOTE, (Included(&from), Unbounded), budget)?
            .filter(|((key, _), _)| key.starts_with(&prefix))
            .map(|((mut key, _), held)| {
                key.drain(..5);
                (Key(key), held)
            }))
    }
    pub(crate) fn has_note(&self, kind: u8, table: TableId, budget: &Budget) -> Result<bool> {
        let from = [&[kind][..], &table.0.to_be_bytes()].concat();
        let end = match table.0.checked_add(1) {
            Some(next) => [&[kind][..], &next.to_be_bytes()].concat(),
            None => vec![kind + 1],
        };
        Ok(self
            .next_temp(NOTE, (Included(&from), Excluded(&end)), budget)?
            .is_some())
    }
    pub(crate) fn noted(
        &self,
        kind: u8,
        table: TableId,
        key: &Key,
        budget: &Budget,
    ) -> Result<bool> {
        let _held = budget.reserve((key.0.len() + 5) as u64, "note probe")?;
        let key = [&[kind][..], &keys::stored(table.0, &key.0)].concat();
        Ok(self
            .next_temp(NOTE, (Included(&key), Included(&key)), budget)?
            .is_some())
    }
    /// One note or follower, decoded with the framing used to stage it.
    pub(crate) fn next_note(
        &self,
        kind: u8,
        after: &mut Vec<u8>,
        budget: &Budget,
    ) -> Result<Option<(TableId, Key, Reservation)>> {
        let end = [kind + 1];
        let Some(((key, value), held)) =
            self.next_temp(NOTE, (Excluded(after.as_slice()), Excluded(&end)), budget)?
        else {
            return Ok(None);
        };
        let bytes = if kind == b'N' { &key[1..] } else { &value };
        let id = TableId(u32::from_be_bytes(bytes[..4].try_into().unwrap()));
        let row = Key::from_bytes(&bytes[4..]);
        *after = key;
        Ok(Some((id, row, held)))
    }
    /// Counts a table among those the commit notice names, charging its summary once.
    fn tallied(&mut self, table: TableId) -> Result<()> {
        if !self.tally.contains_key(&table) {
            self.held.grow(384)?;
            self.tally.insert(table, 0);
        }
        Ok(())
    }
    /// Notes a key: `N` one the statement took, `V` one whose vectors and deliveries alone go,
    /// `M` the values a follower reference names. A `V` note names its table among those the
    /// commit changed, by no row, so the commit reaches what it notes.
    pub(crate) fn note(&mut self, kind: u8, table: TableId, key: &Key) -> Result<()> {
        let note = [&[kind][..], &keys::stored(table.0, &key.0)].concat();
        self.put_temp(NOTE, &note, &[])?;
        if kind == b'V' {
            self.tallied(table)?;
            self.written = true;
        }
        Ok(())
    }
    pub(crate) fn queue(&mut self, kind: u8, table: TableId, key: &Key) -> Result<()> {
        self.queued += 1;
        let at = [&[kind][..], &self.queued.to_be_bytes()].concat();
        self.put_temp(NOTE, &at, &keys::stored(table.0, &key.0))
    }
    /// The next written key after `after` of a table `wanted` admits, with its row as `view`
    /// reads it: `None` for a row the set removed.
    pub(crate) fn next_written(
        &self,
        view: &dyn Read,
        after: &mut Vec<u8>,
        wanted: &dyn Fn(TableId) -> bool,
        (budget, cover): (&Budget, &mut Passing),
    ) -> Result<Option<(TableId, Key, Option<Row>)>> {
        while let Some((id, key, _held)) = self.next_note(b'N', after, budget)? {
            if wanted(id) {
                let row = view.row(id, &key, cover)?;
                return Ok(Some((id, key, row)));
            }
        }
        Ok(None)
    }
}

impl Read for ChangeSet {
    fn committed(&self) -> &View {
        &self.base
    }
    fn setting(&self, at: usize) -> Result<Option<u64>> {
        self.base.setting(at)
    }
    fn valid(&self) -> Result<()> {
        self.base.valid()
    }
    fn commit(&self) -> TxId {
        self.base.commit()
    }

    fn row(&self, table: TableId, key: &Key, cover: &mut Passing) -> Result<Option<Row>> {
        let row = match self.rows.get(&table.0).and_then(|staged| staged.get(key)) {
            Some(staged) => shown(staged, cover),
            None => Ok(self
                .base
                .row(table, key, cover)?
                .map(|row| self.shaped(table, row))),
        }?;
        self.rows_read
            .set(self.rows_read.get() + u64::from(row.is_some()));
        Ok(row)
    }

    fn rows<'a>(
        &'a self,
        table: TableId,
        from: Bound<&Key>,
        to: Bound<&Key>,
        back: bool,
        cover: &'a mut Passing,
    ) -> Result<RowIter<'a>> {
        let span = keys::span(from, to);
        let stored = self.base.moment.rows(table.0, &span, back, cover)?;
        let staged = staged_in(&self.rows, table.0, span, back);
        let mut staged_cover = self.reserved.passing();
        let staged = staged.map(move |(key, row)| (key, shown(row, &mut staged_cover)));
        // A stored row is reshaped only while this change set drops something.
        let stored = stored.map(move |row| row.map(|(key, row)| (key, self.shaped(table, row))));
        Ok(Box::new(
            overlay(stored, staged, back, |_, row| row).inspect(|row| {
                self.rows_read
                    .set(self.rows_read.get() + u64::from(row.is_ok()));
            }),
        ))
    }

    fn counted(&self, of: Keys, from: Bound<&Key>, to: Bound<&Key>) -> Result<UnitIter<'_>> {
        // With nothing staged in what is counted, the stored records are counted where they are.
        Ok(match of {
            Keys::Table(table) if self.rows.contains_key(&table.0) => {
                let span = keys::span(from, to);
                let stored = self.base.moment.row_keys(table.0, &span)?;
                let staged = staged_in(&self.rows, table.0, span, false)
                    .map(|(key, row)| (key, row.values.as_ref().map(|_| ())));
                Box::new(
                    overlay(stored, staged, false, |_, value| Ok(value))
                        .map(|found| found.map(drop)),
                )
            }
            Keys::Index(index) if self.index.contains_key(&index.0) => Box::new(
                self.index(index, from, to, false)?
                    .map(|found| found.map(drop)),
            ),
            _ => return self.base.counted(of, from, to),
        })
    }

    fn index(
        &self,
        index: IndexId,
        from: Bound<&Key>,
        to: Bound<&Key>,
        back: bool,
    ) -> Result<EntryIter<'_>> {
        let span = keys::span(from, to);
        let stored = self.base.moment.joined(index.0, &span, back)?;
        let staged = staged_in(&self.index, index.0, span, back);
        let staged = staged.map(|(joined, entry)| (joined, *entry));
        Ok(Box::new(
            overlay(stored, staged, back, |_, entry| Ok(entry)).map(split),
        ))
    }

    fn kept(&self, count: CountId, from: Bound<&Key>, to: Bound<&Key>) -> Result<KeptIter<'_>> {
        counts::kept(self, count, &keys::span(from, to))
    }

    fn count(&self, table: TableId) -> Result<u64> {
        let change = self.counts.get(&table.0).copied().unwrap_or(0);
        Ok(self.base.count(table)?.saturating_add_signed(change))
    }

    fn load(&self, beside: &Beside, reservation: &mut Reservation) -> Result<Value> {
        self.base.load(beside, reservation)
    }

    fn staged(&self, table: TableId, each: &mut dyn FnMut(&Key) -> Result<bool>) -> Result<()> {
        for key in self.rows.get(&table.0).into_iter().flat_map(BTreeMap::keys) {
            if !each(key)? {
                break;
            }
        }
        Ok(())
    }

    fn vectors(
        &self,
        from: VectorRecord<'_>,
        each: &mut dyn FnMut(VectorRecord<'_>, &[u8]) -> Result<bool>,
    ) -> Result<()> {
        if self.vectors.is_empty() {
            return self.base.vectors(from, each);
        }
        let start = Key(keys::vector(from));
        let end = Key(vec![start.0[0] + 1]);
        let mut staged = self.vectors.range(start..end).peekable();
        let mut visiting = true;
        let mut key = Key(Vec::new());
        let mut show = |stored: Key, bytes: &[u8]| {
            let record = keys::vector_of(&stored.0, &mut key)
                .ok_or_else(|| self.damaged("a staged vector key is not a vector record"))?;
            each(record, bytes)
        };
        self.base.vectors(from, &mut |record, value| {
            let stored = Key(keys::vector(record));
            let before = std::iter::from_fn(|| staged.next_if(|(key, _)| *key <= &stored));
            let before = before.map(|(key, value)| (key, value.as_deref()));
            for found in overlay(
                std::iter::once(Ok((stored.clone(), value))),
                before,
                false,
                |_, value| Ok(value),
            ) {
                let (key, value) = found?;
                visiting = show(key, value)?;
                if !visiting {
                    break;
                }
            }
            Ok(visiting)
        })?;
        if visiting {
            for found in overlay(
                std::iter::empty(),
                staged.map(|(key, value)| (key, value.as_deref())),
                false,
                |_, value| Ok(value),
            ) {
                let (key, value) = found?;
                if !show(key, value)? {
                    break;
                }
            }
        }
        Ok(())
    }

    fn vector_number(&self, number: VectorNumber) -> Result<u64> {
        self.numbers
            .get(&disk::vector_number_at(number))
            .copied()
            .map_or_else(|| self.base.vector_number(number), Ok)
    }

    fn damaged(&self, reason: &str) -> Error {
        self.base.damaged(reason)
    }
}
