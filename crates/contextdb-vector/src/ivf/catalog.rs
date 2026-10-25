//! Paged generation catalogs. All state changes use the caller's native transaction.
//! No resident run/member map grows with population. Coalescing joins immutable
//! list chains; it never copies the already merged population on each seal.
use super::{
    codec,
    memory::corrupt,
    storage::{Manifest, PageKind, ReadPages, WritePages},
};
use contextdb_core::Result;

const MAGIC: u64 = u64::from_le_bytes(*b"IVFCAT02");
const WORDS: usize = 32;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Catalog {
    pub next_run: u64,
    pub first: u64,
    pub last: u64,
    pub runs: u64,
    pub base_slots: u64,
    pub tail_head: u64,
    pub tail_end: u64,
    pub legacy: Option<Manifest>,
    pub legacy_first: u64,
    pub legacy_runs: u64,
    pub frontier: u64,
    pub switch_lsn: u64,
    pub remap_slot: u64,
    pub remap_examined: u64,
    pub remap_output: u64,
    pub remap_join_target: u64,
    pub coalesce_left: u64,
    pub coalesce_right: u64,
    pub coalesce_output: u64,
    pub coalesce_list: u32,
    pub unserviceable: Option<super::storage::PageKey>,
    pub legacy_unserviceable: bool,
}
impl Catalog {
    pub fn new(base_slots: u64) -> Self {
        Self {
            next_run: 1,
            base_slots,
            ..Self::default()
        }
    }
    pub fn load(source: &impl ReadPages, m: Manifest) -> Result<Self> {
        let mut raw = [0; super::storage::PAGE_BYTES];
        if !super::storage::read_page(source, m.key(PageKind::Catalog, 0, 0), &mut raw)? {
            return if m.slots == 0 {
                Ok(Self::new(0))
            } else {
                Err(corrupt())
            };
        }
        let mut w = [0u64; WORDS];
        for (dst, src) in w.iter_mut().zip(raw.as_chunks::<8>().0) {
            *dst = u64::from_le_bytes(*src);
        }
        if w[0] != MAGIC || w[1] == 0 || w[9] > 1 {
            return Err(corrupt());
        }
        if w[6] > w[7]
            || w[5] > m.slots
            || (w[4] == 0) != (w[2] == 0)
            || (w[4] == 0) != (w[3] == 0)
            || w[4] >= w[1]
            || w[4] > m.slots
            || (w[8] != 0
                && (w[22] == 0 || w[8] >= w[1] || w[20] > u64::from(m.active_lists.max(1))))
            || w[27] > 4
            || w[31] > 1
            || w[26] > u64::from(m.active_lists.max(1))
            || ((w[25] == 0) != (w[23] == 0))
            || ((w[25] == 0) != (w[24] == 0))
        {
            return Err(corrupt());
        }
        let legacy = if w[9] == 0 {
            None
        } else {
            Some(Manifest {
                partition: m.partition,
                generation: w[10],
                dimensions: usize::try_from(w[11]).map_err(|_| corrupt())?,
                lists: u32::try_from(w[12]).map_err(|_| corrupt())?,
                active_lists: u32::try_from(w[13]).map_err(|_| corrupt())?,
                slots: w[14],
                sealed_slots: w[15],
                codebook: raw[WORDS * 8..WORDS * 8 + 32]
                    .try_into()
                    .map_err(|_| corrupt())?,
                run: 0,
            })
        };
        Ok(Self {
            next_run: w[1],
            first: w[2],
            last: w[3],
            runs: w[4],
            base_slots: w[5],
            tail_head: w[6],
            tail_end: w[7],
            legacy,
            legacy_first: w[16],
            legacy_runs: w[17],
            frontier: w[18],
            switch_lsn: w[19],
            remap_slot: w[20],
            remap_examined: w[21],
            remap_output: w[22],
            remap_join_target: w[8],
            coalesce_left: w[23],
            coalesce_right: w[24],
            coalesce_output: w[25],
            coalesce_list: u32::try_from(w[26]).map_err(|_| corrupt())?,
            unserviceable: if w[27] == 0 {
                None
            } else {
                Some(super::storage::PageKey {
                    partition: m.partition,
                    generation: m.generation,
                    kind: match w[27] {
                        1 => PageKind::Codebook,
                        2 => PageKind::Directory,
                        3 => PageKind::ListUsed,
                        _ => PageKind::Catalog,
                    },
                    list: u32::try_from(w[28]).map_err(|_| corrupt())?,
                    run: w[29],
                    ordinal: w[30],
                })
            },
            legacy_unserviceable: w[31] == 1,
        })
    }
    pub fn store(self, tx: &mut impl WritePages, m: Manifest) -> Result<()> {
        let old = self.legacy;
        let w = [
            MAGIC,
            self.next_run,
            self.first,
            self.last,
            self.runs,
            self.base_slots,
            self.tail_head,
            self.tail_end,
            self.remap_join_target,
            u64::from(old.is_some()),
            old.map_or(0, |x| x.generation),
            old.map_or(0, |x| x.dimensions as u64),
            old.map_or(0, |x| u64::from(x.lists)),
            old.map_or(0, |x| u64::from(x.active_lists)),
            old.map_or(0, |x| x.slots),
            old.map_or(0, |x| x.sealed_slots),
            self.legacy_first,
            self.legacy_runs,
            self.frontier,
            self.switch_lsn,
            self.remap_slot,
            self.remap_examined,
            self.remap_output,
            self.coalesce_left,
            self.coalesce_right,
            self.coalesce_output,
            u64::from(self.coalesce_list),
            self.unserviceable.map_or(0, |k| match k.kind {
                PageKind::Codebook => 1,
                PageKind::Directory => 2,
                PageKind::ListUsed => 3,
                _ => 4,
            }),
            self.unserviceable.map_or(0, |k| u64::from(k.list)),
            self.unserviceable.map_or(0, |k| k.run),
            self.unserviceable.map_or(0, |k| k.ordinal),
            u64::from(self.legacy_unserviceable),
        ];
        let mut raw = [0; super::storage::PAGE_BYTES];
        for (dst, word) in raw.as_chunks_mut::<8>().0.iter_mut().zip(w) {
            dst.copy_from_slice(&word.to_le_bytes());
        }
        if let Some(old) = old {
            raw[WORDS * 8..WORDS * 8 + 32].copy_from_slice(&old.codebook);
        }
        super::storage::put_page(tx, m.key(PageKind::Catalog, 0, 0), &raw)
    }
    pub fn allocate_run(&mut self) -> Result<u64> {
        let id = self.next_run;
        self.next_run = id.checked_add(1).ok_or_else(corrupt)?;
        Ok(id)
    }
    pub fn attach(
        &mut self,
        tx: &mut impl WritePages,
        m: Manifest,
        id: u64,
        mut run: Run,
    ) -> Result<()> {
        run.next = 0;
        run.store(tx, m, id)?;
        if self.last != 0 {
            let mut tail = Run::load(tx, m, self.last)?;
            tail.next = id;
            tail.store(tx, m, self.last)?;
        } else {
            self.first = id;
        }
        self.last = id;
        self.runs = self.runs.checked_add(1).ok_or_else(corrupt)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Run {
    pub first: u64,
    pub last: u64,
    pub entries: u64,
    pub next: u64,
}
impl Run {
    pub fn load(s: &impl ReadPages, m: Manifest, id: u64) -> Result<Self> {
        let mut raw = [0; super::storage::PAGE_BYTES];
        if id == 0 || !super::storage::read_page(s, m.key(PageKind::Catalog, 1, id), &mut raw)? {
            return Err(corrupt());
        }
        let word = |i: usize| -> Result<u64> {
            Ok(u64::from_le_bytes(
                raw[i * 8..i * 8 + 8].try_into().map_err(|_| corrupt())?,
            ))
        };
        if word(0)? != MAGIC {
            return Err(corrupt());
        }
        Ok(Self {
            first: word(1)?,
            last: word(2)?,
            entries: word(3)?,
            next: word(4)?,
        })
    }
    pub fn store(self, tx: &mut impl WritePages, m: Manifest, id: u64) -> Result<()> {
        let mut raw = [0; super::storage::PAGE_BYTES];
        for (dst, word) in raw.as_chunks_mut::<8>().0.iter_mut().zip([
            MAGIC,
            self.first,
            self.last,
            self.entries,
            self.next,
        ]) {
            dst.copy_from_slice(&word.to_le_bytes());
        }
        super::storage::put_page(tx, m.key(PageKind::Catalog, 1, id), &raw)
    }
    pub fn append(&mut self, tx: &mut impl WritePages, m: Manifest, slot: u64) -> Result<()> {
        if self.entries == 0 {
            self.first = slot;
        } else {
            let mut prev = Entry::load(tx, m, self.last)?;
            prev.next = slot;
            prev.store(tx, m, self.last)?;
        }
        self.last = slot;
        self.entries = self.entries.checked_add(1).ok_or_else(corrupt)?;
        Ok(())
    }
}

/// Slot metadata is native paged data, not an allocation/member in the reader heap.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Entry {
    pub lsn: u64,
    pub next: u64,
    pub run: u64,
    pub tail: u64,
    pub prepared: bool,
}
impl Entry {
    pub fn load(s: &impl ReadPages, m: Manifest, slot: u64) -> Result<Self> {
        let mut raw = [0; 40];
        codec::read(
            s,
            m,
            PageKind::SlotMetadata,
            0,
            slot.checked_mul(40).ok_or_else(corrupt)?,
            &mut raw,
        )?;
        let word = |i: usize| -> Result<u64> {
            Ok(u64::from_le_bytes(
                raw[i * 8..i * 8 + 8].try_into().map_err(|_| corrupt())?,
            ))
        };
        if word(4)? > 1 {
            return Err(corrupt());
        }
        Ok(Self {
            lsn: word(0)?,
            next: word(1)?,
            run: word(2)?,
            tail: word(3)?,
            prepared: word(4)? == 1,
        })
    }
    pub fn store(self, tx: &mut impl WritePages, m: Manifest, slot: u64) -> Result<()> {
        let mut raw = [0; 40];
        for (dst, word) in raw.as_chunks_mut::<8>().0.iter_mut().zip([
            self.lsn,
            self.next,
            self.run,
            self.tail,
            u64::from(self.prepared),
        ]) {
            dst.copy_from_slice(&word.to_le_bytes());
        }
        codec::write(
            tx,
            m,
            PageKind::SlotMetadata,
            0,
            slot.checked_mul(40).ok_or_else(corrupt)?,
            &raw,
        )
    }
}

pub(crate) fn tail_slot(s: &impl ReadPages, m: Manifest, ordinal: u64) -> Result<u64> {
    let mut raw = [0; 8];
    codec::read(
        s,
        m,
        PageKind::Tail,
        0,
        ordinal.checked_mul(8).ok_or_else(corrupt)?,
        &mut raw,
    )?;
    Ok(u64::from_le_bytes(raw))
}
pub(crate) fn append_tail(
    tx: &mut impl WritePages,
    m: Manifest,
    c: &mut Catalog,
    slot: u64,
) -> Result<u64> {
    let seq = c.tail_end;
    codec::write(
        tx,
        m,
        PageKind::Tail,
        0,
        seq.checked_mul(8).ok_or_else(corrupt)?,
        &slot.to_le_bytes(),
    )?;
    c.tail_end = c.tail_end.checked_add(1).ok_or_else(corrupt)?;
    Ok(seq)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CatalogState {
    pub delta_runs: u64,
    pub runs_over_cap: u64,
    pub legacy_runs: u64,
    pub retained_codebook: Option<[u8; 32]>,
    pub remap_entries_examined: u64,
    pub remap_output_run: u64,
    pub coalesce_list: u32,
    pub unserviceable: Option<super::storage::PageKey>,
    pub legacy_unserviceable: bool,
    pub coalescing: bool,
    pub pending_coalesce_pages: u64,
    pub recovery_action: Option<&'static str>,
    /// False means a live verified result not yet recorded by maintenance.
    pub verdict_recorded: bool,
}
pub fn inspect(
    source: &impl ReadPages,
    partition: u64,
    max_delta_runs: u64,
) -> Result<Option<CatalogState>> {
    let Some(m) = source.manifest(partition)? else {
        return Ok(None);
    };
    let c = Catalog::load(source, m)?;
    // A recorded verdict takes precedence over the session's live verification.
    let verdict_recorded = c.unserviceable.is_some();
    let damage = if verdict_recorded {
        c.unserviceable
    } else if let Some(verdict) = source.inspection_verdict(partition, m.generation) {
        verdict.damage()
    } else {
        let damage = super::recovery::inspect_damage(source, m, c)?;
        let verdict = damage.map_or(
            super::storage::Verdict::Healthy,
            super::storage::Verdict::Corrupt,
        );
        source.record_inspection_verdict(partition, m.generation, verdict);
        damage
    };
    Ok(Some(CatalogState {
        delta_runs: c.runs,
        runs_over_cap: c.runs.saturating_sub(max_delta_runs),
        legacy_runs: c.legacy_runs,
        retained_codebook: c.legacy.map(|x| x.codebook),
        remap_entries_examined: c.remap_examined,
        remap_output_run: c.remap_output,
        coalesce_list: c.coalesce_list,
        unserviceable: damage,
        legacy_unserviceable: c.legacy_unserviceable,
        coalescing: c.coalesce_output != 0,
        recovery_action: damage.map(|_| "rebuild"),
        verdict_recorded,
        pending_coalesce_pages: if c.coalesce_output == 0 {
            0
        } else {
            let rounds = c.runs.saturating_sub(max_delta_runs).max(1);
            let lists = u64::from(m.active_lists.max(1));
            let control = super::writer::coalesce_control_pages(m)? as u64;
            rounds
                .checked_mul(lists)
                .and_then(|v| v.checked_sub(u64::from(c.coalesce_list)))
                .and_then(|v| v.checked_mul(super::writer::DIRECTORY_PAGES as u64))
                .and_then(|v| {
                    rounds
                        .checked_mul(control)
                        .and_then(|control| v.checked_add(control))
                })
                .ok_or_else(corrupt)?
        },
    }))
}
