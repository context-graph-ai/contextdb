//! The tidy-up's calls into the storage library, and the store folder's one advisory file,
//! `space`: what the writer must remember past a commit the disk limit refused, which that
//! commit cannot hold. Only the writer writes it, under its turn, by replacing it whole; any
//! process reads it. Each entry carries the committed state it was decided at and is believed
//! only while that state holds, so a missing, stale or damaged file says nothing, never
//! something wrong. It is not counted against the disk limit and a snapshot does not copy it.

use std::fs::{self, File};
use std::io::{Read as _, Write as _};
use std::path::Path;

use super::{HeldFor, Store, Stored, VectorId, disk, locked};
use crate::types::Result;

/// The compaction retry and the partitions refused for room, as the writer decided them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Space {
    /// The release mark the retry was chosen under, the next candidate's ordinal, and whether
    /// the refused attempt had chosen nothing, so no smaller move is owed.
    pub retry: ((u64, u64), u32, bool),
    /// Partitions whose seal or build step was refused for room, by column and number, with the
    /// stamp of the partition's committed state then; never a partition's key values.
    pub full: Vec<(VectorId, u32, u64)>,
}

const VERSION: u8 = 1;

/// The most bytes the file holds, and so the most a reader reads of it.
const LIMIT: usize = 16 << 20;

/// FNV-1a, 64 bits: the file's checksum and a partition's stamp.
pub(crate) fn fnv(bytes: &[u8]) -> u64 {
    let step = |hash: u64, byte: &u8| (hash ^ u64::from(*byte)).wrapping_mul(0x100_0000_01b3);
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, step)
}

fn take<'a>(at: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
    let (taken, rest) = at.split_at_checked(n)?;
    *at = rest;
    Some(taken)
}

fn int<const N: usize>(at: &mut &[u8]) -> Option<[u8; N]> {
    take(at, N)?.try_into().ok()
}

impl Space {
    /// The file in `folder`; a missing or damaged one, or one of another version, is empty.
    pub(crate) fn read(folder: &Path) -> Space {
        let mut bytes = Vec::new();
        let read = File::open(folder.join("space"))
            .and_then(|f| f.take(LIMIT as u64).read_to_end(&mut bytes));
        read.ok()
            .and_then(|_| Space::decode(&bytes))
            .unwrap_or_default()
    }

    /// The writer's copy at open, after removing a replacement a kill left half written.
    pub(super) fn opened(folder: &Path) -> std::sync::Mutex<Space> {
        drop(fs::remove_file(folder.join("space.tmp")));
        std::sync::Mutex::new(Space::read(folder))
    }

    fn decode(bytes: &[u8]) -> Option<Space> {
        let (mut at, sum) = bytes.split_at_checked(bytes.len().checked_sub(8)?)?;
        if fnv(at).to_le_bytes() != sum || take(&mut at, 1)? != [VERSION] {
            return None;
        }
        let mark = (
            u64::from_le_bytes(int(&mut at)?),
            u64::from_le_bytes(int(&mut at)?),
        );
        let k = u32::from_le_bytes(int(&mut at)?);
        let retry = (mark, k, take(&mut at, 1)? == [1]);
        let mut full = Vec::new();
        for _ in 0..u32::from_le_bytes(int(&mut at)?) {
            let vector = VectorId(u32::from_le_bytes(int(&mut at)?));
            let number = u32::from_le_bytes(int(&mut at)?);
            full.push((vector, number, u64::from_le_bytes(int(&mut at)?)));
        }
        Some(Space { retry, full })
    }

    fn encode(&self) -> Vec<u8> {
        let ((last, reader), k, terminal) = self.retry;
        let mut out = vec![VERSION];
        for part in [
            &last.to_le_bytes()[..],
            &reader.to_le_bytes(),
            &k.to_le_bytes(),
        ] {
            out.extend(part);
        }
        out.push(u8::from(terminal));
        // The retry always, and the partitions in order while they fit with the count and sum.
        let full = &self.full[..self.full.len().min((LIMIT - out.len() - 12) / 16)];
        out.extend((full.len() as u32).to_le_bytes());
        for (vector, number, stamp) in full {
            out.extend(vector.0.to_le_bytes());
            out.extend(number.to_le_bytes());
            out.extend(stamp.to_le_bytes());
        }
        out.extend(fnv(&out).to_le_bytes());
        out
    }

    // Replaces the file whole: a synced replacement renamed over it, then the folder synced.
    fn write(&self, folder: &Path) -> std::io::Result<()> {
        let temp = folder.join("space.tmp");
        let mut file = File::create(&temp)?;
        file.write_all(&self.encode())?;
        file.sync_all()?;
        fs::rename(&temp, folder.join("space"))?;
        File::open(folder)?.sync_all()
    }
}

impl Store {
    /// The writer's copy of the folder's `space` file.
    pub(crate) fn space(&self) -> Space {
        locked(&self.space).clone()
    }

    /// Changes the writer's copy and writes the file when it changed. A failure to write, a
    /// full file system included, leaves the old file, and the writer goes on from its copy.
    pub(crate) fn remember(&self, change: impl FnOnce(&mut Space)) {
        let mut space = locked(&self.space);
        let before = space.clone();
        change(&mut space);
        if *space != before {
            drop(space.write(&self.folder));
        }
    }

    pub(crate) fn compaction_position(&self) -> Result<Option<Vec<u8>>> {
        self.compaction(|t| Ok(t.compaction_position(false)?.0))
    }

    /// The fewest free pages a commit since the last call left itself, taken.
    pub(crate) fn surplus(&self) -> Option<u64> {
        locked(&self.surplus).take()
    }

    /// One step of the pass that packs the file, in a commit of its own, after the release of
    /// what is due: it ends the pass when free pages are fewer than `stop`, and with `k`
    /// above zero it takes the `k`-th page that has a lower place alone. A step refused for room
    /// has failed, and the pass keeps its last stored place. Also says whether the step had
    /// chosen a page to move, and the pages a refused step read.
    pub(crate) fn compaction_step(
        &self,
        stop: u64,
        k: u32,
    ) -> (Result<redb::CompactionStep>, bool, u64) {
        let HeldFor::Writing { data, .. } = &self.held else {
            unreachable!()
        };
        let (mut selected, mut read) = (false, 0);
        let mut step = || -> Stored<_> {
            let mut write = data.begin_write()?;
            let from = write.compaction_position(false)?.0;
            let at = from.as_deref();
            let (step, _) =
                write.compact_pages_choosing(256, at, stop, k as usize, &mut selected)?;
            read = step.read;
            write.prepare_compaction_commit()?;
            write.commit()?;
            Ok(step)
        };
        let step = step().map_err(|e| disk::failed(&self.folder, "compaction", e));
        (step, selected, read)
    }

    /// The release mark: the last committed id and the oldest reader clamped to it.
    pub(crate) fn compaction_mark(&self) -> Result<(u64, u64)> {
        self.compaction(|t| Ok(t.compaction_mark()?))
    }

    /// Whether one look finds a live reader holding back freed pages; false when it cannot.
    pub(crate) fn readers_hold(&self) -> bool {
        self.compaction(|t| Ok(t.readers_hold_freed_pages()?))
            .unwrap_or(false)
    }

    pub(crate) fn compaction_state(&self) -> Result<(u64, u64, u64, bool)> {
        self.compaction(|t| Ok(t.compaction_state()?))
    }

    fn compaction<T>(&self, f: impl FnOnce(&redb::WriteTransaction) -> Stored<T>) -> Result<T> {
        let HeldFor::Writing { data, .. } = &self.held else {
            unreachable!()
        };
        let space = || -> Stored<_> { f(&data.begin_write()?) };
        space().map_err(|e| disk::failed(&self.folder, "compaction", e))
    }
}
