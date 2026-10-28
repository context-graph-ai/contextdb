//! Immutable publication names, admitted before construction and shared through final free.
#![allow(
    clippy::result_large_err,
    reason = "Keep typed inline budget refusals allocation-free."
)]
use contextdb_core::read_memory::{NativeRefusal, ReadCredit, ReadMemoryOwner, ReadShared};
use contextdb_core::{Error, Result};
use std::alloc::Layout;

#[derive(Debug)]
pub(crate) struct TableNames {
    // Physical payloads precede their original-payer invoice.
    names: Vec<String>,
    memory: ReadCredit,
}

#[derive(Debug, Clone)]
pub(crate) enum PublicationTables {
    Funded(ReadShared<TableNames>),
    #[cfg(test)]
    Unfunded(std::collections::BTreeSet<String>),
}

fn size_error() -> Error {
    Error::NativeEngineOther(NativeRefusal::Other(
        "publication table size overflow or traversal mismatch",
    ))
}

impl PublicationTables {
    pub(crate) fn try_from_visitor(
        owner: ReadMemoryOwner,
        mut visit: impl FnMut(&mut dyn FnMut(&str) -> Result<()>) -> Result<()>,
    ) -> Result<Self> {
        let mut count = 0usize;
        let mut strings = 0usize;
        visit(&mut |name| {
            count = count.checked_add(1).ok_or_else(size_error)?;
            strings = strings.checked_add(name.len()).ok_or_else(size_error)?;
            Ok(())
        })?;
        let backing = Layout::array::<String>(count)
            .map_err(|_| size_error())?
            .size();
        let heap = backing.checked_add(strings).ok_or_else(size_error)?;
        let control = ReadCredit::try_new_owned(
            owner.clone(),
            ReadShared::<TableNames>::layout().size(),
            "bounded_read",
            "publication_tables",
            "Release retained publication metadata or raise MEMORY_LIMIT.",
        )?;
        let memory = ReadCredit::try_new_owned(
            owner,
            heap,
            "bounded_read",
            "publication_tables",
            "Release retained publication metadata or raise MEMORY_LIMIT.",
        )?;
        let mut payload = TableNames {
            names: Vec::with_capacity(count),
            memory,
        };
        let mut built_strings = 0usize;
        visit(&mut |name| {
            let next = built_strings
                .checked_add(name.len())
                .ok_or_else(size_error)?;
            // Refuse a mismatched second traversal before it could allocate past admission.
            if payload.names.len() == count || next > strings {
                return Err(size_error());
            }
            payload.names.push(name.to_owned());
            built_strings = next;
            Ok(())
        })?;
        if payload.names.len() != count || built_strings != strings {
            return Err(size_error());
        }
        payload.names.sort_unstable();
        payload.names.dedup();
        let retained_strings = payload.names.iter().map(String::capacity).sum::<usize>();
        let retained_backing = Layout::array::<String>(payload.names.capacity())
            .map_err(|_| size_error())?
            .size();
        if retained_backing != backing || retained_strings > strings {
            return Err(size_error());
        }
        // dedup has physically destroyed duplicate strings; retain the entire Vec capacity.
        payload
            .memory
            .try_shrink_native(strings - retained_strings)
            .map_err(Error::NativeEngineOther)?;
        ReadShared::try_from_prepaid_native(payload, control)
            .map(Self::Funded)
            .map_err(Error::NativeEngineOther)
    }
    pub(crate) fn try_one(owner: ReadMemoryOwner, name: &str) -> Result<Self> {
        Self::try_from_visitor(owner, |visit| visit(name))
    }
    pub(crate) fn contains(&self, name: &str) -> bool {
        match self {
            Self::Funded(names) => names
                .names
                .binary_search_by(|candidate| candidate.as_str().cmp(name))
                .is_ok(),
            #[cfg(test)]
            Self::Unfunded(names) => names.contains(name),
        }
    }
    pub(crate) fn iter(&self) -> PublicationTableIter<'_> {
        match self {
            Self::Funded(names) => PublicationTableIter::Funded(names.names.iter()),
            #[cfg(test)]
            Self::Unfunded(names) => PublicationTableIter::Unfunded(names.iter()),
        }
    }
}

pub(crate) enum PublicationTableIter<'a> {
    Funded(std::slice::Iter<'a, String>),
    #[cfg(test)]
    Unfunded(std::collections::btree_set::Iter<'a, String>),
}
impl<'a> Iterator for PublicationTableIter<'a> {
    type Item = &'a String;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Funded(names) => names.next(),
            #[cfg(test)]
            Self::Unfunded(names) => names.next(),
        }
    }
}
impl<'a> IntoIterator for &'a PublicationTables {
    type Item = &'a String;
    type IntoIter = PublicationTableIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
