//! Derived damage does not invalidate canonical rows or their version journal.
use super::{
    book::Codebook,
    catalog::{Catalog, Run},
    codec,
    memory::corrupt,
    storage::{Manifest, PageKey, PageKind, ReadPages},
};
use contextdb_core::{Error, Result};
pub(crate) fn damaged_page(error: &Error) -> bool {
    matches!(error, Error::StoreCorrupted { .. })
}
pub(crate) fn directory_damage(
    source: &impl ReadPages,
    m: Manifest,
    c: Catalog,
) -> Result<Option<PageKey>> {
    let mut id = 0;
    let roots = c.runs.checked_add(1).ok_or_else(corrupt)?;
    for root in 0..roots {
        let mut view = m;
        view.run = id;
        if let Err(error) = codec::validate_directory_pages(source, view) {
            if damaged_page(&error) {
                return Ok(Some(view.key(PageKind::Directory, 0, 0)));
            }
            return Err(error);
        }
        id = if root == 0 {
            c.first
        } else {
            Run::load(source, m, id)?.next
        };
    }
    if id != 0 {
        return Err(corrupt());
    }
    Ok(None)
}
pub(crate) fn inspect_damage(
    source: &impl ReadPages,
    m: Manifest,
    c: Catalog,
) -> Result<Option<PageKey>> {
    if let Some(key) = c.unserviceable {
        return Ok(Some(key));
    }
    if m.active_lists == 0 {
        return Ok(None);
    }
    if let Err(error) = Codebook::verify_values(source, m) {
        if damaged_page(&error) {
            return Ok(Some(m.key(PageKind::Codebook, 0, 0)));
        }
        return Err(error);
    }
    directory_damage(source, m, c)
}
