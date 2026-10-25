use super::{
    memory::corrupt,
    storage::{Manifest, PAGE_BYTES, PageKind, ReadPages, WritePages},
};
use contextdb_core::Result;

/// Copy a bounded canonical/metadata range through admitted native payloads.
pub(crate) fn read(
    source: &impl ReadPages,
    manifest: Manifest,
    kind: PageKind,
    list: u32,
    mut offset: u64,
    mut out: &mut [u8],
) -> Result<u64> {
    let mut pages = 0;
    while !out.is_empty() {
        let mut page = [0u8; PAGE_BYTES];
        if !source.read_admitted_page(
            manifest.key(kind, list, offset / PAGE_BYTES as u64),
            &mut page,
        )? {
            return Err(corrupt());
        }
        let at = (offset % PAGE_BYTES as u64) as usize;
        let n = out.len().min(PAGE_BYTES - at);
        out[..n].copy_from_slice(&page[at..at + n]);
        out = &mut out[n..];
        offset += n as u64;
        pages += 1;
    }
    Ok(pages)
}
pub(crate) fn write(
    target: &mut impl WritePages,
    manifest: Manifest,
    kind: PageKind,
    list: u32,
    mut offset: u64,
    mut data: &[u8],
) -> Result<()> {
    while !data.is_empty() {
        let key = manifest.key(kind, list, offset / PAGE_BYTES as u64);
        let mut page = [0u8; PAGE_BYTES];
        target.read_admitted_page(key, &mut page)?;
        let at = (offset % PAGE_BYTES as u64) as usize;
        let n = data.len().min(PAGE_BYTES - at);
        page[at..at + n].copy_from_slice(&data[..n]);
        target.put_page(key, &page)?;
        offset += n as u64;
        data = &data[n..];
    }
    Ok(())
}
pub(crate) fn count(source: &impl ReadPages, manifest: Manifest, list: u32) -> Result<u64> {
    let mut page = [0u8; PAGE_BYTES];
    if !source.read_admitted_page(
        manifest.key(PageKind::Directory, 0, list as u64 / 488),
        &mut page,
    )? {
        return if manifest.sealed_slots > 0 {
            Err(corrupt())
        } else {
            Ok(0)
        };
    }
    let at = (list as usize % 488) * 8;
    Ok(u64::from_le_bytes(
        page[at..at + 8].try_into().map_err(|_| corrupt())?,
    ))
}
pub(crate) fn set_count(
    target: &mut impl WritePages,
    manifest: Manifest,
    list: u32,
    value: u64,
) -> Result<()> {
    write(
        target,
        manifest,
        PageKind::Directory,
        0,
        list as u64 * 8,
        &value.to_le_bytes(),
    )
}
pub(crate) fn visible(
    source: &impl ReadPages,
    manifest: Manifest,
    slot: u64,
    page: &mut [u8; PAGE_BYTES],
    loaded: &mut Option<u64>,
) -> Result<bool> {
    let ordinal = slot / (PAGE_BYTES as u64 * 8);
    if *loaded != Some(ordinal) {
        if !source.read_admitted_page(manifest.key(PageKind::Visibility, 0, ordinal), page)? {
            return Err(corrupt());
        }
        *loaded = Some(ordinal);
    }
    let bit = (slot % (PAGE_BYTES as u64 * 8)) as usize;
    Ok(page[bit / 8] & (1 << (bit % 8)) != 0)
}
pub(crate) fn set_visible(
    target: &mut impl WritePages,
    manifest: Manifest,
    slot: u64,
    live: bool,
) -> Result<()> {
    let key = manifest.key(PageKind::Visibility, 0, slot / (PAGE_BYTES as u64 * 8));
    let mut page = [0u8; PAGE_BYTES];
    target.read_admitted_page(key, &mut page)?;
    let bit = (slot % (PAGE_BYTES as u64 * 8)) as usize;
    if live {
        page[bit / 8] |= 1 << (bit % 8);
    } else {
        page[bit / 8] &= !(1 << (bit % 8));
    }
    target.put_page(key, &page)
}
