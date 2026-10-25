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
        if !super::storage::read_page(
            source,
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
        super::storage::read_page(target, key, &mut page)?;
        let at = (offset % PAGE_BYTES as u64) as usize;
        let n = data.len().min(PAGE_BYTES - at);
        page[at..at + n].copy_from_slice(&data[..n]);
        super::storage::put_page(target, key, &page)?;
        offset += n as u64;
        data = &data[n..];
    }
    Ok(())
}
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Directory {
    pub count: u64,
    pub head: u64,
    pub tail: u64,
}
fn directory_offset(list: u32) -> u64 {
    let per_page = (PAGE_BYTES / 24) as u64;
    u64::from(list) / per_page * PAGE_BYTES as u64 + u64::from(list) % per_page * 24
}
pub(crate) fn directory(source: &impl ReadPages, m: Manifest, list: u32) -> Result<Directory> {
    let mut raw = [0; PAGE_BYTES];
    let offset = directory_offset(list);
    if !super::storage::read_page(
        source,
        m.key(PageKind::Directory, 0, offset / PAGE_BYTES as u64),
        &mut raw,
    )? {
        return if m.sealed_slots > 0 && m.run == 0 {
            Err(corrupt())
        } else {
            Ok(Directory::default())
        };
    }
    let at = (offset % PAGE_BYTES as u64) as usize;
    let word = |i: usize| -> Result<u64> {
        Ok(u64::from_le_bytes(
            raw[at + i * 8..at + i * 8 + 8]
                .try_into()
                .map_err(|_| corrupt())?,
        ))
    };
    let d = Directory {
        count: word(0)?,
        head: word(1)?,
        tail: word(2)?,
    };
    if (d.count == 0) != (d.head == 0) || (d.count == 0) != (d.tail == 0) {
        return Err(corrupt());
    }
    Ok(d)
}
pub(crate) fn set_directory(
    tx: &mut impl WritePages,
    m: Manifest,
    list: u32,
    d: Directory,
) -> Result<()> {
    let mut raw = [0; 24];
    for (dst, word) in raw
        .as_chunks_mut::<8>()
        .0
        .iter_mut()
        .zip([d.count, d.head, d.tail])
    {
        dst.copy_from_slice(&word.to_le_bytes());
    }
    write(tx, m, PageKind::Directory, 0, directory_offset(list), &raw)
}
pub(crate) fn set_count(
    tx: &mut impl WritePages,
    m: Manifest,
    list: u32,
    value: u64,
) -> Result<()> {
    let mut d = directory(tx, m, list)?;
    d.count = value;
    set_directory(tx, m, list, d)
}
pub(crate) fn next_list_page(s: &impl ReadPages, m: Manifest, list: u32, id: u64) -> Result<u64> {
    if id == 0 {
        return Err(corrupt());
    }
    let mut raw = [0; 8];
    read(s, m, PageKind::ListLinks, list, (id - 1) * 8, &mut raw)?;
    Ok(u64::from_le_bytes(raw))
}
pub(crate) fn set_next_list_page(
    tx: &mut impl WritePages,
    m: Manifest,
    list: u32,
    id: u64,
    next: u64,
) -> Result<()> {
    if id == 0 {
        return Err(corrupt());
    }
    write(
        tx,
        m,
        PageKind::ListLinks,
        list,
        (id - 1) * 8,
        &next.to_le_bytes(),
    )
}
pub(crate) fn allocate_list_page(tx: &mut impl WritePages, m: Manifest, list: u32) -> Result<u64> {
    let mut raw = [0; PAGE_BYTES];
    let per = (PAGE_BYTES / 8) as u64;
    let key = m.key(PageKind::ListAllocator, 0, u64::from(list) / per);
    super::storage::read_page(tx, key, &mut raw)?;
    let at = (u64::from(list) % per) as usize * 8;
    let old = u64::from_le_bytes(raw[at..at + 8].try_into().map_err(|_| corrupt())?);
    let next = old.checked_add(1).ok_or_else(super::memory::overflow)?;
    raw[at..at + 8].copy_from_slice(&next.to_le_bytes());
    super::storage::put_page(tx, key, &raw)?;
    set_next_list_page(tx, m, list, next, 0)?;
    Ok(next)
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
        if !super::storage::read_page(source, manifest.key(PageKind::Visibility, 0, ordinal), page)?
        {
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
    super::storage::read_page(target, key, &mut page)?;
    let bit = (slot % (PAGE_BYTES as u64 * 8)) as usize;
    if live {
        page[bit / 8] |= 1 << (bit % 8);
    } else {
        page[bit / 8] &= !(1 << (bit % 8));
    }
    super::storage::put_page(target, key, &page)
}
