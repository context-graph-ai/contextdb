//! Bounded borrowed readers for canonical metadata. Inspecting a signed body
//! does not authenticate it; callers must separately verify its signature.
use super::canonical::invalid;
use contextdb_core::Result;

pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Reader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    pub fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(n).ok_or_else(invalid)?;
        let value = self.bytes.get(self.offset..end).ok_or_else(invalid)?;
        self.offset = end;
        Ok(value)
    }
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().map_err(|_| invalid())?,
        ))
    }
    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().map_err(|_| invalid())?,
        ))
    }
    pub fn count(&mut self) -> Result<usize> {
        let count = usize::try_from(self.u64()?).map_err(|_| invalid())?;
        if count > self.bytes.len() - self.offset {
            return Err(invalid());
        }
        Ok(count)
    }
    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.count()?;
        self.take(n)
    }
    pub fn string(&mut self) -> Result<&'a str> {
        std::str::from_utf8(self.bytes()?).map_err(|_| invalid())
    }
    pub fn bool(&mut self) -> Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(invalid()),
        }
    }
    pub fn digest(&mut self) -> Result<[u8; 32]> {
        self.take(32)?.try_into().map_err(|_| invalid())
    }
    pub fn end(&self) -> Result<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(invalid())
        }
    }
    fn json(&mut self) -> Result<()> {
        // The source byte slice bounds nesting and children. Keep traversal on
        // the heap rather than impose an undocumented depth policy or recurse.
        struct Frame<'a> {
            remaining: usize,
            object: bool,
            previous: Option<&'a str>,
        }
        let mut stack = vec![Frame {
            remaining: 1,
            object: false,
            previous: None,
        }];
        while let Some(frame) = stack.last_mut() {
            if frame.remaining == 0 {
                stack.pop();
                continue;
            }
            frame.remaining -= 1;
            if frame.object {
                let key = self.string()?;
                if frame.previous.is_some_and(|p| p >= key) {
                    return Err(invalid());
                }
                frame.previous = Some(key);
            }
            match self.u8()? {
                0 => {}
                1 => {
                    self.bool()?;
                }
                2 => {
                    self.string()?;
                }
                tag @ (3 | 4) => {
                    let remaining = self.count()?;
                    stack.try_reserve(1).map_err(|_| invalid())?;
                    stack.push(Frame {
                        remaining,
                        object: tag == 4,
                        previous: None,
                    });
                }
                5..=7 => {
                    self.take(8)?;
                }
                _ => return Err(invalid()),
            }
        }
        Ok(())
    }
    pub fn value(&mut self) -> Result<()> {
        match self.u8()? {
            0 => {}
            1 => {
                self.bool()?;
            }
            2 | 3 | 6 | 9 => {
                self.take(8)?;
            }
            4 => {
                self.string()?;
            }
            5 => {
                self.take(16)?;
            }
            7 => self.json()?,
            8 => {
                let n = self.count()?.checked_mul(4).ok_or_else(invalid)?;
                self.take(n)?;
            }
            _ => return Err(invalid()),
        }
        Ok(())
    }
    pub fn key(&mut self) -> Result<()> {
        let n = self.count()?;
        if n == 0 {
            return Err(invalid());
        }
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..n {
            if !seen.insert(self.string()?) {
                return Err(invalid());
            }
            self.value()?;
        }
        Ok(())
    }
    pub fn reference(&mut self) -> Result<&'a str> {
        let table = self.string()?;
        self.key()?;
        Ok(table)
    }
}

pub(crate) struct SealView<'a> {
    pub root_table: &'a str,
    pub member_count: u64,
    pub unit_digest: [u8; 32],
}
impl<'a> SealView<'a> {
    pub fn read(bytes: &'a [u8]) -> Result<Self> {
        let mut r = Reader::new(bytes);
        let root_table = r.reference()?;
        r.take(32 + 32 + 16 + 8 + 4)?;
        let kind = r.u8()?;
        if kind > 1 {
            return Err(invalid());
        }
        let member_count = r.u64()?;
        let unit_digest = r.digest()?;
        let previous = r.bool()?;
        if previous {
            r.digest()?;
        }
        if previous != (kind == 1) {
            return Err(invalid());
        }
        r.take(64)?;
        r.end()?;
        Ok(Self {
            root_table,
            member_count,
            unit_digest,
        })
    }
}

pub(crate) struct TerminalView<'a> {
    pub kind: &'static str,
    pub cause: Option<&'a str>,
}
impl<'a> TerminalView<'a> {
    pub fn read(bytes: &'a [u8]) -> Result<Self> {
        let mut signed = Reader::new(bytes);
        let core = signed.bytes()?;
        if signed.bytes()?.len() != 64 {
            return Err(invalid());
        }
        signed.end()?;
        let mut r = Reader::new(core);
        if r.string()? != "delivery-terminal-core.v1" || r.u32()? != 1 {
            return Err(invalid());
        }
        r.string()?;
        r.take(32 + 16 + 32 + 16)?;
        r.reference()?;
        r.digest()?;
        match r.u8()? {
            0 => {
                r.take(16)?;
            }
            1 => {
                r.take(32)?;
            }
            _ => return Err(invalid()),
        }
        r.digest()?;
        if r.bool()? {
            r.digest()?;
        }
        if r.bool()? && r.u8()? > 1 {
            return Err(invalid());
        }
        r.take(12)?;
        let kind = match r.u8()? {
            0 => "accepted",
            1 => "equivalent",
            2 => "refused",
            _ => return Err(invalid()),
        };
        let cause = if r.bool()? { Some(r.string()?) } else { None };
        r.take(12 + 32)?;
        match r.u8()? {
            0 => {
                r.take(8 * 3 + 32)?;
            }
            1 => {
                r.take(32 + 8)?;
            }
            _ => return Err(invalid()),
        }
        r.end()?;
        Ok(Self { kind, cause })
    }
}

/// Authenticate the exact core bytes separately from borrowed field inspection.
#[cfg(feature = "test-seams")]
pub(crate) fn verify_terminal_signature(node: &str, bytes: &[u8]) -> Result<()> {
    let mut signed = Reader::new(bytes);
    let core = signed.bytes()?;
    let signature = signed.bytes()?;
    signed.end()?;
    crate::identity::FabricIdentity::verify_lineage_by_node_id(node, core, signature)?;
    TerminalView::read(bytes)?;
    Ok(())
}
