//! Borrowed inspection of the canonical JSON string carried by Value::Json.
//! JSON object keys are already sorted by the actual serde_json Value codec.
//! No JSON tree, decoded String, escape buffer or key directory is allocated.
use super::{
    native_decode::{Decoded, Invalid},
    native_record_codec::NativeEncoder,
};
use std::str::FromStr;
#[derive(Clone, Copy)]
struct Text<'a> {
    raw: &'a str,
}
impl<'a> Text<'a> {
    fn bytes(self) -> impl Iterator<Item = u8> + 'a {
        let mut i = self.raw.as_bytes().iter().copied();
        std::iter::from_fn(move || {
            let b = i.next()?;
            Some(if b != b'\\' {
                b
            } else {
                match i.next()? {
                    b'"' => b'"',
                    b'\\' => b'\\',
                    b'b' => 8,
                    b't' => 9,
                    b'n' => 10,
                    b'f' => 12,
                    b'r' => 13,
                    b'u' => {
                        i.next()?;
                        i.next()?;
                        let hi = i.next()?;
                        let lo = i.next()?;
                        let hex = |v| if v <= b'9' { v - b'0' } else { v - b'a' + 10 };
                        hex(hi) * 16 + hex(lo)
                    }
                    _ => return None,
                }
            })
        })
    }
    fn emit(self, e: &mut NativeEncoder<'_>) {
        e.u64(self.bytes().count() as u64);
        for b in self.bytes() {
            e.u8(b)
        }
    }
}
struct Parser<'a> {
    text: &'a str,
    pos: usize,
}
impl<'a> Parser<'a> {
    fn peek(&self) -> Decoded<u8> {
        self.text
            .as_bytes()
            .get(self.pos)
            .copied()
            .ok_or(Invalid::Codec)
    }
    fn consume(&mut self, b: u8) -> Decoded<()> {
        if self.peek()? != b {
            return Err(Invalid::Canonical);
        }
        self.pos += 1;
        Ok(())
    }
    fn string(&mut self) -> Decoded<Text<'a>> {
        self.consume(b'"')?;
        let start = self.pos;
        loop {
            let b = self.peek()?;
            if b == b'"' {
                let end = self.pos;
                self.pos += 1;
                return Ok(Text {
                    raw: &self.text[start..end],
                });
            }
            if b < 0x20 {
                return Err(Invalid::Canonical);
            }
            self.pos += 1;
            if b == b'\\' {
                let escape = self.peek()?;
                self.pos += 1;
                match escape {
                    b'"' | b'\\' | b'b' | b't' | b'n' | b'f' | b'r' => {}
                    b'u' => {
                        let code = self
                            .text
                            .as_bytes()
                            .get(self.pos..self.pos + 4)
                            .ok_or(Invalid::Codec)?;
                        if code[0] != b'0' || code[1] != b'0' {
                            return Err(Invalid::Canonical);
                        }
                        let hex = |x| match x {
                            b'0'..=b'9' => Ok(x - b'0'),
                            b'a'..=b'f' => Ok(x - b'a' + 10),
                            _ => Err(Invalid::Canonical),
                        };
                        let v = hex(code[2])? * 16 + hex(code[3])?;
                        if v >= 32 || matches!(v, 8 | 9 | 10 | 12 | 13) {
                            return Err(Invalid::Canonical);
                        }
                        self.pos += 4;
                    }
                    _ => return Err(Invalid::Canonical),
                }
            }
        }
    }
    fn value(&mut self, e: &mut NativeEncoder<'_>, depth: usize) -> Decoded<()> {
        // serde_json's standard Value deserializer has the same recursion bar.
        if depth >= 128 {
            return Err(Invalid::Codec);
        }
        match self.peek()? {
            b'n' => {
                self.literal("null")?;
                e.u8(0)
            }
            b't' => {
                self.literal("true")?;
                e.u8(1);
                e.u8(1)
            }
            b'f' => {
                self.literal("false")?;
                e.u8(1);
                e.u8(0)
            }
            b'"' => {
                e.u8(2);
                self.string()?.emit(e)
            }
            b'[' => {
                self.pos += 1;
                let start = self.pos;
                let mut counter = Parser {
                    text: self.text,
                    pos: start,
                };
                let count = counter.sequence_count(b']', depth)?;
                e.u8(3);
                e.u64(count as u64);
                for i in 0..count {
                    if i != 0 {
                        self.consume(b',')?
                    }
                    self.value(e, depth + 1)?;
                }
                self.consume(b']')?;
            }
            b'{' => {
                self.pos += 1;
                let start = self.pos;
                let mut counter = Parser {
                    text: self.text,
                    pos: start,
                };
                let count = counter.object_count(depth)?;
                e.u8(4);
                e.u64(count as u64);
                let mut prev = None;
                for i in 0..count {
                    if i != 0 {
                        self.consume(b',')?
                    }
                    let key = self.string()?;
                    if prev.is_some_and(|p: Text<'_>| {
                        p.bytes().cmp(key.bytes()) != std::cmp::Ordering::Less
                    }) {
                        return Err(Invalid::Canonical);
                    }
                    prev = Some(key);
                    key.emit(e);
                    self.consume(b':')?;
                    self.value(e, depth + 1)?;
                }
                self.consume(b'}')?;
            }
            b'-' | b'0'..=b'9' => {
                let start = self.pos;
                while self.pos < self.text.len()
                    && matches!(
                        self.text.as_bytes()[self.pos],
                        b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9'
                    )
                {
                    self.pos += 1;
                }
                let raw = &self.text[start..self.pos];
                let n = serde_json::Number::from_str(raw).map_err(|_| Invalid::Codec)?;
                // Scalar formatting uses stack-only serde_json number formatting.
                struct Check<'a> {
                    raw: &'a [u8],
                    pos: usize,
                }
                impl std::io::Write for Check<'_> {
                    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                        if self.raw.get(self.pos..self.pos + b.len()) != Some(b) {
                            return Err(std::io::ErrorKind::InvalidData.into());
                        }
                        self.pos += b.len();
                        Ok(b.len())
                    }
                    fn flush(&mut self) -> std::io::Result<()> {
                        Ok(())
                    }
                }
                let mut w = Check {
                    raw: raw.as_bytes(),
                    pos: 0,
                };
                serde_json::to_writer(&mut w, &n).map_err(|_| Invalid::Canonical)?;
                if w.pos != raw.len() {
                    return Err(Invalid::Canonical);
                }
                if let Some(v) = n.as_i64() {
                    e.u8(5);
                    e.raw(&v.to_be_bytes())
                } else if let Some(v) = n.as_u64() {
                    e.u8(6);
                    e.u64(v)
                } else {
                    e.u8(7);
                    e.u64(n.as_f64().ok_or(Invalid::Codec)?.to_bits())
                }
            }
            _ => return Err(Invalid::Codec),
        }
        Ok(())
    }
    fn literal(&mut self, s: &str) -> Decoded<()> {
        if self.text.as_bytes().get(self.pos..self.pos + s.len()) != Some(s.as_bytes()) {
            return Err(Invalid::Codec);
        }
        self.pos += s.len();
        Ok(())
    }
    fn skip(&mut self, _depth: usize) -> Decoded<()> {
        match self.peek()? {
            b'"' => {
                self.string()?;
            }
            b'[' | b'{' => {
                let mut nesting = 1usize;
                self.pos += 1;
                while nesting != 0 {
                    match self.peek()? {
                        b'"' => {
                            self.string()?;
                        }
                        b'[' | b'{' => {
                            nesting = nesting.checked_add(1).ok_or(Invalid::Codec)?;
                            self.pos += 1;
                        }
                        b']' | b'}' => {
                            nesting -= 1;
                            self.pos += 1;
                        }
                        _ => self.pos += 1,
                    }
                }
            }
            b'n' => self.literal("null")?,
            b't' => self.literal("true")?,
            b'f' => self.literal("false")?,
            b'-' | b'0'..=b'9' => {
                while self.pos < self.text.len()
                    && matches!(
                        self.text.as_bytes()[self.pos],
                        b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9'
                    )
                {
                    self.pos += 1;
                }
            }
            _ => return Err(Invalid::Codec),
        }
        Ok(())
    }
    fn sequence_count(&mut self, end: u8, depth: usize) -> Decoded<usize> {
        if self.peek()? == end {
            return Ok(0);
        }
        let mut count = 0usize;
        loop {
            self.skip(depth + 1)?;
            count = count.checked_add(1).ok_or(Invalid::Codec)?;
            if self.peek()? == end {
                return Ok(count);
            }
            self.consume(b',')?;
        }
    }
    fn object_count(&mut self, depth: usize) -> Decoded<usize> {
        if self.peek()? == b'}' {
            return Ok(0);
        }
        let mut count = 0usize;
        loop {
            self.string()?;
            self.consume(b':')?;
            self.skip(depth + 1)?;
            count = count.checked_add(1).ok_or(Invalid::Codec)?;
            if self.peek()? == b'}' {
                return Ok(count);
            }
            self.consume(b',')?;
        }
    }
}
pub(crate) fn encode(text: &str, e: &mut NativeEncoder<'_>) -> Decoded<()> {
    let mut p = Parser { text, pos: 0 };
    p.value(e, 0)?;
    if p.pos != text.len() {
        return Err(Invalid::Canonical);
    }
    Ok(())
}
pub(crate) fn validate(text: &str) -> Decoded<()> {
    super::native_record_codec::finite_len(|e| encode(text, e)).map(|_| ())
}
// Match serde_json::Value equality, including IEEE equality of signed zero.
pub(crate) fn semantic_eq(a: &str, b: &str) -> Decoded<bool> {
    fn eq(a: &mut Parser<'_>, b: &mut Parser<'_>, depth: usize) -> Decoded<bool> {
        if depth >= 128 {
            return Err(Invalid::Codec);
        }
        let x = a.peek()?;
        let y = b.peek()?;
        if matches!(x, b'-' | b'0'..=b'9') && matches!(y, b'-' | b'0'..=b'9') {
            let ai = a.pos;
            let bi = b.pos;
            a.skip(depth)?;
            b.skip(depth)?;
            return Ok(serde_json::Number::from_str(&a.text[ai..a.pos])
                .map_err(|_| Invalid::Codec)?
                == serde_json::Number::from_str(&b.text[bi..b.pos])
                    .map_err(|_| Invalid::Codec)?);
        }
        if x != y {
            return Ok(false);
        }
        match x {
            b'"' => Ok(a.string()?.bytes().eq(b.string()?.bytes())),
            b'n' => {
                a.literal("null")?;
                b.literal("null")?;
                Ok(true)
            }
            b't' => {
                a.literal("true")?;
                b.literal("true")?;
                Ok(true)
            }
            b'f' => {
                a.literal("false")?;
                b.literal("false")?;
                Ok(true)
            }
            b'[' | b'{' => {
                a.pos += 1;
                b.pos += 1;
                let end = if x == b'[' { b']' } else { b'}' };
                let mut first = true;
                loop {
                    let ae = a.peek()? == end;
                    let be = b.peek()? == end;
                    if ae || be {
                        if ae && be {
                            a.pos += 1;
                            b.pos += 1;
                            return Ok(true);
                        }
                        return Ok(false);
                    }
                    if !first {
                        a.consume(b',')?;
                        b.consume(b',')?
                    }
                    first = false;
                    if x == b'{' {
                        if !a.string()?.bytes().eq(b.string()?.bytes()) {
                            return Ok(false);
                        }
                        a.consume(b':')?;
                        b.consume(b':')?;
                    }
                    if !eq(a, b, depth + 1)? {
                        return Ok(false);
                    }
                }
            }
            _ => Err(Invalid::Codec),
        }
    }
    let mut a = Parser { text: a, pos: 0 };
    let mut b = Parser { text: b, pos: 0 };
    eq(&mut a, &mut b, 0)
}
