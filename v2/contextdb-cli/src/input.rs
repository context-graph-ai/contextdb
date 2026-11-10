//! Finds where a SQL statement ends in the text typed or piped so far.

/// What the text gathered so far holds.
#[derive(Debug, PartialEq, Eq)]
pub enum Scan {
    /// A whole statement: the text up to this byte offset, which is just past its `;`.
    Ends(usize),
    /// The start of a statement that the next line continues.
    Open,
    /// Nothing but spaces and closed comments: no statement has begun.
    Blank,
}

/// Looks for the first statement in `text`, as `psql` and `sqlite3` do: a statement ends at the
/// first `;` that is not inside a quoted string, a quoted name, a `--` comment or a `/* */`
/// comment, however many lines it spans.
///
/// A `/*` that is not closed yet is [`Scan::Open`]: the next line continues the comment.
pub fn scan(text: &str) -> Scan {
    let mut chars = text.char_indices().peekable();
    let mut blank = true;
    while let Some((at, c)) = chars.next() {
        let next = chars.peek().map(|(_, next)| *next);
        match c {
            ';' => return Scan::Ends(at + 1),
            '-' if next == Some('-') => {
                chars.by_ref().find(|(_, c)| *c == '\n');
            }
            '/' if next == Some('*') => {
                chars.next();
                let mut star = false;
                let closed = chars.by_ref().any(|(_, c)| {
                    let closes = star && c == '/';
                    star = c == '*';
                    closes
                });
                if !closed {
                    return Scan::Open;
                }
            }
            '\'' | '"' => {
                blank = false;
                // A quote written twice inside its own quotes closes and opens again, which
                // reads the same as staying inside.
                chars.by_ref().find(|(_, close)| *close == c);
            }
            _ => blank &= c.is_whitespace(),
        }
    }
    if blank { Scan::Blank } else { Scan::Open }
}
