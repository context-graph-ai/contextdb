# contextdb-parser — Agent Rules

Repo-wide rules are in the [root `AGENTS.md`](../../AGENTS.md). This file carries the one
discipline that is local to this crate.

## Char-boundary discipline

**Every fixed-width lookahead into the input string must be boundary-safe.** This crate scans raw
user SQL, and user SQL contains multi-byte UTF-8 — in string literals, in comments, in quoted
identifiers. A byte-offset slice built from `idx + <ascii token>.len()` is a *fixed-width* window:
`idx` is a char boundary because it came from `char_indices()`, but `idx + len` is not, the moment a
multi-byte character starts inside that window. `&s[a..b]` on a non-boundary **panics**, and a panic
in the parser takes down the caller's process on valid input.

So: never index with `&input[a..b]` on a window whose end you computed. Use `str::get(a..b)`, which
returns `None` at a non-boundary instead of panicking — and a window that ends mid-character can
never equal an ASCII token anyway, so `None` is also the correct answer.

The shipped pattern, `contains_token_outside_strings` in `src/parser.rs`:

```rust
// `idx` is a char boundary, but `idx + token.len()` need not be: a
// multi-byte character starting inside the fixed-width window would put
// it mid-character. `get` returns None there instead of panicking, and
// a window that ends mid-character can never equal an ASCII token.
if is_word_boundary(input, idx.saturating_sub(1))
    && input
        .get(idx..idx + token.len())
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(token))
    && is_word_boundary(input, idx + token.len())
{
    return true;
}
```

Note that `is_word_boundary` reads `s.as_bytes()[idx]` — that is safe *because* it is only ever
called with an index it first bounds-checks (`idx >= s.len()` returns `true`), and it only asks
"is this byte ASCII-alphanumeric or `_`". A continuation byte of a multi-byte character is neither,
so it reads as a boundary, which is the right answer. Byte inspection is fine; byte *slicing* is not.

Applies to any new scanner you add here — keyword sequence detection, literal skipping, comment
skipping. Prefer iterating `char_indices()` and comparing characters over reconstructing substrings
at all.

### Regression coverage

`tests/utf8_multibyte_boundary_tests.rs` carries a generated matrix that drives multi-byte
characters through string literals, comments and identifiers at every offset around the scanner's
lookahead window.
A new scanner belongs in that matrix. Do not delete a case from it — see the mutation-evidence rule
in the root file.
