# Performance result: <promise> on <machine>, <date>

Copy this file to `docs/performance/results/<unique-id>.md` and fill every `<...>` from a
measurement. A row of [the bars](../README.md#the-bars-of-the-engine-in-v2) takes its last figure from
here.

Rules:

- **Never invent a value.** A field that was not recorded is `unknown`; one that does not apply is
  `not applicable`, with the reason.
- **A figure is not a promise.** The promise is the product's own words, copied from its root
  document, or "no figure promised".
- **A speed or size promise names the test that guards it by counting work**, or says "none yet".
- **Name the machine** (processor, cores, memory) and say whether it is a user's machine.
- Never paste hostnames, user names, absolute paths or secrets.

## Promise

- Promise, in the product's words: `<quote, or "no figure promised">`
- Counted test that guards it: `<test name and file, or "none yet">`

## Measurement

- Date: `<YYYY-MM-DD>`; revision measured: `<full SHA>`
- Program and command: `<exact command, local paths replaced by placeholders>`
- Data: `<what it holds, rows or vectors, dimension, bytes>`
- Passes and statistic: `<number of passes; p50, p95>`; threads: `<n>`; memory limit: `<bytes>`
- Machine: `<processor, cores, memory>`; operating system: `<...>`; user's machine: `<yes | no>`
- Settings that can change the result: `<cargo features, environment, search mode>`

## Figures

| Row | Figure | Unit | Previous figure and its date |
|---|---|---|---|
| `<what was measured>` | `<value>` | `<ms, MiB, rows per second, times its data>` | `<value and date, or "none">` |

For a search, also give recall (the share of the true ten nearest found) and the number of
questions it was taken over. For a size, give the data's own size beside the store's.

## Limitations

- `<what was not measured, what could differ on another machine>`
