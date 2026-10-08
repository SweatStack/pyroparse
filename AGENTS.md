# AGENTS.md

Guidance for humans and AI agents working on Pyroparse. For build/test/structure
mechanics, see [DEVELOPING.md](DEVELOPING.md).

## Design principles

Pyroparse is **opinionated and analysis-ready by default**. Opinionated is not the
same as lossless — we already select standard columns, convert units, fold
developer fields, and return a sorted, unique-timestamp series by default.

### Defaults favor the majority, with an explicit opt-out

Choose the default that is **safest and easiest for most developers and most
files**, even when it carries a cost for a minority — provided that cost is
**documented and has an explicit opt-out**. A default may be lossy when it is
clearly the best choice for the common case.

| Default | Lossy? | Opt-out | Why it's the default |
|---|---|---|---|
| `columns=None` → standard columns | yes | `columns="all"` | Most consumers want the canonical set. |
| Sort records by `timestamp` | no (determinate) | — (always on) | A time series should be monotonic. |
| `deduplicate=True` → collapse duplicate timestamps (keep last) | yes | `deduplicate=False` | The vast majority want a unique-per-second, index-ready series. |

**`deduplicate` — the consequence to document loudly:** on sub-second-sampled
files (e.g. a 10 Hz muscle-oxygen sensor writing ~10 records under one 1-second FIT
timestamp), `deduplicate=True` keeps one row per second and discards the rest.
Those consumers pass `deduplicate=False`. This is an accepted, documented tradeoff:
the common case gets a clean series out of the box; the sub-second minority flips
one flag.

### Parse vs. shape

The parse path applies only normalization determinable from the file + FIT spec.
Caller-choice **shaping** rides explicit params (`columns`, `deduplicate`) whose
defaults follow the majority-favoring rule above. Shaping stays as kwargs on the
loaders — we deliberately do **not** add a separate transform namespace for a small
number of operations.

**Device-meaning rules** go one step beyond the file and the spec: the file says
X, the device meant Y. A sensor writes 0 for "no SmO2 reading"; a rowing monitor
repeats the last stroke through a rest. Such a rule belongs in the parse path,
always on, when it meets all of these:

- it is grounded in documented device or profile behavior, not a corpus statistic;
- it is scoped as narrowly as the file allows (a field, a sport);
- it writes the value the device would have written for the state it inferred
  (0 for a stop, see below), and nulls only where no measurement exists (a
  sentinel);
- it leaves a trace in `metadata.extra` when it changed anything (or, for a
  decoding sentinel, is documented in `docs/FIT-FORMAT.md` section 8).

Examples: the SmO2 sentinel, indoor-rowing rests (`cleaned_columns`), pool-swim
reconstruction (`reconstructed_columns`).

### A stop is 0, missing is null

Zero and null mean different things in a record column, and consumers act on
the difference: a time-weighted average, total work and a mean-max curve count
a 0 and skip (or fill) a null. So:

- **A stop is 0.** When the file lets us determine that the athlete stopped, every
  movement column the session records (`power`, `cadence`, `speed`) is 0 there,
  whatever the file held: a held value, a null, or nothing. This is the value a
  device writes when you stop (a bike computer writes 0 W when you coast), so it
  is a measurement of the inferred state, not an invented one.
- **Missing is null.** Null means no measurement exists: a sentinel the device
  writes for "no signal" (an SmO2 of 0, which cannot be measured), a dropout, or
  a column the session never recorded. A stop never creates a column of zeros
  where the session recorded none.
- **Undetermined stays as written.** Only a stop the file determines becomes 0.
  Time before a pool swim's first length, which no Length message describes,
  stays null.

Where it applies today (keep this list complete when adding a rule):

| Rule | The stop | Columns set to 0 | Code |
|---|---|---|---|
| Indoor-rowing rests | more than 3 records whose distance advances ≤ 1 cm | `power`, `cadence`, `speed` | `clean_rowing_rests` |
| Pool-swim rests | an idle Length message (`length_type = idle`) | `speed`, `cadence` | `assign_lengths` |

### Don't trust FIT field 253 for placement

The `timestamp`/end field (253) on session/lap/length messages is device-unreliable
— some devices pin it to a constant. Key record→session/lap/length assignment off
`start_time` (field 2), as `assign_laps` / `assign_lengths` do. See
`docs/FIT-FORMAT.md`.

### Local time is an offset, not a timestamp

The only local-time field in an activity file is `activity.local_timestamp`, the
local twin of that message's own `timestamp` — the *end* of the activity. Derive
`offset = local_timestamp − timestamp` and apply it to `session.start_time`.
Sessions and laps carry no local time, and an offset is never guessed when the
Activity message is missing.

### Resolve fields from the profile, not by hand

Where the FIT profile says how to read a field, generate that reading from
`Profile.xlsx` instead of restating it in code. `device_info.product` and
`device_info.device_type` are read through their subfields (by `manufacturer` and
by `source_type`) with resolvers generated into `src/fit/profile.rs`; list a field
under `[subfields]` in `scripts/profile.toml` to get one. Decoders keep raw
values, and names are derived once, after re-emissions are merged.

### Decode developer fields by their FieldDescription, never by name

A developer field's encoding is whatever its FieldDescription declares
(`fit_base_type_id`, `scale`, `offset`); the same "SmO2" field is a float in one
app and a scaled integer in another. Folding a field into a canonical column is
a naming decision; how its bytes decode is not.
