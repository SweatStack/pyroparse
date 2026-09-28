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

### Decode developer fields by their FieldDescription, never by name

A developer field's encoding is whatever its FieldDescription declares
(`fit_base_type_id`, `scale`, `offset`); the same "SmO2" field is a float in one
app and a scaled integer in another. Folding a field into a canonical column is
a naming decision; how its bytes decode is not.
