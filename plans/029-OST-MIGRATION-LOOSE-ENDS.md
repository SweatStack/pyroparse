# Plan 029 — Closing out the open-sport-taxonomy migration

**Status:** proposed
**Author:** Aart (with Claude)
**Depends on:** open-sport-taxonomy ≥ 0.5.0 (already pinned)

## 1. Context

The core migration from pyroparse's hand-generated `Sport` enum to
[`open-sport-taxonomy`](https://github.com/AartGoossens/open-sport-taxonomy)
(OST) is **functionally complete and green** (281 passed / 2 skipped against
OST 0.5.0):

- `_sport.py`, `_sport_categories.py`, `scripts/generate_sport.py` deleted.
- `Sport` is re-exported from OST; `classify_sport` and the `has_gps`
  specificity heuristic are gone.
- `_decode_sport()` (in `_metadata.py`) is the single FIT→OST decode path.
- The Rust layer is unchanged: it emits FIT enum *name strings*
  (`profile::sport_name` / `sub_sport_name`), with `"unknown"` for
  out-of-range values.

What remains are **loose ends, not blockers** — but they are exactly the kind
of inconsistency the "no technical debt, world-class" bar rejects. They fall
into one design decision, one code chokepoint, test coverage of the new
contract, documentation, changelog/versioning, and one decoupled upstream PR.

## 2. The invariant we are committing to

> **`ActivityMetadata.sport` is always either `None` or the canonical string
> form of a valid OST `Sport`.**

The file-native path already guarantees this (`_decode_sport` only ever returns
`None`, a decoded OST string, or `str(Sport.GENERIC)`). The **only hole** is the
user-supplied metadata override, which today is stored verbatim and can inject
an invalid value (`{"sport": "gravel"}` → stored as `"gravel"`, which is not a
valid OST code — the real code is `cycling.gravel`).

Every item below exists to make that invariant true, enforced, tested, and
documented.

## 3. Decision required (the one real fork)

**How should a user-supplied `sport` override be treated?**

| Option | Behavior on `{"sport": "gravel"}` | Pros | Cons |
|---|---|---|---|
| **A. Strict (recommended)** — `Sport(raw)` | Raises `ValueError` with a message pointing at valid forms | Upholds the invariant; fails fast; override is developer code, not end-user input | A caller using a non-canonical alias must learn the canonical code |
| B. Lenient — `Sport.parse(raw)` | Stores `"gravel"` (non-standard), only normalizes string form | Never raises | Doesn't catch typos, doesn't fix `gravel`→`cycling.gravel`; barely better than verbatim → invariant *not* upheld |
| C. Verbatim (status quo) | Stores `"gravel"` | Zero work | Invariant broken; technical debt |

**Recommendation: A (strict).** The override is a value supplied in *code* by
the integrator, not untrusted end-user input, so failing fast on an invalid
sport is the robust choice and is the only option that preserves the invariant.
`None` is still accepted (explicitly clears the sport). The README's current
`gravel` example becomes `cycling.gravel`.

Everything downstream of this plan assumes Option A; if you pick B or C, items
4.1, 4.2 (the strict tests), and the README "validated" wording change.

## 4. Work items

### 4.1 Enforce the invariant at a single chokepoint  *(code)*

`_merge_metadata(base, overrides)` in `src/pyroparse/_metadata.py` is the one
function all five `Activity.load_*/open_*` call sites funnel overrides through —
the correct and only place to validate.

- When `overrides` contains `"sport"` and the value is not `None`, normalize via
  `str(Sport(value))` (strict). Re-raise as a `ValueError` whose message names
  the offending value and points to `cycling.gravel` / `cycling+stationary`
  style forms and to OST.
- Leave an explicit `sport=None` override as a way to clear the sport.
- No other field needs validation; keep the change surgical.

This keeps the file-native path untouched (already valid) and closes the only
hole. ~10 lines, one function.

### 4.2 Lock the contract with tests  *(tests/test_sport.py)*

Test-first: write these red, then implement 4.1.

1. **Override, valid:** `metadata={"sport": "cycling.gravel"}` →
   `meta.sport == "cycling.gravel"`.
2. **Override, normalized:** a non-canonical-but-parseable modifier order
   round-trips to canonical form (guards that we go through `Sport`).
3. **Override, invalid:** `metadata={"sport": "gravel"}` (or `"notasport"`)
   raises `ValueError` (Option A).
4. **Override, cleared:** `metadata={"sport": None}` → `meta.sport is None`.
5. **Genuinely-unknown FIT value:** simulate the Rust `"unknown"` emission —
   `_decode_sport("unknown", None) == "generic"` and
   `_decode_sport("cycling", "unknown") == "generic"`. (Currently untested;
   this is the real out-of-range path, distinct from the already-covered
   "known-but-unmapped" `downhill` case.)
6. **Invariant sweep (the world-class touch):** parametric test asserting that
   for every FIT `sport` name pyroparse's Rust profile can emit, `_decode_sport`
   returns either `None` or a string that `Sport(...)` accepts — i.e. decode can
   never produce an invalid sport and never raises. Drive it from OST's
   `reference/garmin-fit-sdk/targets.yaml` (or a curated list) to keep it
   bounded and self-updating.

The existing 0.5.0 assertion updates (`cycling+stationary`, generic fallback,
multi-session) already landed in this branch and stay.

### 4.3 Teach the taxonomy in the README  *(docs)*

The README never mentions that `sport` is now a published taxonomy with a
defined vocabulary. Add a short **"Sport values"** subsection near the metadata
docs:

- One paragraph: values come from open-sport-taxonomy; dotted hierarchy
  (`cycling` → `cycling.road`), `+` modifiers (`cycling+stationary`,
  `running.trail+virtual`); link to the OST project; note `pp.Sport` is the OST
  class re-exported.
- **Honest examples.** Replace the 7 `cycling.road` occurrences that overstate
  specificity. The bare-`ride.fit` examples should show `"cycling"` (what a
  road ride with no FIT `sub_sport` actually decodes to); use `cycling.road`
  / `cycling+stationary` only in the taxonomy explainer where the point is the
  notation, not a specific file.
- Fix the override example: `metadata={"sport": "gravel"}` →
  `metadata={"sport": "cycling.gravel"}`, and note overrides are validated
  (per 4.1).
- Update the `ActivityMetadata.sport` field comment to reference OST forms.

### 4.4 Record the change  *(CHANGELOG.md + version)*

Add an `[Unreleased]` section:

- **Changed:** `sport` values now come from open-sport-taxonomy — canonical OST
  strings (`cycling`, `cycling.road`, `cycling+stationary`) instead of the old
  custom enum. `pp.Sport` is now the OST `Sport` class.
- **Added:** `open-sport-taxonomy>=0.5.0,<0.6` runtime dependency; indoor
  activities now decode to `+stationary` modifiers; `metadata` sport overrides
  are validated against the taxonomy.
- **Removed:** the generated `Sport` enum, `classify_sport`, and the GPS-based
  sport-specificity heuristic.

This changes output values → a **minor bump to 0.4.0** under pre-1.0 semver is
appropriate (set in `pyproject.toml` at release time, not in this branch). No
release is triggered here.

### 4.5 Upstream OST fix — decoupled, sequenced  *(separate repo/PR)*

Native indoor rowing (`sport=rowing(15) + sub_sport=indoor_rowing(14)`) decodes
to bare `rowing` because OST 0.5.0's `mappings/garmin_fit.yaml` only maps the
`fitness_equipment/indoor_rowing` (4,14) form to `rowing+stationary`, not the
native-rowing form — an asymmetry vs. cycling/running/walking, which map both.

- Open a one-row PR on open-sport-taxonomy adding
  `target: { sport: 15, sub_sport: 14 } → rowing+stationary`.
- **Do not block this plan on it.** Document the current bare-`rowing` behavior
  as a known limitation tied to the upstream gap (a comment already exists in
  `test_session_sports`).
- *Follow-up after it releases:* bump the pyroparse pin and flip the rowing-leg
  expectations in `test_session_sports` to `rowing+stationary` — zero pyroparse
  code change.

## 5. Sequencing

1. **4.2 (red)** — write the contract tests first.
2. **4.1** — implement override validation; tests go green.
3. **4.3 + 4.4** — README + CHANGELOG.
4. **Verify** — `make test` (full suite) + `make build` (confirm the Rust
   extension is unaffected; no Rust changes in this plan).
5. **Land** — commit on a branch (`migration/ost-loose-ends`) with a clear
   message; **hold the push/PR until reviewed** per standing instruction.
6. **4.5** — upstream PR, in parallel or after; follow-up bump when it ships.

## 6. Risks

- **Strict override could break a downstream caller** passing loose strings.
  Mitigated: no one uses pyroparse yet (stated), so this is the cheapest moment
  to tighten the contract.
- **README example drift** — the chosen examples must match what the bundled
  fixtures actually decode to; verify against `test.fit` (= `cycling`) so docs
  can't silently re-rot.

## 7. Non-goals

- Changing `sport: str | None` to a `Sport` *object* — the string form is
  deliberately serialization-friendly (`to_dict`, Parquet, the utf8 catalog
  column in `_batch.py`). `Sport` validation happens at the boundary; storage
  stays `str`.
- Re-deriving `.road`/`.trainer` specificity from GPS or other heuristics —
  removed on purpose; specificity comes only from the FIT `sub_sport`.
- Any Rust-side change. The decode boundary lives entirely in Python.
- No big-endian / Profile.xlsx / chained-file concerns (out of scope here).
