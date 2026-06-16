"""Tests for FIT sport/sub_sport → OST Sport decoding in metadata.

The sport taxonomy itself is owned and tested by ``open-sport-taxonomy``.
These tests cover only the pyroparse side: the thin ``_decode_sport``
wrapper that maps raw FIT enum names to the canonical OST string, the
``_merge_metadata`` override path that validates caller-supplied sports,
and the invariant that ``ActivityMetadata.sport`` is always ``None`` or a
valid OST sport string.
"""

from __future__ import annotations

import pytest

from pyroparse import Sport
from pyroparse._metadata import ActivityMetadata, _decode_sport, _merge_metadata


def _is_valid_sport(value: str) -> bool:
    """True if *value* is the canonical string form of a real OST sport."""
    try:
        return str(Sport(value)) == value
    except ValueError:
        return False


class TestSportReexport:
    def test_sport_is_ost_class(self):
        from open_sport_taxonomy import Sport as OstSport
        assert Sport is OstSport


class TestDecodeSport:
    @pytest.mark.parametrize(
        "sport, sub_sport, expected",
        [
            ("cycling", "road",    "cycling.road"),
            ("cycling", "gravel_cycling", "cycling.gravel"),
            # Since OST 0.9.0 the generic Garmin code decodes to the dominant
            # discipline: cycling/generic -> cycling.road, running/generic ->
            # running.road (modern devices write road rides/runs to generic).
            ("cycling", "generic", "cycling.road"),
            ("cycling", None,      "cycling.road"),
            ("cycling", "indoor_cycling", "cycling+stationary"),
            ("running", "trail",   "running.trail"),
            ("running", "treadmill", "running+stationary"),
            ("running", "generic", "running.road"),
            ("swimming", "open_water", "swimming.open_water"),
        ],
    )
    def test_known_pairs(self, sport, sub_sport, expected):
        assert _decode_sport(sport, sub_sport) == expected

    def test_no_sport_returns_none(self):
        assert _decode_sport(None, None) is None
        assert _decode_sport(None, "road") is None

    def test_unknown_fit_name_falls_back_to_generic(self):
        assert _decode_sport("paragliding", None) == "generic"

    def test_downhill_sub_sport_maps_to_mountain(self):
        # Since OST 0.8.4 ``cycling/downhill`` decodes to ``cycling.mountain``
        # (it previously had no mapping and fell back to ``generic``).
        assert _decode_sport("cycling", "downhill") == "cycling.mountain"

    def test_genuinely_unknown_fit_value_falls_back_to_generic(self):
        # Out-of-range FIT enum values are emitted by the Rust layer as the
        # literal string ``"unknown"`` (see ``profile::sport_name``). OST
        # rejects that name, and the wrapper shields the caller with
        # ``generic`` rather than letting the ValueError escape.
        assert _decode_sport("unknown", None) == "generic"
        assert _decode_sport("cycling", "unknown") == "generic"


# A representative breadth of FIT sport enum names (from the Garmin FIT SDK
# ``sport`` enum, mirrored in ``src/fit/profile.rs``) plus the ``"unknown"``
# sentinel the Rust layer emits for out-of-range values. The point is not
# exhaustiveness but breadth: it guards the wrapper's shielding contract.
_FIT_SPORT_NAMES = [
    "generic", "running", "cycling", "transition", "fitness_equipment",
    "swimming", "walking", "rowing", "hiking", "mountaineering",
    "cross_country_skiing", "alpine_skiing", "snowboarding", "paddling",
    "rock_climbing", "sailing", "ice_skating", "inline_skating",
    "snowshoeing", "stand_up_paddleboarding", "golf", "horseback_riding",
    "e_biking", "motorcycling", "multisport", "training", "unknown",
]


class TestDecodeInvariant:
    """``_decode_sport`` must never raise and never produce an invalid
    sport — its only outputs are ``None`` or a canonical OST sport string."""

    @pytest.mark.parametrize("sport", _FIT_SPORT_NAMES)
    @pytest.mark.parametrize("sub_sport", [None, "generic", "indoor_cycling", "unknown"])
    def test_output_is_none_or_valid_sport(self, sport, sub_sport):
        result = _decode_sport(sport, sub_sport)
        assert result is None or _is_valid_sport(result), (
            f"_decode_sport({sport!r}, {sub_sport!r}) produced {result!r}, "
            "which is not a valid OST sport"
        )


class TestSportOverride:
    """Caller-supplied ``metadata={"sport": ...}`` overrides are validated
    against the taxonomy so the stored sport is always canonical or None."""

    def _base(self) -> ActivityMetadata:
        return ActivityMetadata(sport="cycling")

    def test_valid_override_is_kept(self):
        merged = _merge_metadata(self._base(), {"sport": "cycling.gravel"})
        assert merged.sport == "cycling.gravel"

    def test_override_is_normalized_to_canonical_form(self):
        # Modifier order is normalized by the taxonomy, proving the value
        # round-trips through ``Sport`` rather than being stored verbatim.
        merged = _merge_metadata(
            self._base(), {"sport": "cycling.road+virtual+stationary"}
        )
        assert merged.sport == "cycling.road+stationary+virtual"

    @pytest.mark.parametrize("bad", ["gravel", "notasport", "Cycling", "cycling.banana"])
    def test_invalid_override_raises(self, bad):
        with pytest.raises(ValueError):
            _merge_metadata(self._base(), {"sport": bad})

    def test_none_override_clears_sport(self):
        merged = _merge_metadata(self._base(), {"sport": None})
        assert merged.sport is None

    def test_override_of_other_fields_is_untouched(self):
        # Non-sport overrides must not be affected by sport validation.
        merged = _merge_metadata(self._base(), {"name": "Morning ride"})
        assert merged.name == "Morning ride"
        assert merged.sport == "cycling"
