"""Hierarchical sport categorization.

Ported from SweatStack. Maps FIT sport/sub_sport values to a standardized
dot-notation hierarchy (e.g. ``"cycling.road"``, ``"running.trail"``).
"""

from __future__ import annotations

from enum import Enum


class _SportBase(str, Enum):
    def __str__(self) -> str:
        return self.value

# ---------------------------------------------------------------------------
# Hierarchy — add new sports/variants here.
# ---------------------------------------------------------------------------

SPORT_CATEGORIES: dict = {
    "cycling": {
        "road": None,
        "tt": None,
        "cyclocross": None,
        "gravel": None,
        "mountainbike": None,
        "track": {
            "250m": None,
            "333m": None,
        },
        "trainer": None,
    },
    "running": {
        "road": None,
        "track": {
            "200m": None,
            "400m": None,
        },
        "trail": None,
        "treadmill": None,
    },
    "walking": {
        "hiking": None,
    },
    "cross_country_skiing": {
        "classic": None,
        "skate": None,
        "backcountry": None,
        "ergometer": None,
    },
    "rowing": {
        "ergometer": None,
        "indoor": None,
        "regatta": None,
        "fixed_seat": None,
        "coastal": None,
    },
    "swimming": {
        "pool": {
            "50m": None,
            "25m": None,
            "25y": None,
            "33m": None,
        },
        "open_water": None,
        "flume": None,
    },
    "generic": None,
    "unknown": None,
}


def _flatten(tree: dict, prefix: str = "") -> list[str]:
    result = []
    for key, children in tree.items():
        value = f"{prefix}{key}" if prefix else key
        result.append(value)
        if children:
            result.extend(_flatten(children, f"{value}."))
    return result


Sport = _SportBase(
    "Sport",
    {cat.replace(".", "_").upper(): cat for cat in _flatten(SPORT_CATEGORIES)},
)
"""Hierarchical sport enum with dot-notation values.

Examples::

    Sport.CYCLING           # "cycling"
    Sport.CYCLING_ROAD      # "cycling.road"
    Sport.RUNNING_TRAIL     # "running.trail"
"""


def classify_sport(
    sport: str | None,
    sub_sport: str | None = None,
    *,
    has_gps: bool = False,
) -> Sport:
    """Map FIT sport/sub_sport strings to a :class:`Sport` category.

    Uses the same heuristics as SweatStack: explicit sub_sport mappings
    where possible, GPS presence as a tiebreaker for indoor/outdoor.
    """
    match sport:
        case "cycling":
            match sub_sport:
                case "indoor_cycling":
                    return Sport.CYCLING_TRAINER
                case "road":
                    return Sport.CYCLING_ROAD
                case "mountain" | "downhill":
                    return Sport.CYCLING_MOUNTAINBIKE
                case "cyclocross":
                    return Sport.CYCLING_CYCLOCROSS
                case "track_cycling":
                    return Sport.CYCLING_TRACK
                case "gravel":
                    return Sport.CYCLING_GRAVEL
                case _:
                    return Sport.CYCLING_ROAD if has_gps else Sport.CYCLING
        case "running":
            match sub_sport:
                case "treadmill":
                    return Sport.RUNNING_TREADMILL
                case "track":
                    return Sport.RUNNING_TRACK
                case "trail":
                    return Sport.RUNNING_TRAIL
                case _:
                    return Sport.RUNNING_ROAD if has_gps else Sport.RUNNING
        case "walking":
            return Sport.WALKING
        case "hiking":
            return Sport.WALKING_HIKING
        case "swimming":
            return Sport.SWIMMING
        case "cross_country_skiing":
            if sub_sport == "skate_skiing":
                return Sport.CROSS_COUNTRY_SKIING_SKATE
            return Sport.CROSS_COUNTRY_SKIING_CLASSIC
        case "rowing":
            if sub_sport == "indoor_rowing":
                return Sport.ROWING_ERGOMETER
            return Sport.ROWING
        case "e_biking":
            return Sport.CYCLING_ROAD if has_gps else Sport.CYCLING
        case "generic":
            return Sport.GENERIC
        case _:
            return Sport.UNKNOWN
