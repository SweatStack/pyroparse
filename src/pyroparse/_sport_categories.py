"""Single source of truth for the sport hierarchy.

Both the code-generation script (``scripts/generate_sport.py``) and runtime
lookups derive from the structures defined here.
"""

from __future__ import annotations

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


def flatten(tree: dict, prefix: str = "") -> list[str]:
    """Flatten the hierarchy into dot-notation strings.

    >>> flatten({"a": {"b": None, "c": {"d": None}}})
    ['a', 'a.b', 'a.c', 'a.c.d']
    """
    result: list[str] = []
    for key, children in tree.items():
        value = f"{prefix}{key}" if prefix else key
        result.append(value)
        if children:
            result.extend(flatten(children, f"{value}."))
    return result
