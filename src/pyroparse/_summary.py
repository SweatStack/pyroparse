"""Groups of session metadata: what the recording device assumed about the
athlete.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, fields


@dataclass
class Athlete:
    """What the recording device assumed about the athlete for this session.

    Values are as of this session and specific to its sport (a watch keeps
    separate heart-rate settings per sport); they may be estimates or out of
    date, and the file does not record where they came from. Never identity
    data.

    - ``max_heart_rate`` — the maximum heart rate the device applied, bpm.
    - ``threshold_heart_rate`` — the lactate-threshold heart rate, bpm.
    """

    max_heart_rate: int | None = None
    threshold_heart_rate: int | None = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def _from_dict(cls, raw: dict | None) -> Athlete:
        """Build from a dict, ignoring keys this version does not know."""
        return cls(**_known(cls, raw))


def _known(cls: type, raw: dict | None) -> dict:
    names = {f.name for f in fields(cls)}
    return {k: v for k, v in (raw or {}).items() if k in names}
