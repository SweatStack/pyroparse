from __future__ import annotations


class MultipleActivitiesError(Exception):
    """Raised when Activity.load_fit() encounters a multi-activity FIT file."""

    def __init__(self, count: int) -> None:
        self.count = count
        super().__init__(
            f"FIT file contains {count} activities. "
            f"Use Session.load_fit() to load multi-activity files."
        )
