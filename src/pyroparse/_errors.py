from __future__ import annotations


class MultipleActivitiesError(Exception):
    """Raised when Activity.load_fit() encounters a multi-activity FIT file."""

    def __init__(self, count: int) -> None:
        self.count = count
        super().__init__(
            f"FIT file contains {count} activities. "
            f"Use Session.load_fit() to load multi-activity files."
        )


# Map of file types to the class that handles them.
_FILE_TYPE_CLASSES = {
    "activity": "Activity or Session",
    "course": "Course",
}


class FileTypeMismatchError(Exception):
    """Raised when a FIT file's type doesn't match the parser used."""

    def __init__(self, expected: str, actual: str) -> None:
        self.expected = expected
        self.actual = actual
        hint = _FILE_TYPE_CLASSES.get(actual, actual)
        a_expected = "an" if expected[0] in "aeiou" else "a"
        a_actual = "an" if actual[0] in "aeiou" else "a"
        super().__init__(
            f"Expected {a_expected} {expected} file, got {a_actual} {actual} file. "
            f"Use {hint}.load_fit() instead."
        )
