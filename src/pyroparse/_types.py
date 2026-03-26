"""Shared type aliases."""

from __future__ import annotations

import os
from typing import BinaryIO

Source = str | os.PathLike[str] | bytes | BinaryIO
PathSource = str | os.PathLike[str]
