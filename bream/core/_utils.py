"""Utility functions for core functionality."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bream.core._definitions import JsonableNonNull, Pathlike


def dump_json_atomically(data: JsonableNonNull, dst: Pathlike, tmp: Pathlike) -> None:
    """Write JSON data to a file atomically using a temporary file.

    Args:
        data: The JSON-serializable data to write
        dst: The destination file path
        tmp: The temporary file path to use during writing

    """
    with tmp.open("w") as f:
        json.dump(data, f)
    tmp.rename(dst)
