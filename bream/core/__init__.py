"""Core of the package."""

from bream.core._definitions import (
    Batch,
    Batches,
    BatchRequest,
    Pathlike,
    Source,
    StreamOptions,
    StreamStatus,
)
from bream.core._stream import Stream

# self-assignment to help sphinx autodoc detect docstring:
Pathlike = Pathlike  # noqa: PLW0127
"""A pathlib.Path object or any type of path provided by cloudpathlib"""

__all__ = [
    "Batch",
    "BatchRequest",
    "Batches",
    "Pathlike",
    "Source",
    "Stream",
    "StreamOptions",
    "StreamStatus",
]
