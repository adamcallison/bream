"""Core of the package."""

from bream.core._definitions import (
    Batch,
    BatchRequest,
    Jsonable,
    JsonableNonNull,
    Pathlike,
    Source,
    StreamOptions,
    StreamStatus,
)
from bream.core._source_collection import SourceCollection
from bream.core._stream import Stream

# self-assignment to help sphinx autodoc detect docstring:
Pathlike = Pathlike  # noqa: PLW0127
"""A pathlib.Path object or any type of path provided by cloudpathlib"""

JsonableNonNull = JsonableNonNull  # noqa: PLW0127
"""An object that can be turned into a non-null json blob."""

Jsonable = Jsonable  # noqa: PLW0127
"""An object that can be turned into a json blob."""

__all__ = [
    "Batch",
    "BatchRequest",
    "Jsonable",
    "JsonableNonNull",
    "Pathlike",
    "Source",
    "SourceCollection",
    "Stream",
    "StreamOptions",
    "StreamStatus",
]
