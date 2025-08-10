"""Definitions."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeAlias

from cloudpathlib import AnyPath

JsonableNonNull = int | float | list["Jsonable"] | dict[str, "Jsonable"]
Jsonable = JsonableNonNull | None


@dataclass(frozen=True)
class BatchRequest:
    """A request for a batch sent to a data source."""

    read_from_after: JsonableNonNull | None
    read_to: JsonableNonNull | None


@dataclass(frozen=True)
class Batch:
    """A batch returned by a data source."""

    data: Any
    """The data for this batch."""

    read_to: JsonableNonNull
    """The source offset that was read to for this batch."""


class Source(ABC):
    """Abstract base class for bream source definitions."""

    #: Name of the source. This is used by bream as a source identifier.
    name: str

    @abstractmethod
    def read(self, batch_request: BatchRequest) -> Batch | None:
        """Read data from the source according to the batch rquest.

        Bream source definitions must implement this.

        Parameters
        ----------
        batch_request
            A request for a batch of data, as prepared by bream.

        Returns
        -------
        Batch | None
            The batch of data if data was available, otherwise None.

        """


class _AnyPathTypingWrapper(Path, ABC):
    """Virtual subclass of `pathlib.Path` for typing. Dispatches to `cloudpathlib.AnyPath`."""

    def __new__(cls, path: str) -> _AnyPathTypingWrapper:  # noqa: PYI034
        """Dispatch construction to `cloudpathlib.AnyPath`."""
        return AnyPath(path)  # type: ignore [return-value]  # pragma: no cover


Pathlike: TypeAlias = "Path | _AnyPathTypingWrapper"


@dataclass(frozen=True)
class StreamStatus:
    """Status information of a stream."""

    started: bool = False
    """Whether the stream has been started."""

    active: bool = False
    """Whether the stream is currently active."""

    retry_count: int = 0
    """The number of times the stream has retried."""

    errors: list[Exception] = field(default_factory=list)
    """The errors the stream has retried after, or terminated with, in order of occurrence."""


@dataclass(frozen=True)
class StreamOptions:
    """Options for a stream."""

    repeat_failed_batch_exactly: bool = True
    """Whether to repeat a failed batch exactly on a stream restart, regardless of
    source configuration.
    """

    max_retry_count: int | None = 0
    """Number of times the stream should attempt to recover on error, or None if it should retry
    infinitely.
    """

    min_seconds_between_retries: float | None = None
    """Minimum number of seconds after the start of a failed batch that the batch should be retried
    or None if `min_batch_seconds` specified on stream start should be used.
    """
