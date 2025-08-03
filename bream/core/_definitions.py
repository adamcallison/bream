"""Definitions."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
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
    """A batch returned by a data source.

    Parameters
    ----------
        data
            The data for this batch.
            The source offset that was read to for this batch.

    """

    data: Any
    read_to: JsonableNonNull


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
    """Status information of a stream.

    Parameters
    ----------
    started
        Whether the stream has been started.
    active
        Whether the stream is currently active.
    error
        The error, if the stream terminated with an error.

    """

    started: bool = False
    active: bool = False
    error: Exception | None = None


@dataclass(frozen=True)
class StreamOptions:
    """Options for a stream."""

    repeat_failed_batch_exactly: bool = True
    """Whether to repeat a failed batch exactly on a stream restart, regardless of
    source configuration.
    """
