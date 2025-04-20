"""Stream batches of data from arbitrary sources."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from threading import Thread
from time import sleep, time
from typing import TYPE_CHECKING

from bream._exceptions import StreamLogicalError
from bream.core._checkpoint import SourceCheckpointer, SourcesCheckpointer
from bream.core._definitions import Batches, Pathlike, Source, StreamStatus

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Sequence

STREAM_DEFINITION_FILE_NAME = "definition"


@dataclass
class _StreamDefinition:
    source_names: list[str]


class _StreamDefinitionFile:
    def __init__(self, path: Pathlike) -> None:
        self._path = path

    @property
    def exists(self) -> bool:
        return self._path.is_file()

    def load(self) -> _StreamDefinition | None:
        if not self.exists:
            return None
        with self._path.open("r") as f:
            return _StreamDefinition(**json.load(f))

    def save(self, definition: _StreamDefinition) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("w") as f:
            json.dump(asdict(definition), f)


@dataclass
class _Wait:
    wait_seconds: float


class _Waiter:
    def __init__(self, seconds_between_ticks: float) -> None:
        self._seconds_between_ticks = seconds_between_ticks
        self._tick = -float("inf")

    def __call__(self) -> Generator[_Wait]:
        while True:
            time_since_tick = time() - self._tick
            sleep_time = max(0, self._seconds_between_ticks - time_since_tick)
            if sleep_time > 0:
                sleep(sleep_time)
            self._tick = time()
            yield _Wait(sleep_time)


class Stream:
    """A stream of batches of data.

    Parameters
    ----------
    sources
        The sources of data batches that this stream will be used to process.

    stream_path
        Path to a location this stream will use to tracks its progress.

    """

    def __init__(self, sources: Sequence[Source], stream_path: Pathlike) -> None:
        """Initialize stream."""
        source_names = [source.name for source in sources]
        if len(source_names) != len(set(source_names)):
            msg = "Duplicate source names detected."
            raise StreamLogicalError(msg)

        self._sources_checkpointer = SourcesCheckpointer(
            *(SourceCheckpointer(source, stream_path) for source in sources),
        )
        self._definition = _StreamDefinition(source_names=[source.name for source in sources])
        self._definition_file = _StreamDefinitionFile(stream_path / STREAM_DEFINITION_FILE_NAME)
        self._validate_definition_against_existing()
        self._started = False
        self._stop = False
        self._thread: Thread | None = None
        self._error: Exception | None = None

    def _validate_definition_against_existing(self) -> None:
        existing_definition = self._definition_file.load()
        if existing_definition is None:
            return
        if self._definition != existing_definition:
            msg = (
                "Attempted redefinition of stream from "
                f"{existing_definition} to {self._definition}."
            )
            raise StreamLogicalError(msg)

    def _main_loop(self, func: Callable[[Batches], None], min_batch_seconds: float) -> None:
        waiter = _Waiter(min_batch_seconds)

        try:
            for _ in waiter():
                if self._stop:
                    break
                with self._sources_checkpointer.batch() as batch:
                    if batch is not None:
                        func(batch)
                if self._stop:
                    break
        except Exception as e:  # noqa: BLE001
            self._error = e

    def start(self, func: Callable[[Batches], None], min_batch_seconds: float) -> None:
        """Start the stream in a background thread.

        Parameters
        ----------
        func
            The batch function that will process each batch of data.
        min_batch_seconds
            The minimum number of seconds between each attempt to fetch a batch.

        """
        if self._started:
            msg = "Cannot start stream twice."
            raise StreamLogicalError(msg)
        if not self._definition_file.exists:
            self._definition_file.save(self._definition)
        self._thread = Thread(target=self._main_loop, args=(func, min_batch_seconds), daemon=True)
        self._started = True
        self._thread.start()

    def stop(self, *, blocking: bool = True) -> None:
        """Mark the running stream to be stopped gracefully.

        Parameters
        ----------
        blocking
            Whether the current thread should wait until the stream is stopped before proceeding.

        """
        if not self._started:
            msg = "Cannot stop a stream that hasn't been started."
            raise StreamLogicalError(msg)
        self._stop = True
        if blocking:
            self.wait()

    def wait(self) -> None:
        """If the stream has been started, block until the stream terminates."""
        if self._thread:
            self._thread.join()

    @property
    def status(self) -> StreamStatus:
        """Status of the stream.

        Returns
        -------
        StreamStatus
            The current status of the stream.

        """
        return StreamStatus(
            started=self._started,
            active=(self._thread is not None and self._thread.is_alive()),
            error=self._error,
        )
