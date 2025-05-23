"""Stream batches of data from arbitrary sources."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from enum import Enum, auto
from threading import Thread
from time import sleep, time
from typing import TYPE_CHECKING

from bream._exceptions import StreamLogicalError
from bream.core._checkpoint import SourceCheckpointer, SourcesCheckpointer
from bream.core._definitions import Batches, Pathlike, Source, StreamOptions, StreamStatus

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Sequence

STREAM_DEFINITION_FILE_NAME = "definition"

WAITHELPER_ITERATION_INTERVAL = 0.5


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


class _WaitHelperStates(Enum):
    wait = auto()
    proceed = auto()


class _WaitHelper:
    def __init__(self, wait_seconds: float, iter_interval: float) -> None:
        self._wait_seconds = wait_seconds
        self._iter_interval = iter_interval
        self._tick = -float("inf")

    def __call__(self) -> Generator[_WaitHelperStates]:
        while True:
            time_ = time()
            time_since_tick, self._tick = time() - self._tick, time_
            remaining_wait_seconds = self._wait_seconds - time_since_tick
            if remaining_wait_seconds <= 0:
                sleep_time = 0.0
                state_to_yield = _WaitHelperStates.proceed
            elif remaining_wait_seconds <= self._iter_interval:
                sleep_time = remaining_wait_seconds
                state_to_yield = _WaitHelperStates.proceed
            else:
                sleep_time = self._iter_interval
                state_to_yield = _WaitHelperStates.wait

            if sleep_time:
                sleep(sleep_time)

            yield state_to_yield


class Stream:
    """A stream of batches of data.

    Parameters
    ----------
    sources
        The sources of data batches that this stream will be used to process.

    stream_path
        Path to a location this stream will use to tracks its progress.

    """

    def __init__(
        self,
        sources: Sequence[Source],
        stream_path: Pathlike,
        stream_options: StreamOptions | None = None,
    ) -> None:
        """Initialize stream."""
        source_names = [source.name for source in sources]
        if len(source_names) != len(set(source_names)):
            msg = "Duplicate source names detected."
            raise StreamLogicalError(msg)
        self._options = stream_options or StreamOptions()

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

    @property
    def options(self) -> StreamOptions:
        return self._options

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
        waiter = _WaitHelper(min_batch_seconds, WAITHELPER_ITERATION_INTERVAL)

        try:
            for waitstate in waiter():
                if self._stop:
                    break
                if waitstate == _WaitHelperStates.wait:
                    continue
                with self._sources_checkpointer.batch() as batch:
                    if batch is not None:
                        func(batch)
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
        if not self._options.repeat_failed_batch_exactly:
            for source_checkpointer in self._sources_checkpointer:
                source_checkpointer.unmark_uncommitted_offset()
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
