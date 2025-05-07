"""Manage checkpoints for stream progress."""

from __future__ import annotations

from abc import ABC
from contextlib import ExitStack, contextmanager, suppress
from typing import TYPE_CHECKING

from bream._exceptions import SourceProtocolError
from bream.core._definitions import Batch, Batches, BatchRequest, Pathlike, Source

if TYPE_CHECKING:
    from collections.abc import Generator

OFFSET_MARK_NAME = "offset"
COMMIT_MARK_NAME = "commit"
DEFAULT_RETAIN_NUM_MARKS = 100


class _CheckpointMarkManager(ABC):
    _mark_name: str

    def __init__(self, checkpoint_mark_path: Pathlike, retain_marks: int) -> None:
        self._path = checkpoint_mark_path
        self._retain_marks = retain_marks

    @property
    def current_mark(self) -> int | None:
        marks = self._get_marks()
        if not marks:
            return None
        return max(marks)

    def mark(self, new_mark: int) -> None:
        current_mark = self.current_mark
        if current_mark is not None and new_mark <= current_mark:
            msg = (
                f"Cannot mark {self._mark_name} {new_mark} because current "
                f"{self._mark_name} is {current_mark}"
            )
            raise ValueError(msg)
        self._ensure_offset_path_exists()
        with (self._path / f"{new_mark}").open("w") as f:
            f.write("")
        self._clean()

    def unmark_current(self) -> None:
        current_mark = self.current_mark
        current_mark_path = self._path / f"{current_mark}"
        current_mark_path.unlink()

    def _ensure_offset_path_exists(self) -> None:
        self._path.mkdir(parents=True, exist_ok=True)

    def _get_marks(self) -> list[int]:
        possible = [p.name for p in self._path.glob("*")]
        actual = []
        for x in possible:
            with suppress(ValueError):
                actual.append(int(x))
        return sorted(actual)

    def _clean(self) -> None:
        marks = self._get_marks()
        num_delete = max(0, len(marks) - self._retain_marks)
        for mark in marks[:num_delete]:
            (self._path / f"{mark}").unlink()


class _OffsetManager(_CheckpointMarkManager):
    _mark_name = OFFSET_MARK_NAME


class _CommitManager(_CheckpointMarkManager):
    _mark_name = COMMIT_MARK_NAME


class SourceCheckpointer:
    """Manage checkpoints for a stream's progress through a data source."""

    def __init__(
        self,
        source: Source,
        stream_path: Pathlike,
        retain_marks: int = DEFAULT_RETAIN_NUM_MARKS,
    ) -> None:
        """Initialize source checkpointer."""
        self._source = source
        self._offset_manager = _OffsetManager(
            stream_path / source.name / OFFSET_MARK_NAME,
            retain_marks,
        )
        self._commit_manager = _CommitManager(
            stream_path / source.name / COMMIT_MARK_NAME,
            retain_marks,
        )

    @property
    def source_name(self) -> str:
        """Name of the source being managed."""
        return self._source.name

    @contextmanager
    def batch(self) -> Generator[Batch | None]:
        """Get a checkpoint-managed batch of data."""
        current_commit = self._current_commit
        uncommitted_offset = self._uncommitted_offset
        batch_request = BatchRequest(
            read_from_after=current_commit if current_commit is not None else None,
            read_to=uncommitted_offset,
        )
        maybe_batch = self._source.read(batch_request)
        if maybe_batch is None:
            yield None
        else:
            batch = maybe_batch
            if uncommitted_offset is not None:
                if batch.read_to != uncommitted_offset:
                    msg = (
                        f"Source was told to read to offset {batch_request.read_to} "
                        f"but claims to have read to {batch.read_to}."
                    )
                    raise SourceProtocolError(msg)
            else:
                if (
                    batch_request.read_from_after is not None
                    and batch.read_to <= batch_request.read_from_after
                ):
                    msg = (
                        "Source was told to read from an offset after "
                        f"{batch_request.read_from_after} but claims to have read to "
                        f"{batch.read_to}."
                    )
                    raise SourceProtocolError(msg)
                self._mark_offset(batch.read_to)
            yield batch
            self._mark_commit(batch.read_to)

    def unmark_uncommitted_offset(self) -> None:
        """If there is an uncommitted offset, unmark it."""
        uncommitted_offset = self._uncommitted_offset
        if uncommitted_offset is None:
            return
        self._offset_manager.unmark_current()

    @property
    def _current_offset(self) -> int | None:
        return self._offset_manager.current_mark

    @property
    def _current_commit(self) -> int | None:
        return self._commit_manager.current_mark

    @property
    def _uncommitted_offset(self) -> int | None:
        return None if self._current_commit == self._current_offset else self._current_offset

    def _mark_offset(self, offset: int) -> None:
        self._offset_manager.mark(offset)

    def _mark_commit(self, commit: int) -> None:
        self._commit_manager.mark(commit)


class SourcesCheckpointer:
    """Manage checkpoints for a stream's progress through a collection of data sources."""

    def __init__(self, *source_checkpointers: SourceCheckpointer) -> None:
        """Initialize sources checkpointer."""
        self._source_checkpointers = source_checkpointers

    @contextmanager
    def batch(self) -> Generator[Batches | None]:
        """Get a checkpoint-managed collection of batches."""
        batches: dict[str, Batch | None] = {}
        with ExitStack() as stack:
            for source_checkpointer in self._source_checkpointers:
                batches[source_checkpointer.source_name] = stack.enter_context(
                    source_checkpointer.batch(),
                )
            yield None if all(v is None for v in batches.values()) else Batches(batches)

    def __iter__(self) -> Generator[SourceCheckpointer]:
        yield from self._source_checkpointers
