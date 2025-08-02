"""Manage checkpoints for stream progress."""

from __future__ import annotations

from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING

from bream._exceptions import CheckpointDirectoryInvalidOperationError, SourceProtocolError
from bream.core._checkpoint_directory import CheckpointDirectory
from bream.core._definitions import Batch, BatchRequest, Pathlike, Source

if TYPE_CHECKING:
    from collections.abc import Generator


class Checkpointer:
    """Manage checkpoints for a stream's progress through a data source."""

    def __init__(
        self,
        source: Source,
        checkpoint_directory: Pathlike | CheckpointDirectory,
    ) -> None:
        """Initialize checkpointer."""
        self._source = source
        if isinstance(checkpoint_directory, CheckpointDirectory):
            self._checkpoint_directory = checkpoint_directory
        else:
            self._checkpoint_directory = CheckpointDirectory(checkpoint_directory)

    def _read_source(
        self,
        batch_request: BatchRequest,
    ) -> Batch | None:
        """Get a checkpoint-managed batch of data for a single source."""
        maybe_batch = self._source.read(batch_request)

        if maybe_batch is None:
            return None

        batch = maybe_batch

        if batch_request.read_to is not None and batch.read_to != batch_request.read_to:
            msg = (
                f"Source {self._source.name} was told to read to offset {batch_request.read_to} "
                f"but claims to have read to {batch.read_to}."
            )
            raise SourceProtocolError(msg)
        # TODO: i had to remove the "don't read_to lower than read_from_after check after"
        # introducing SourceCollection. think about consequences

        return batch

    @contextmanager
    def batch(self) -> Generator[Batch | None]:
        """Get a checkpoint-managed batch of data."""
        latest_committed_checkpoint = self._checkpoint_directory.max_committed
        uncommitted_checkpoint = self._checkpoint_directory.uncommitted

        read_from_after = (
            latest_committed_checkpoint.checkpoint_data if latest_committed_checkpoint else None
        )
        read_to = uncommitted_checkpoint.checkpoint_data if uncommitted_checkpoint else None
        batch_request = BatchRequest(read_from_after=read_from_after, read_to=read_to)
        maybe_batch = self._read_source(batch_request)

        if maybe_batch is None:
            yield None
            return

        batch = maybe_batch

        if not uncommitted_checkpoint:
            checkpoint_data = batch.read_to
            self._checkpoint_directory.create_uncommitted(checkpoint_data)

        yield batch
        self._checkpoint_directory.commit()

    def forget_uncommitted_checkpoint(self) -> None:
        """Forget the uncommitted checkpoint if it exists."""
        with suppress(CheckpointDirectoryInvalidOperationError):
            self._checkpoint_directory.remove_uncommitted()
