from __future__ import annotations

import contextlib
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from freezegun import freeze_time

from bream.core._checkpoint_directory import Checkpoint, CheckpointMetadata, CheckpointStatus
from bream.core._definitions import Batch, BatchRequest, JsonableNonNull, Source

if TYPE_CHECKING:
    from pathlib import Path

_DEFAULT_CREATED_AT_TIMESTAMP = 1753600000.0
_DEFAULT_COMMITTED_AT_TIMESTAMP = 1753600010.0

_DEFAULT_METADATA = CheckpointMetadata(
    created_at=_DEFAULT_CREATED_AT_TIMESTAMP,
    committed_at=None,
)


_DEFAULT_COMMITTED_METADATA = CheckpointMetadata(
    created_at=_DEFAULT_CREATED_AT_TIMESTAMP,
    committed_at=_DEFAULT_COMMITTED_AT_TIMESTAMP,
)


@contextlib.contextmanager
def freeze_default_created_at_time():
    """Context manager that freezes time to the default created_at timestamp."""
    with freeze_time(
        datetime.fromtimestamp(_DEFAULT_CREATED_AT_TIMESTAMP, tz=timezone.utc),
    ):
        yield


@contextlib.contextmanager
def freeze_default_committed_at_time():
    """Context manager that freezes time to the default created_at timestamp."""
    with freeze_time(
        datetime.fromtimestamp(_DEFAULT_COMMITTED_AT_TIMESTAMP, tz=timezone.utc),
    ):
        yield


@contextlib.contextmanager
def freeze_default_times_with_ticking():
    start = datetime.fromtimestamp(_DEFAULT_CREATED_AT_TIMESTAMP, tz=timezone.utc)
    delta = _DEFAULT_COMMITTED_AT_TIMESTAMP - _DEFAULT_CREATED_AT_TIMESTAMP
    with freeze_time(start, auto_tick_seconds=delta):
        yield


def make_checkpoint(number: int, checkpoint_data: JsonableNonNull) -> Checkpoint:
    return Checkpoint(
        number=number,
        checkpoint_data=checkpoint_data,
        checkpoint_metadata=_DEFAULT_METADATA,
    )


def make_committed_checkpoint(number: int, checkpoint_data: JsonableNonNull) -> Checkpoint:
    return Checkpoint(
        number=number,
        checkpoint_data=checkpoint_data,
        checkpoint_metadata=_DEFAULT_COMMITTED_METADATA,
    )


def setup_checkpoint_directory(
    checkpoint_dir: Path,
    *,
    committed_checkpoints: list[Checkpoint] | None = None,
    uncommitted_checkpoints: list[Checkpoint] | None = None,
) -> None:
    """
    Helper function to set up a checkpoint directory in an arbitrary state.

    Args:
        checkpoint_dir: The directory where checkpoint files should be created
        committed_checkpoints: List of tuples (number, data) for committed checkpoints
        uncommitted_checkpoints: List of tuples (number, data) for uncommitted checkpoints
                                 (allows multiple for testing invalid states)
    """
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    for checkpoints, status in [
        (committed_checkpoints, CheckpointStatus.COMMITTED),
        (uncommitted_checkpoints, CheckpointStatus.UNCOMMITTED),
    ]:
        if checkpoints:
            for checkpoint in checkpoints:
                checkpoint.to_json(checkpoint_dir, status=status)


def get_checkpoint_directory_state(
    checkpoint_dir: Path,
) -> tuple[list[Checkpoint], list[Checkpoint]]:
    """
    Helper function to get the state of a checkpoint directory.

    Args:
        checkpoint_dir: The directory where checkpoint files should be created

    Returns:
        committed_checkpoints: List of tuples (number, data) for committed checkpoints
        uncommitted_checkpoints: List of tuples (number, data) for uncommitted checkpoints
    """
    fetched_checkpoints: dict[str, list[Checkpoint]] = {}
    for extension in [CheckpointStatus.COMMITTED, CheckpointStatus.UNCOMMITTED]:
        paths = checkpoint_dir.glob(f"*.{extension}")
        fetched_checkpoints_: list[Checkpoint] = []
        for p in paths:
            checkpoint = Checkpoint.from_json(p)
            fetched_checkpoints_.append(checkpoint)
        fetched_checkpoints[extension] = fetched_checkpoints_
    sk = lambda x: x.number  # noqa: E731
    return (
        sorted(fetched_checkpoints[CheckpointStatus.COMMITTED], key=sk),
        sorted(fetched_checkpoints[CheckpointStatus.UNCOMMITTED], key=sk),
    )


class MockSource(Source):
    def __init__(
        self,
        name: str,
        advances_per_batch: int,
        empty: bool = False,
    ) -> None:
        self.name = name
        self._advances_per_batch = advances_per_batch
        self._empty = empty

    def read(self, batch_request: BatchRequest) -> Batch | None:
        if self._empty:
            return None
        br_read_from_after: int | None = batch_request.read_from_after  # type: ignore[assignment]
        br_read_to: int | None = batch_request.read_to  # type: ignore[assignment]
        return Batch(
            data=f"data_from: {batch_request}",
            read_to=br_read_to
            or (
                ((br_read_from_after + 1) if br_read_from_after is not None else 0)
                + self._advances_per_batch
                - 1
            ),
        )
