"""Manage checkpoints for stream progress."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal, cast

from bream._exceptions import (
    CheckpointDirectoryInvalidOperationError,
    CheckpointDirectoryValidityError,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from bream.core._definitions import JsonableNonNull, Pathlike

STATUS = Literal["committed", "uncommitted"]
COMMITTED: STATUS = "committed"
COMMITTED_TMP = f"{COMMITTED}_tmp"
UNCOMMITTED: STATUS = "uncommitted"
UNCOMMITTED_TMP = f"{UNCOMMITTED}_tmp"
DEFAULT_RETAIN_NUM_COMMITTED_CHECKPOINTS = 100


@dataclass
class Checkpoint:
    """Data model of a checkpoint."""

    number: int
    checkpoint_data: JsonableNonNull
    checkpoint_metadata: dict[str, JsonableNonNull | None]

    def to_json(self, parent_path: Pathlike, *, status: STATUS) -> None:
        """Write checkpoint to JSON file.

        Args:
            parent_path: Parent directory path where the checkpoint file will be created
            status: used to determine file extension

        """
        parent_path.mkdir(parents=True, exist_ok=True)

        filename = f"{self.number}.{status}"
        dst_path = parent_path / filename
        tmp_extension = {
            UNCOMMITTED: UNCOMMITTED_TMP,
            COMMITTED: COMMITTED_TMP,
        }[status]
        tmp_path = parent_path / f"{dst_path.stem}.{tmp_extension}"

        data = {
            "checkpoint_data": self.checkpoint_data,
            "checkpoint_metadata": self.checkpoint_metadata,
        }

        _dump_json_atomically(data, dst_path, tmp_path)

    @classmethod
    def from_json(cls, file_path: Pathlike) -> Checkpoint:
        """Create Checkpoint from JSON file.

        Args:
            file_path: Path to the checkpoint JSON file

        Returns:
            Checkpoint instance loaded from the file

        """
        number = int(file_path.stem)

        with file_path.open("r") as f:
            data = json.load(f)

        return cls(
            number=number,
            checkpoint_data=data["checkpoint_data"],
            checkpoint_metadata=cast(
                "dict[str, JsonableNonNull | None]",
                data["checkpoint_metadata"],
            ),
        )


class CheckpointDirectory:
    """Manages a directory containing checkpoint files for a stream."""

    def __init__(
        self,
        path: Pathlike,
        retain_old_committed_checkpoints: int = DEFAULT_RETAIN_NUM_COMMITTED_CHECKPOINTS,
    ) -> None:
        self._path = path
        self._retain_old_committed_checkpoints = retain_old_committed_checkpoints
        self._raise_if_unrecoverable_invalid_state()
        self._repair_if_recoverable_invalid_state()
        self._clean_old_committed_checkpoints()

    def __getattribute__(self, name: str) -> Any:  # noqa: ANN401
        if name == "__init__" or not name.startswith("_"):
            self._raise_if_unrecoverable_invalid_state()
            self._repair_if_recoverable_invalid_state()
            self._clean_old_committed_checkpoints()
        return object.__getattribute__(self, name)

    @property
    def path(self) -> Pathlike:
        return self._path

    @property
    def _uncommitted_paths(self) -> list[Pathlike]:
        return list(self._path.glob(f"*.{UNCOMMITTED}"))

    @property
    def _committed_paths(self) -> list[Pathlike]:
        return list(self._path.glob(f"*.{COMMITTED}"))

    @property
    def max_committed(self) -> Checkpoint | None:
        """The latest committed checkpoint if there are any."""
        committed_ints = [int(p.stem) for p in self._committed_paths]
        if not committed_ints:
            return None
        max_committed_int = max(committed_ints)
        checkpoint_path = self._path / f"{max_committed_int}.{COMMITTED}"
        return Checkpoint.from_json(checkpoint_path)

    @property
    def uncommitted(self) -> Checkpoint | None:
        """The uncommitted checkpoint if there is one."""
        uncommitted_ints = [int(p.stem) for p in self._uncommitted_paths]
        if not uncommitted_ints:
            return None
        uncommitted_int = uncommitted_ints[0]
        checkpoint_path = self._path / f"{uncommitted_int}.{UNCOMMITTED}"
        return Checkpoint.from_json(checkpoint_path)

    def create_uncommitted(self, checkpoint_data: JsonableNonNull) -> None:
        """Create an uncommitted checkpoint with the given data.

        Raises CheckpointDirectoryInvalidOperationError is there is already one.
        """
        uncommitted_paths = self._uncommitted_paths
        if uncommitted_paths:
            msg = "There is already an uncommitted checkpoint."
            raise CheckpointDirectoryInvalidOperationError(msg)
        committed_ints = [int(p.stem) for p in self._committed_paths]
        uncomitted_int_to_create = max(committed_ints) + 1 if committed_ints else 0

        checkpoint = Checkpoint(
            number=uncomitted_int_to_create,
            checkpoint_data=checkpoint_data,
            checkpoint_metadata={
                "created_at": datetime.now(tz=timezone.utc).timestamp(),
                "committed_at": None,
            },
        )
        checkpoint.to_json(self._path, status=UNCOMMITTED)

    def remove_uncommitted(self) -> None:
        """Remove the uncommitted checkpoint.

        Raises CheckpointDirectoryInvalidOperationError is there isn't one.
        """
        uncommitted_paths = self._uncommitted_paths
        if not uncommitted_paths:
            msg = "There is no uncommitted checkpoint to remove."
            raise CheckpointDirectoryInvalidOperationError(msg)
        uncommitted_path = uncommitted_paths[0]
        uncommitted_path.unlink()

    def commit(self) -> None:
        """Commit the uncommitted checkpoint.

        Raises CheckpointDirectoryInvalidOperationError is there isn't one.
        """
        uncommitted_paths = self._uncommitted_paths
        if not uncommitted_paths:
            msg = "There is no uncommitted checkpoint to commit."
            raise CheckpointDirectoryInvalidOperationError(msg)
        uncommitted_path = uncommitted_paths[0]

        uncommitted_checkpoint = Checkpoint.from_json(uncommitted_path)

        uncommitted_checkpoint.to_json(self._path, status=COMMITTED)

        uncommitted_path.unlink()

    def _raise_if_unrecoverable_invalid_state(self) -> None:
        committed_ints = sorted(int(p.stem) for p in self._committed_paths)
        uncommitted_ints = sorted(int(p.stem) for p in self._uncommitted_paths)

        uncommitted_ints_without_committed = set(uncommitted_ints) - set(committed_ints)
        if len(uncommitted_ints_without_committed) > 1:
            msg = "There should be at most one uncommitted checkpoint."
            raise CheckpointDirectoryValidityError(msg)

        uncommitted_int = (
            None
            if not uncommitted_ints_without_committed
            else next(
                iter(uncommitted_ints_without_committed),
            )
        )
        if not _are_consecutive(committed_ints):
            msg = "Commited checkpoints should be consecutive."
            raise CheckpointDirectoryValidityError(msg)

        max_committed_int = None if not committed_ints else max(committed_ints)
        possible_uncommitted_int = 0 if max_committed_int is None else max_committed_int + 1

        if uncommitted_int not in (None, possible_uncommitted_int):
            msg = (
                "Uncommitted checkpoint must be one greater than maximum committed checkpoint, "
                "or zero if there isn't one."
            )
            raise CheckpointDirectoryValidityError(msg)

    def _repair_if_recoverable_invalid_state(self) -> None:
        committed_ints = {int(p.stem) for p in self._committed_paths}
        uncommitted_ints = {int(p.stem) for p in self._uncommitted_paths}
        committed_ints_with_uncommitted = set(committed_ints).intersection(uncommitted_ints)
        for i in committed_ints_with_uncommitted:
            p = self._path / f"{i}.{UNCOMMITTED}"
            p.unlink()

    def _clean_old_committed_checkpoints(self) -> None:
        sorted_committed_paths = sorted(self._committed_paths, key=lambda p: int(p.stem))
        num_delete = max(0, len(sorted_committed_paths) - self._retain_old_committed_checkpoints)
        for p in sorted_committed_paths[:num_delete]:
            # must go in sorted order to maintain the consecutivity in case of failure
            p.unlink()


def _are_consecutive(elements: Sequence) -> bool:
    return not elements or all(n == i for i, n in enumerate(elements, elements[0]))


def _dump_json_atomically(data: JsonableNonNull, dst: Pathlike, tmp: Pathlike) -> None:
    with tmp.open("w") as f:
        json.dump(data, f)
    tmp.rename(dst)
