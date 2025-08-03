"""Manage checkpoints for stream progress."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any

from bream._exceptions import (
    CheckpointDirectoryInvalidOperationError,
    CheckpointDirectoryValidityError,
)
from bream.core._utils import dump_json_atomically

if TYPE_CHECKING:
    from collections.abc import Sequence

    from bream.core._definitions import JsonableNonNull, Pathlike


DEFAULT_RETAIN_NUM_COMMITTED_CHECKPOINTS = 100


class CheckpointStatus(str, Enum):
    COMMITTED = "committed"
    UNCOMMITTED = "uncommitted"

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class CheckpointMetadata:
    created_at: float
    committed_at: float | None


@dataclass(frozen=True)
class Checkpoint:
    """Data model of a checkpoint."""

    _TMP_FILE_SUFFIX = "_tmp"

    number: int
    checkpoint_data: JsonableNonNull
    checkpoint_metadata: CheckpointMetadata

    def to_json(self, parent_path: Pathlike, *, status: CheckpointStatus) -> None:
        """Write checkpoint to JSON file.

        Args:
            parent_path: Parent directory path where the checkpoint file will be created
            status: used to determine file extension

        """
        parent_path.mkdir(parents=True, exist_ok=True)

        filename = f"{self.number}.{status}"
        dst_path = parent_path / filename
        tmp_extension = f"{status}{self._TMP_FILE_SUFFIX}"
        tmp_path = parent_path / f"{dst_path.stem}.{tmp_extension}"

        data: JsonableNonNull = {
            "checkpoint_data": self.checkpoint_data,
            "checkpoint_metadata": asdict(self.checkpoint_metadata),
        }

        dump_json_atomically(data, dst_path, tmp_path)

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
            checkpoint_metadata=CheckpointMetadata(
                created_at=data["checkpoint_metadata"]["created_at"],
                committed_at=data["checkpoint_metadata"]["committed_at"],
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
        self._validate_repair_and_clean()

    def __getattribute__(self, name: str) -> Any:  # noqa: ANN401
        if name == "__init__" or not name.startswith("_"):
            self._validate_repair_and_clean()
        return object.__getattribute__(self, name)

    @property
    def path(self) -> Pathlike:
        return self._path

    @property
    def _uncommitted_paths(self) -> list[Pathlike]:
        return list(self._path.glob(f"*.{CheckpointStatus.UNCOMMITTED}"))

    @property
    def _committed_paths(self) -> list[Pathlike]:
        return list(self._path.glob(f"*.{CheckpointStatus.COMMITTED}"))

    @property
    def max_committed(self) -> Checkpoint | None:
        """The latest committed checkpoint if there are any."""
        committed_ints = [int(p.stem) for p in self._committed_paths]
        if not committed_ints:
            return None
        max_committed_int = max(committed_ints)
        checkpoint_path = self._path / f"{max_committed_int}.{CheckpointStatus.COMMITTED}"
        return Checkpoint.from_json(checkpoint_path)

    @property
    def uncommitted(self) -> Checkpoint | None:
        """The uncommitted checkpoint if there is one."""
        uncommitted_ints = [int(p.stem) for p in self._uncommitted_paths]
        if not uncommitted_ints:
            return None
        uncommitted_int = uncommitted_ints[0]
        checkpoint_path = self._path / f"{uncommitted_int}.{CheckpointStatus.UNCOMMITTED}"
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
        uncommitted_int_to_create = max(committed_ints) + 1 if committed_ints else 0

        checkpoint = Checkpoint(
            number=uncommitted_int_to_create,
            checkpoint_data=checkpoint_data,
            checkpoint_metadata=CheckpointMetadata(
                created_at=datetime.now(tz=timezone.utc).timestamp(),
                committed_at=None,
            ),
        )
        checkpoint.to_json(self._path, status=CheckpointStatus.UNCOMMITTED)

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

        committed_checkpoint = replace(
            uncommitted_checkpoint,
            checkpoint_metadata=replace(
                uncommitted_checkpoint.checkpoint_metadata,
                committed_at=datetime.now(tz=timezone.utc).timestamp(),
            ),
        )

        committed_checkpoint.to_json(self._path, status=CheckpointStatus.COMMITTED)

        uncommitted_path.unlink()

    def _validate_repair_and_clean(self) -> None:
        """Single-scan validation, repair, and cleanup to maintain directory integrity."""
        committed_paths = list(self._path.glob(f"*.{CheckpointStatus.COMMITTED}"))
        uncommitted_paths = list(self._path.glob(f"*.{CheckpointStatus.UNCOMMITTED}"))

        self._raise_if_unrecoverable_invalid_state_from_paths(committed_paths, uncommitted_paths)
        self._repair_uncommitted_duplicates_from_paths(committed_paths, uncommitted_paths)
        self._clean_old_committed_checkpoints_from_paths(committed_paths, uncommitted_paths)

    def _raise_if_unrecoverable_invalid_state_from_paths(
        self,
        committed_paths: list[Pathlike],
        uncommitted_paths: list[Pathlike],
    ) -> None:
        committed_ints = sorted(int(p.stem) for p in committed_paths)
        uncommitted_ints = sorted(int(p.stem) for p in uncommitted_paths)

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

    def _repair_uncommitted_duplicates_from_paths(
        self,
        committed_paths: list[Pathlike],
        uncommitted_paths: list[Pathlike],
    ) -> None:
        committed_ints = {int(p.stem) for p in committed_paths}
        uncommitted_ints = {int(p.stem) for p in uncommitted_paths}
        committed_ints_with_uncommitted = set(committed_ints).intersection(uncommitted_ints)
        for i in committed_ints_with_uncommitted:
            p = self._path / f"{i}.{CheckpointStatus.UNCOMMITTED}"
            p.unlink()

    def _clean_old_committed_checkpoints_from_paths(
        self,
        committed_paths: list[Pathlike],
        uncommitted_paths: list[Pathlike],
    ) -> None:
        del uncommitted_paths
        sorted_committed_paths = sorted(committed_paths, key=lambda p: int(p.stem))
        num_delete = max(0, len(sorted_committed_paths) - self._retain_old_committed_checkpoints)
        for p in sorted_committed_paths[:num_delete]:
            # must go in sorted order to maintain the consecutivity in case of failure
            p.unlink()


def _are_consecutive(elements: Sequence) -> bool:
    return not elements or all(n == i for i, n in enumerate(elements, elements[0]))
