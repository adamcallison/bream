from __future__ import annotations

import json
from typing import TYPE_CHECKING

from bream.core._checkpoint_directory import COMMITTED, UNCOMMITTED
from bream.core._definitions import Batch, BatchRequest, JsonableNonNull, Source

if TYPE_CHECKING:
    from pathlib import Path


def setup_checkpoint_directory(
    checkpoint_dir: Path,
    *,
    committed_checkpoints: list[tuple[int, JsonableNonNull]] | None = None,
    uncommitted_checkpoints: list[tuple[int, JsonableNonNull]] | None = None,
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

    for checkpoints, extension in [
        (committed_checkpoints, COMMITTED),
        (uncommitted_checkpoints, UNCOMMITTED),
    ]:
        if checkpoints:
            for number, data in checkpoints:
                checkpoint_file = checkpoint_dir / f"{number}.{extension}"
                with checkpoint_file.open("w") as f:
                    json.dump(data, f)


def get_checkpoint_directory_state(
    checkpoint_dir: Path,
) -> tuple[
    list[tuple[int, JsonableNonNull]],
    list[tuple[int, JsonableNonNull]],
]:
    """
    Helper function to get the state of a checkpoint directory.

    Args:
        checkpoint_dir: The directory where checkpoint files should be created

    Returns:
        committed_checkpoints: List of tuples (number, data) for committed checkpoints
        uncommitted_checkpoints: List of tuples (number, data) for uncommitted checkpoints
    """
    fetched_tuples: dict[str, list[tuple[int, JsonableNonNull]]] = {}
    for extension in [COMMITTED, UNCOMMITTED]:
        paths = checkpoint_dir.glob(f"*.{extension}")
        fetched_tuples_: list[tuple[int, JsonableNonNull]] = []
        for p in paths:
            with p.open("r") as f:
                fetched_tuples_.append((int(p.stem), json.load(f)))
        fetched_tuples[extension] = fetched_tuples_
    sk = lambda x: x[0]  # noqa: E731
    return sorted(fetched_tuples[COMMITTED], key=sk), sorted(fetched_tuples[UNCOMMITTED], key=sk)


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
