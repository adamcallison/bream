from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING

import pytest

from bream.core._checkpoint import (
    COMMIT_MARK_NAME,
    OFFSET_MARK_NAME,
    SourceCheckpointer,
    SourcesCheckpointer,
)
from bream.core._definitions import Batch, BatchRequest, Source

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path


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
        return Batch(
            data=f"data_from_batch_request: {batch_request}",
            read_to=batch_request.read_to
            or (
                (
                    (batch_request.read_from_after + 1)
                    if batch_request.read_from_after is not None
                    else 0
                )
                + self._advances_per_batch
                - 1
            ),
        )


class MockSourceCheckpointer(SourceCheckpointer):
    def __init__(self, source_name: str) -> None:
        self._source_name = source_name
        self.context_entered = False
        self.context_exited_cleanly = False

    @property
    def source_name(self) -> str:
        return self._source_name

    @contextmanager
    def batch(self) -> Generator[None]:
        self.context_entered = True
        yield
        self.context_exited_cleanly = True


class TestSourceCheckpointer:
    @staticmethod
    def setup_checkpoint_state(
        path: Path,
        offsets=list[int],
        commits=list[int],
    ) -> None:
        offset_path = path / OFFSET_MARK_NAME
        commit_path = path / COMMIT_MARK_NAME
        offset_path.mkdir(parents=True, exist_ok=True)
        commit_path.mkdir(parents=True, exist_ok=True)

        for offset in offsets:
            with (offset_path / str(offset)).open("w") as f:
                f.write("")
        for commit in commits:
            with (commit_path / str(commit)).open("w") as f:
                f.write("")

    def test_source_name_is_source_name(self, tmp_path):
        source = MockSource("mock_source", advances_per_batch=3)
        source_checkpointer = SourceCheckpointer(source, tmp_path)
        assert source_checkpointer.source_name == source.name

    @pytest.mark.parametrize(
        ("initial_offsets", "initial_commits", "expected_batch"),
        [
            (
                [5, 10],
                [5, 10],
                Batch(
                    data="data_from_batch_request: BatchRequest(read_from_after=10, read_to=None)",
                    read_to=13,
                ),
            ),
            (
                [5, 10, 18],
                [5, 10],
                Batch(
                    data="data_from_batch_request: BatchRequest(read_from_after=10, read_to=18)",
                    read_to=18,
                ),
            ),
        ],
    )
    def test_batch_is_correct_if_source_not_empty(
        self,
        tmp_path,
        initial_offsets,
        initial_commits,
        expected_batch,
    ):
        source = MockSource("mock_source", advances_per_batch=3)
        source_checkpointer = SourceCheckpointer(source, tmp_path)
        self.setup_checkpoint_state(tmp_path / source.name, initial_offsets, initial_commits)
        with source_checkpointer.batch() as batch:
            pass
        assert batch == expected_batch

    def test_batch_is_null_if_source_empty(self, tmp_path):
        source = MockSource("mock_source", advances_per_batch=3, empty=True)
        source_checkpointer = SourceCheckpointer(source, tmp_path)
        self.setup_checkpoint_state(tmp_path / source.name, [5, 10], [5, 10])
        with source_checkpointer.batch() as batch:
            pass
        assert batch is None

    @pytest.mark.parametrize(
        ("initial_offsets", "initial_commits", "expected_offsets", "expected_commits"),
        [
            ([], [], [2], [2]),
            ([1], [], [1], [1]),
            ([2], [2], [2, 5], [2, 5]),
            ([2, 5], [2], [2, 5], [2, 5]),
            ([100, 101], [100, 101], [100, 101, 104], [100, 101, 104]),
            ([100, 101, 104], [100, 101], [100, 101, 104], [100, 101, 104]),
        ],
    )
    def test_batch_advances_offset_and_commit_if_no_error(
        self,
        tmp_path,
        initial_offsets,
        initial_commits,
        expected_offsets,
        expected_commits,
    ):
        source = MockSource("mock_source", advances_per_batch=3)
        self.setup_checkpoint_state(tmp_path / source.name, initial_offsets, initial_commits)
        source_checkpointer = SourceCheckpointer(source, tmp_path)
        with source_checkpointer.batch():
            pass
        actual_offsets = sorted(
            int(p.name) for p in (tmp_path / source.name / OFFSET_MARK_NAME).glob("*")
        )
        actual_commits = sorted(
            int(p.name) for p in (tmp_path / source.name / COMMIT_MARK_NAME).glob("*")
        )
        assert actual_offsets == expected_offsets
        assert actual_commits == expected_commits

    @pytest.mark.parametrize(
        ("initial_offsets", "initial_commits", "expected_offsets", "expected_commits"),
        [
            ([], [], [2], []),
            ([1], [], [1], []),
            ([2], [2], [2, 5], [2]),
            ([2, 5], [2], [2, 5], [2]),
            ([100, 101], [100, 101], [100, 101, 104], [100, 101]),
            ([100, 101, 104], [100, 101], [100, 101, 104], [100, 101]),
        ],
    )
    def test_batch_advances_offset_but_not_commit_if_error(
        self,
        tmp_path,
        initial_offsets,
        initial_commits,
        expected_offsets,
        expected_commits,
    ):
        source = MockSource("mock_source", advances_per_batch=3)
        self.setup_checkpoint_state(tmp_path / source.name, initial_offsets, initial_commits)
        source_checkpointer = SourceCheckpointer(source, tmp_path)
        try:
            with source_checkpointer.batch():
                raise ValueError
        except ValueError:
            pass
        actual_offsets = sorted(
            int(p.name) for p in (tmp_path / source.name / OFFSET_MARK_NAME).glob("*")
        )
        actual_commits = sorted(
            int(p.name) for p in (tmp_path / source.name / COMMIT_MARK_NAME).glob("*")
        )
        assert actual_offsets == expected_offsets
        assert actual_commits == expected_commits

    @pytest.mark.parametrize(
        ("retain_marks",),
        [
            (100,),
            (20,),
        ],
    )
    def test_batch_garbage_collects_checkpoint_marks(self, tmp_path, retain_marks):
        source = MockSource("mock_source", advances_per_batch=1)
        self.setup_checkpoint_state(
            tmp_path / source.name,
            list(range(20, 201)),
            list(range(20, 201)),
        )
        source_checkpointer = SourceCheckpointer(source, tmp_path, retain_marks=retain_marks)
        with source_checkpointer.batch():
            pass
        actual_offsets = sorted(
            int(p.name) for p in (tmp_path / source.name / OFFSET_MARK_NAME).glob("*")
        )
        actual_commits = sorted(
            int(p.name) for p in (tmp_path / source.name / COMMIT_MARK_NAME).glob("*")
        )
        expected_offsets = list(range(102, 202))[-retain_marks:]
        expected_commits = list(range(102, 202))[-retain_marks:]
        assert actual_offsets == expected_offsets
        assert actual_commits == expected_commits


class TestSourcesCheckpointer:
    def test_batch_enters_source_checkpointer_batch_contexts_and_exits_cleanly_if_no_error(self):
        source_checkpointers = [
            MockSourceCheckpointer("source1"),
            MockSourceCheckpointer("source2"),
        ]
        sources_checkpointer = SourcesCheckpointer(*source_checkpointers)
        with sources_checkpointer.batch():
            pass
        assert all(sc.context_entered for sc in source_checkpointers)
        assert all(sc.context_exited_cleanly for sc in source_checkpointers)

    def test_batch_enters_source_checkpointer_batch_contexts_but_doesnt_exit_cleanly_if_error(self):
        source_checkpointers = [
            MockSourceCheckpointer("source1"),
            MockSourceCheckpointer("source2"),
        ]
        sources_checkpointer = SourcesCheckpointer(*source_checkpointers)
        try:
            with sources_checkpointer.batch():
                raise ValueError
        except ValueError:
            pass
        assert all(sc.context_entered for sc in source_checkpointers)
        assert not any(sc.context_exited_cleanly for sc in source_checkpointers)
