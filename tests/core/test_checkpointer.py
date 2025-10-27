from __future__ import annotations

from contextlib import suppress

import pytest

from bream._exceptions import SourceProtocolError
from bream.core._checkpointer import (
    CheckpointDirectory,
    Checkpointer,
)
from bream.core._definitions import Batch, BatchRequest
from tests.core.helpers import MockSource


def _prepare_checkpoint_directory(*, path, commit_datas, commit_final) -> CheckpointDirectory:
    checkpoint_directory = CheckpointDirectory(path)
    for commit_data in commit_datas[:-1]:
        checkpoint_directory.create_uncommitted(commit_data)
        checkpoint_directory.commit()
    checkpoint_directory.create_uncommitted(commit_datas[-1])
    if commit_final:
        checkpoint_directory.commit()
    return checkpoint_directory


class MockSourceWithWrongOffset(MockSource):
    """A MockSource that returns a different read_to than what was requested."""

    def __init__(self, name: str, requested_offset: int, returned_offset: int) -> None:
        super().__init__(name, advances_per_batch=3)
        self._requested_offset = requested_offset
        self._returned_offset = returned_offset

    def read(self, batch_request: BatchRequest) -> Batch | None:
        # Only return wrong offset if the request matches our expected scenario
        if batch_request.read_to == self._requested_offset:
            return Batch(
                data=f"data_from: {batch_request}",
                read_to=self._returned_offset,
            )
        # Fall back to normal behavior
        return super().read(batch_request)  # pragma: no cover


class TestCheckpointer:
    @pytest.mark.parametrize(
        ("commit_datas", "commit_final", "expected_batch"),
        [
            (
                (5, 10),
                True,
                Batch(
                    data="data_from: BatchRequest(read_from_after=10, read_to=None)",
                    read_to=13,
                ),
            ),
            (
                (5, 10, 18),
                False,
                Batch(
                    data="data_from: BatchRequest(read_from_after=10, read_to=18)",
                    read_to=18,
                ),
            ),
        ],
    )
    def test_batch_is_correct_if_source_not_empty(
        self,
        tmp_path,
        commit_datas,
        commit_final,
        expected_batch,
    ):
        source = MockSource("a_source", advances_per_batch=3)
        checkpoint_directory = _prepare_checkpoint_directory(
            path=tmp_path,
            commit_datas=commit_datas,
            commit_final=commit_final,
        )
        checkpointer = Checkpointer(source, checkpoint_directory)
        with checkpointer.batch() as batch:
            ...
        assert batch == expected_batch

    def test_batch_is_null_if_source_empty(self, tmp_path):
        source = MockSource("a_source", advances_per_batch=3, empty=True)
        checkpoint_directory = _prepare_checkpoint_directory(
            path=tmp_path,
            commit_datas=(5, 10),
            commit_final=False,
        )
        checkpointer = Checkpointer(source, checkpoint_directory)
        with checkpointer.batch() as batch:
            ...
        assert batch is None

    def test_successful_batch_creates_new_committed_checkpoint_if_no_uncommitted_exists(
        self,
        tmp_path,
    ):
        source = MockSource("a_source", advances_per_batch=3)
        checkpoint_directory = _prepare_checkpoint_directory(
            path=tmp_path,
            commit_datas=(5, 10),
            commit_final=True,
        )
        checkpointer = Checkpointer(source, checkpoint_directory)

        with checkpointer.batch():
            ...

        expected_max_committed_data = 13
        assert checkpoint_directory.max_committed.checkpoint_data == expected_max_committed_data
        assert checkpoint_directory.uncommitted is None

    def test_unsuccessful_batch_creates_new_uncommitted_checkpoint_if_no_uncommitted_exists(
        self,
        tmp_path,
    ):
        source = MockSource("a_source", advances_per_batch=3)
        checkpoint_directory = _prepare_checkpoint_directory(
            path=tmp_path,
            commit_datas=(5, 10),
            commit_final=True,
        )
        checkpointer = Checkpointer(source, checkpoint_directory)

        with suppress(ValueError), checkpointer.batch():
            raise ValueError

        expected_max_committed_data, expected_uncommitted_data = 10, 13
        assert checkpoint_directory.max_committed.checkpoint_data == expected_max_committed_data
        assert checkpoint_directory.uncommitted.checkpoint_data == expected_uncommitted_data

    def test_successful_batch_commits_if_uncommitted_exists(self, tmp_path):
        source = MockSource("a_source", advances_per_batch=3)
        checkpoint_directory = _prepare_checkpoint_directory(
            path=tmp_path,
            commit_datas=(5, 10, 18),
            commit_final=False,
        )
        checkpointer = Checkpointer(source, checkpoint_directory)

        with checkpointer.batch():
            ...

        expected_max_committed_data = 18
        assert checkpoint_directory.max_committed.checkpoint_data == expected_max_committed_data
        assert checkpoint_directory.uncommitted is None

    def test_unsuccessful_doesnt_commit_if_uncommitted_exists(self, tmp_path):
        source = MockSource("a_source", advances_per_batch=3)
        checkpoint_directory = _prepare_checkpoint_directory(
            path=tmp_path,
            commit_datas=(5, 10, 18),
            commit_final=False,
        )
        checkpointer = Checkpointer(source, checkpoint_directory)

        with suppress(ValueError), checkpointer.batch():
            raise ValueError

        expected_max_committed_data, expected_uncommitted_data = 10, 18

        assert checkpoint_directory.max_committed.checkpoint_data == expected_max_committed_data
        assert checkpoint_directory.uncommitted.checkpoint_data == expected_uncommitted_data

    def test_forget_uncommitted_checkpoint_works_if_uncommitted_exists(self, tmp_path):
        source = MockSource("a_source", advances_per_batch=3)
        checkpoint_directory = _prepare_checkpoint_directory(
            path=tmp_path,
            commit_datas=(5, 10, 18),
            commit_final=False,
        )
        checkpointer = Checkpointer(source, checkpoint_directory)

        checkpointer.forget_uncommitted_checkpoint()

        expected_max_committed_data = 10

        assert checkpoint_directory.max_committed.checkpoint_data == expected_max_committed_data
        assert checkpoint_directory.uncommitted is None

    def test_forget_uncommitted_checkpoint_does_nothing_if_uncommitted_doesnt_exist(self, tmp_path):
        source = MockSource("a_source", advances_per_batch=3)
        checkpoint_directory = _prepare_checkpoint_directory(
            path=tmp_path,
            commit_datas=(5, 10),
            commit_final=True,
        )
        checkpointer = Checkpointer(source, checkpoint_directory)

        checkpointer.forget_uncommitted_checkpoint()

        expected_max_committed_data = 10

        assert checkpoint_directory.max_committed.checkpoint_data == expected_max_committed_data
        assert checkpoint_directory.uncommitted is None

    def test_raises_source_protocol_error_when_source_reads_to_wrong_offset(self, tmp_path):
        """Test that SourceProtocolError is raised when source doesn't read to requested offset."""
        requested_offset = 18
        returned_offset = 20

        source = MockSourceWithWrongOffset("test_source", requested_offset, returned_offset)
        checkpoint_directory = _prepare_checkpoint_directory(
            path=tmp_path,
            commit_datas=(10, requested_offset),
            commit_final=False,
        )
        checkpointer = Checkpointer(source, checkpoint_directory)

        expected_error_msg = (
            f"Source test_source was told to read to offset {requested_offset} "
            f"but claims to have read to {returned_offset}."
        )

        with pytest.raises(SourceProtocolError, match=expected_error_msg), checkpointer.batch():
            ...
