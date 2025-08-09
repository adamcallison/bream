from __future__ import annotations

from contextlib import suppress

import pytest

from bream._exceptions import SourceProtocolError
from bream.core._checkpointer import (
    CheckpointDirectory,
    Checkpointer,
)
from bream.core._definitions import Batch, BatchRequest
from tests.core.helpers import (
    MockSource,
    freeze_default_committed_at_time,
    freeze_default_created_at_time,
    freeze_default_times_with_ticking,
    get_checkpoint_directory_state,
    make_checkpoint,
    make_committed_checkpoint,
    setup_checkpoint_directory,
)


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
        ("initial_committed", "initial_uncommitted", "expected_batch"),
        [
            (
                [make_committed_checkpoint(0, 5), make_committed_checkpoint(1, 10)],
                None,
                Batch(
                    data="data_from: BatchRequest(read_from_after=10, read_to=None)",
                    read_to=13,
                ),
            ),
            (
                [make_committed_checkpoint(0, 5), make_committed_checkpoint(1, 10)],
                make_checkpoint(2, 18),
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
        initial_committed,
        initial_uncommitted,
        expected_batch,
    ):
        source = MockSource("a_source", advances_per_batch=3)
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=initial_committed,
            uncommitted_checkpoints=[initial_uncommitted] if initial_uncommitted else None,
        )
        checkpointer = Checkpointer(source, CheckpointDirectory(tmp_path))
        with checkpointer.batch() as batch:
            pass
        assert batch == expected_batch

    def test_batch_is_null_if_source_empty(self, tmp_path):
        source = MockSource("a_source", advances_per_batch=3, empty=True)
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=[
                make_committed_checkpoint(0, 5),
                make_committed_checkpoint(1, 10),
            ],
        )
        checkpointer = Checkpointer(source, CheckpointDirectory(tmp_path))
        with checkpointer.batch() as batch:
            pass
        assert batch is None

    def test_successful_batch_creates_new_committed_checkpoint_if_no_uncommitted_exists(
        self,
        tmp_path,
    ):
        source = MockSource("a_source", advances_per_batch=3)
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=[
                make_committed_checkpoint(0, 5),
                make_committed_checkpoint(1, 10),
            ],
        )
        checkpointer = Checkpointer(source, CheckpointDirectory(tmp_path))
        with freeze_default_times_with_ticking(), checkpointer.batch():
            pass

        assert get_checkpoint_directory_state(tmp_path) == (
            [
                make_committed_checkpoint(0, 5),
                make_committed_checkpoint(1, 10),
                make_committed_checkpoint(2, 13),
            ],
            [],
        )

    def test_unsuccessful_batch_creates_new_uncommitted_checkpoint_if_no_uncommitted_exists(
        self,
        tmp_path,
    ):
        source = MockSource("a_source", advances_per_batch=3)
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=[
                make_committed_checkpoint(0, 5),
                make_committed_checkpoint(1, 10),
            ],
        )
        checkpointer = Checkpointer(source, CheckpointDirectory(tmp_path))
        with freeze_default_created_at_time(), suppress(ValueError), checkpointer.batch():
            raise ValueError

        assert get_checkpoint_directory_state(tmp_path) == (
            [make_committed_checkpoint(0, 5), make_committed_checkpoint(1, 10)],
            [make_checkpoint(2, 13)],
        )

    def test_successful_batch_commits_if_uncommitted_exists(self, tmp_path):
        source = MockSource("a_source", advances_per_batch=3)
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=[
                make_committed_checkpoint(0, 5),
                make_committed_checkpoint(1, 10),
            ],
            uncommitted_checkpoints=[make_checkpoint(2, 18)],
        )
        checkpointer = Checkpointer(source, CheckpointDirectory(tmp_path))
        with freeze_default_committed_at_time(), checkpointer.batch():
            pass

        assert get_checkpoint_directory_state(tmp_path) == (
            [
                make_committed_checkpoint(0, 5),
                make_committed_checkpoint(1, 10),
                make_committed_checkpoint(2, 18),
            ],
            [],
        )

    def test_unsuccessful_doesnt_commit_if_uncommitted_exists(self, tmp_path):
        source = MockSource("a_source", advances_per_batch=3)
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=[
                make_committed_checkpoint(0, 5),
                make_committed_checkpoint(1, 10),
            ],
            uncommitted_checkpoints=[make_checkpoint(2, 18)],
        )
        checkpointer = Checkpointer(source, CheckpointDirectory(tmp_path))
        with suppress(ValueError), checkpointer.batch():
            raise ValueError

        assert get_checkpoint_directory_state(tmp_path) == (
            [make_committed_checkpoint(0, 5), make_committed_checkpoint(1, 10)],
            [make_checkpoint(2, 18)],
        )

    def test_forget_uncommitted_checkpoint_works_if_uncommitted_exists(self, tmp_path):
        source = MockSource("a_source", advances_per_batch=3)
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=[
                make_committed_checkpoint(0, 5),
                make_committed_checkpoint(1, 10),
            ],
            uncommitted_checkpoints=[make_checkpoint(2, 18)],
        )
        checkpointer = Checkpointer(source, CheckpointDirectory(tmp_path))

        checkpointer.forget_uncommitted_checkpoint()

        assert get_checkpoint_directory_state(tmp_path) == (
            [make_committed_checkpoint(0, 5), make_committed_checkpoint(1, 10)],
            [],
        )

    def test_forget_uncommitted_checkpoint_does_nothing_if_uncommitted_doesnt_exist(self, tmp_path):
        source = MockSource("a_source", advances_per_batch=3)
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=[
                make_committed_checkpoint(0, 5),
                make_committed_checkpoint(1, 10),
            ],
        )
        checkpointer = Checkpointer(source, CheckpointDirectory(tmp_path))

        checkpointer.forget_uncommitted_checkpoint()

        assert get_checkpoint_directory_state(tmp_path) == (
            [make_committed_checkpoint(0, 5), make_committed_checkpoint(1, 10)],
            [],
        )

    def test_raises_source_protocol_error_when_source_reads_to_wrong_offset(self, tmp_path):
        """Test that SourceProtocolError is raised when source doesn't read to requested offset."""
        # Set up an uncommitted checkpoint that will be used as read_to in the batch request
        requested_offset = 18
        returned_offset = 20  # Different from requested

        source = MockSourceWithWrongOffset("test_source", requested_offset, returned_offset)
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=[make_committed_checkpoint(0, 10)],
            uncommitted_checkpoints=[make_checkpoint(1, requested_offset)],
        )
        checkpointer = Checkpointer(source, CheckpointDirectory(tmp_path))

        expected_error_msg = (
            f"Source test_source was told to read to offset {requested_offset} "
            f"but claims to have read to {returned_offset}."
        )

        with pytest.raises(SourceProtocolError, match=expected_error_msg), checkpointer.batch():
            ...
