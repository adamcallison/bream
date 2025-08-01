from __future__ import annotations

from contextlib import suppress

import pytest

from bream.core._checkpointer import (
    CheckpointDirectory,
    Checkpointer,
)
from bream.core._definitions import Batch
from tests.core.helpers import (
    MockSource,
    freeze_default_time,
    get_checkpoint_directory_state,
    make_checkpoint,
    setup_checkpoint_directory,
)


class TestCheckpointer:
    @pytest.mark.parametrize(
        ("initial_committed", "initial_uncommitted", "expected_batch"),
        [
            (
                [make_checkpoint(0, 5), make_checkpoint(1, 10)],
                None,
                Batch(
                    data="data_from: BatchRequest(read_from_after=10, read_to=None)",
                    read_to=13,
                ),
            ),
            (
                [make_checkpoint(0, 5), make_checkpoint(1, 10)],
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
            committed_checkpoints=[make_checkpoint(0, 5), make_checkpoint(1, 10)],
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
            committed_checkpoints=[make_checkpoint(0, 5), make_checkpoint(1, 10)],
        )
        checkpointer = Checkpointer(source, CheckpointDirectory(tmp_path))
        with freeze_default_time(), checkpointer.batch():
            pass

        assert get_checkpoint_directory_state(tmp_path) == (
            [make_checkpoint(0, 5), make_checkpoint(1, 10), make_checkpoint(2, 13)],
            [],
        )

    def test_unsuccessful_batch_creates_new_uncommitted_checkpoint_if_no_uncommitted_exists(
        self,
        tmp_path,
    ):
        source = MockSource("a_source", advances_per_batch=3)
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=[make_checkpoint(0, 5), make_checkpoint(1, 10)],
        )
        checkpointer = Checkpointer(source, CheckpointDirectory(tmp_path))
        with freeze_default_time(), suppress(ValueError), checkpointer.batch():
            raise ValueError

        assert get_checkpoint_directory_state(tmp_path) == (
            [make_checkpoint(0, 5), make_checkpoint(1, 10)],
            [make_checkpoint(2, 13)],
        )

    def test_successful_batch_commits_if_uncommitted_exists(self, tmp_path):
        source = MockSource("a_source", advances_per_batch=3)
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=[make_checkpoint(0, 5), make_checkpoint(1, 10)],
            uncommitted_checkpoints=[make_checkpoint(2, 18)],
        )
        checkpointer = Checkpointer(source, CheckpointDirectory(tmp_path))
        with checkpointer.batch():
            pass

        assert get_checkpoint_directory_state(tmp_path) == (
            [make_checkpoint(0, 5), make_checkpoint(1, 10), make_checkpoint(2, 18)],
            [],
        )

    def test_unsuccessful_doesnt_commit_if_uncommitted_exists(self, tmp_path):
        source = MockSource("a_source", advances_per_batch=3)
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=[make_checkpoint(0, 5), make_checkpoint(1, 10)],
            uncommitted_checkpoints=[make_checkpoint(2, 18)],
        )
        checkpointer = Checkpointer(source, CheckpointDirectory(tmp_path))
        with suppress(ValueError), checkpointer.batch():
            raise ValueError

        assert get_checkpoint_directory_state(tmp_path) == (
            [make_checkpoint(0, 5), make_checkpoint(1, 10)],
            [make_checkpoint(2, 18)],
        )

    def test_forget_uncommitted_checkpoint_works_if_uncommitted_exists(self, tmp_path):
        source = MockSource("a_source", advances_per_batch=3)
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=[make_checkpoint(0, 5), make_checkpoint(1, 10)],
            uncommitted_checkpoints=[make_checkpoint(2, 18)],
        )
        checkpointer = Checkpointer(source, CheckpointDirectory(tmp_path))

        checkpointer.forget_uncommitted_checkpoint()

        assert get_checkpoint_directory_state(tmp_path) == (
            [make_checkpoint(0, 5), make_checkpoint(1, 10)],
            [],
        )

    def test_forget_uncommitted_checkpoint_does_nothing_if_uncommitted_doesnt_exist(self, tmp_path):
        source = MockSource("a_source", advances_per_batch=3)
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=[make_checkpoint(0, 5), make_checkpoint(1, 10)],
        )
        checkpointer = Checkpointer(source, CheckpointDirectory(tmp_path))

        checkpointer.forget_uncommitted_checkpoint()

        assert get_checkpoint_directory_state(tmp_path) == (
            [make_checkpoint(0, 5), make_checkpoint(1, 10)],
            [],
        )
