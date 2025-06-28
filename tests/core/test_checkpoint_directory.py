from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import call, patch

import pytest

from bream._exceptions import (
    CheckpointDirectoryInvalidOperationError,
    CheckpointDirectoryValidityError,
)
from bream.core._checkpoint_directory import (
    Checkpoint,
    CheckpointDirectory,
)
from tests.core.helpers import get_checkpoint_directory_state, setup_checkpoint_directory

if TYPE_CHECKING:
    from pathlib import Path

    from bream.core._definitions import JsonableNonNull


class TestCheckpointDirectory:
    def test_path_is_correct(self, tmp_path):
        checkpoint_directory = CheckpointDirectory(tmp_path)
        assert checkpoint_directory.path == tmp_path

    @pytest.mark.parametrize(
        ("special_helper_method",),
        [
            ("_validate",),
            ("_clean_old_committed_checkpoints",),
        ],
    )
    def test__init__calls_special_helper_method(self, tmp_path, special_helper_method):
        with patch.object(CheckpointDirectory, special_helper_method) as mock_special_helper_method:
            CheckpointDirectory(tmp_path)
            mock_special_helper_method.assert_called_once_with()

    @pytest.mark.parametrize(
        ("special_helper_method",),
        [
            ("_validate",),
            ("_clean_old_committed_checkpoints",),
        ],
    )
    def test_method_calls_call_special_helper_method(self, tmp_path, special_helper_method):
        class CheckpointDirectoryWithNewMethod(CheckpointDirectory):
            def a_new_method(self) -> None: ...

        with patch.object(
            CheckpointDirectoryWithNewMethod,
            special_helper_method,
        ) as mock_special_helper_method:
            checkpoing_directory = CheckpointDirectoryWithNewMethod(tmp_path)
            checkpoing_directory.a_new_method()
            assert mock_special_helper_method.call_args_list == [call(), call()]

    @pytest.mark.parametrize(
        ("committed_checkpoints", "uncommitted_checkpoints", "error_message"),
        [
            (
                None,
                [(0, {"a_source": 0}), (1, {"a_source": 1})],
                "There should be at most one uncommitted checkpoint.",
            ),
            (
                [(0, {"a_source": 0})],
                [(1, {"a_source": 1}), (2, {"a_source": 2})],
                "There should be at most one uncommitted checkpoint.",
            ),
            (
                [(0, {"a_source": 0}), (2, {"a_source": 2})],
                None,
                "Commited checkpoints should be consecutive.",
            ),
            (
                [(0, {"a_source": 0}), (2, {"a_source": 2})],
                [(3, {"a_source": 3})],
                "Commited checkpoints should be consecutive.",
            ),
            (
                None,
                [(1, {"a_source": 1})],
                "Uncommitted checkpoint must be one greater than maximum committed checkpoint, "
                "or zero if there isn't one.",
            ),
            (
                [(0, {"a_source": 0}), (1, {"a_source": 1})],
                [(3, {"a_source": 3})],
                "Uncommitted checkpoint must be one greater than maximum committed checkpoint, "
                "or zero if there isn't one.",
            ),
            (
                [(0, {"a_source": 0}), (1, {"a_source": 1})],
                [(1, {"a_source": 1})],
                "Uncommitted checkpoint must be one greater than maximum committed checkpoint, "
                "or zero if there isn't one.",
            ),
            (
                [(1, {"a_source": 1}), (2, {"a_source": 2})],
                [(0, {"a_source": 0})],
                "Uncommitted checkpoint must be one greater than maximum committed checkpoint, "
                "or zero if there isn't one.",
            ),
        ],
    )
    def test_invalid_state_raises(
        self,
        tmp_path,
        committed_checkpoints,
        uncommitted_checkpoints,
        error_message,
    ):
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=committed_checkpoints,
            uncommitted_checkpoints=uncommitted_checkpoints,
        )
        with pytest.raises(
            CheckpointDirectoryValidityError,
            match=error_message,
        ):
            CheckpointDirectory(tmp_path)

    @pytest.mark.parametrize(
        (
            "committed_checkpoints",
            "uncommitted_checkpoints",
            "retain_old_committed_checkpoints",
            "expected_surviving_committed_checkpoint_ints",
        ),
        [
            (None, None, 2, []),
            ([(0, {"a_source": 0})], None, 2, [0]),
            ([(0, {"a_source": 0})], [(1, {"a_source": 1})], 2, [0]),
            ([(0, {"a_source": 0}), (1, {"a_source": 1})], None, 2, [0, 1]),
            ([(0, {"a_source": 0}), (1, {"a_source": 1})], [(2, {"a_source": 2})], 2, [0, 1]),
            ([(0, {"a_source": 0}), (1, {"a_source": 1}), (2, {"a_source": 2})], None, 2, [1, 2]),
            (
                [(0, {"a_source": 0}), (1, {"a_source": 1}), (2, {"a_source": 2})],
                [(3, {"a_source": 3})],
                2,
                [1, 2],
            ),
            (
                [(0, {"a_source": 0}), (1, {"a_source": 1}), (2, {"a_source": 2})],
                None,
                3,
                [0, 1, 2],
            ),
            (
                [(0, {"a_source": 0}), (1, {"a_source": 1}), (2, {"a_source": 2})],
                [(3, {"a_source": 3})],
                3,
                [0, 1, 2],
            ),
            (
                [
                    (0, {"a_source": 0}),
                    (1, {"a_source": 1}),
                    (2, {"a_source": 2}),
                    (3, {"a_source": 3}),
                ],
                None,
                3,
                [1, 2, 3],
            ),
            (
                [
                    (0, {"a_source": 0}),
                    (1, {"a_source": 1}),
                    (2, {"a_source": 2}),
                    (3, {"a_source": 3}),
                ],
                [(4, {"a_source": 4})],
                3,
                [1, 2, 3],
            ),
            (
                [
                    (0, {"a_source": 0}),
                    (1, {"a_source": 1}),
                    (2, {"a_source": 2}),
                    (3, {"a_source": 3}),
                    (4, {"a_source": 4}),
                ],
                None,
                3,
                [2, 3, 4],
            ),
            (
                [
                    (0, {"a_source": 0}),
                    (1, {"a_source": 1}),
                    (2, {"a_source": 2}),
                    (3, {"a_source": 3}),
                    (4, {"a_source": 4}),
                ],
                [(5, {"a_source": 5})],
                3,
                [2, 3, 4],
            ),
        ],
    )
    def test_excess_old_committed_checkpoints_are_cleaned_correctly(
        self,
        tmp_path,
        committed_checkpoints,
        uncommitted_checkpoints,
        retain_old_committed_checkpoints,
        expected_surviving_committed_checkpoint_ints,
    ):
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=committed_checkpoints,
            uncommitted_checkpoints=uncommitted_checkpoints,
        )
        CheckpointDirectory(
            tmp_path,
            retain_old_committed_checkpoints=retain_old_committed_checkpoints,
        )
        (
            surviving_committed_checkpoints,
            surviving_uncommitted_checkpoints,
        ) = get_checkpoint_directory_state(tmp_path)
        surviving_committed_checkpoint_ints = [t[0] for t in surviving_committed_checkpoints]
        surviving_uncommitted_checkpoint_ints = [t[0] for t in surviving_uncommitted_checkpoints]
        assert sorted(surviving_committed_checkpoint_ints) == sorted(
            expected_surviving_committed_checkpoint_ints,
        )
        assert surviving_uncommitted_checkpoint_ints == (
            [] if not uncommitted_checkpoints else [uncommitted_checkpoints[0][0]]
        )

    @pytest.mark.parametrize(
        ("committed_checkpoints", "uncommitted_checkpoints", "expected_committed_checkpoint"),
        [
            (None, None, None),
            (None, [(0, {"a_source": 0})], None),
            ([(0, {"a_source": 0})], None, Checkpoint(number=0, data={"a_source": 0})),
            (
                [(0, {"a_source": 0})],
                [(1, {"a_source": 1})],
                Checkpoint(number=0, data={"a_source": 0}),
            ),
            (
                [(0, {"a_source": 0}), (1, {"a_source": 1})],
                None,
                Checkpoint(number=1, data={"a_source": 1}),
            ),
            (
                [(0, {"a_source": 0}), (1, {"a_source": 1})],
                [(2, {"a_source": 2})],
                Checkpoint(number=1, data={"a_source": 1}),
            ),
        ],
    )
    def test_max_committed_correct(
        self,
        tmp_path,
        committed_checkpoints,
        uncommitted_checkpoints,
        expected_committed_checkpoint,
    ):
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=committed_checkpoints,
            uncommitted_checkpoints=uncommitted_checkpoints,
        )
        checkpoint_directory = CheckpointDirectory(tmp_path)
        assert checkpoint_directory.max_committed == expected_committed_checkpoint

    @pytest.mark.parametrize(
        ("committed_checkpoints", "uncommitted_checkpoints", "expected_committed_checkpoint"),
        [
            (None, None, None),
            (None, [(0, {"a_source": 0})], Checkpoint(number=0, data={"a_source": 0})),
            ([(0, {"a_source": 0})], None, None),
            (
                [(0, {"a_source": 0})],
                [(1, {"a_source": 1})],
                Checkpoint(number=1, data={"a_source": 1}),
            ),
            ([(0, {"a_source": 0}), (1, {"a_source": 1})], None, None),
            (
                [(0, {"a_source": 0}), (1, {"a_source": 1})],
                [(2, {"a_source": 2})],
                Checkpoint(number=2, data={"a_source": 2}),
            ),
        ],
    )
    def test_uncommitted_correct(
        self,
        tmp_path,
        committed_checkpoints,
        uncommitted_checkpoints,
        expected_committed_checkpoint,
    ):
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=committed_checkpoints,
            uncommitted_checkpoints=uncommitted_checkpoints,
        )
        checkpoint_directory = CheckpointDirectory(tmp_path)
        assert checkpoint_directory.uncommitted == expected_committed_checkpoint

    @pytest.mark.parametrize(
        ("committed_checkpoints", "uncommitted_checkpoints"),
        [
            (None, [(0, {"a_source": 0})]),
            ([(0, {"a_source": 0})], [(1, {"a_source": 1})]),
            ([(0, {"a_source": 0}), (1, {"a_source": 1})], [(2, {"a_source": 2})]),
        ],
    )
    def test_create_uncommitted_raises_if_uncommitted_already_exists(
        self,
        tmp_path,
        committed_checkpoints,
        uncommitted_checkpoints,
    ):
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=committed_checkpoints,
            uncommitted_checkpoints=uncommitted_checkpoints,
        )
        checkpoint_directory = CheckpointDirectory(tmp_path)
        with pytest.raises(
            CheckpointDirectoryInvalidOperationError,
            match="There is already an uncommitted checkpoint.",
        ):
            checkpoint_directory.create_uncommitted({"a_source": 9001})

    @pytest.mark.parametrize(
        ("committed_checkpoints", "expected_uncommitted_int"),
        [
            (None, 0),
            ([(0, {"a_source": 0})], 1),
            ([(0, {"a_source": 0}), (1, {"a_source": 1})], 2),
        ],
    )
    def test_create_uncommitted_works(
        self,
        tmp_path: Path,
        committed_checkpoints,
        expected_uncommitted_int,
    ):
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=committed_checkpoints,
        )
        _, existing_uncommitted_checkpoints = get_checkpoint_directory_state(tmp_path)
        assert not existing_uncommitted_checkpoints
        checkpoint_directory = CheckpointDirectory(tmp_path)
        data_to_write: JsonableNonNull = {"a_source": 9001}
        checkpoint_directory.create_uncommitted(data_to_write)
        _, actual_uncommitted_checkpoints = get_checkpoint_directory_state(tmp_path)
        assert actual_uncommitted_checkpoints == [(expected_uncommitted_int, data_to_write)]

    @pytest.mark.parametrize(
        ("committed_checkpoints",),
        [
            (None,),
            ([(0, {"a_source": 0})],),
            ([(0, {"a_source": 0}), (1, {"a_source": 1})],),
        ],
    )
    def test_remove_uncommitted_raises_if_nothing_remove(
        self,
        tmp_path,
        committed_checkpoints,
    ):
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=committed_checkpoints,
        )
        checkpoint_directory = CheckpointDirectory(tmp_path)
        with pytest.raises(
            CheckpointDirectoryInvalidOperationError,
            match="There is no uncommitted checkpoint to remove.",
        ):
            checkpoint_directory.remove_uncommitted()

    @pytest.mark.parametrize(
        ("committed_checkpoints", "uncommitted_checkpoint"),
        [
            (None, (0, {"a_source": 0})),
            ([(0, {"a_source": 0})], (1, {"a_source": 1})),
            ([(0, {"a_source": 0}), (1, {"a_source": 1})], (2, {"a_source": 2})),
        ],
    )
    def test_remove_uncommitted_works(
        self,
        tmp_path,
        committed_checkpoints,
        uncommitted_checkpoint,
    ):
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=committed_checkpoints,
            uncommitted_checkpoints=[uncommitted_checkpoint],
        )
        checkpoint_directory = CheckpointDirectory(tmp_path)
        checkpoint_directory.remove_uncommitted()
        assert get_checkpoint_directory_state(tmp_path) == (
            committed_checkpoints or [],
            [],
        )

    @pytest.mark.parametrize(
        ("committed_checkpoints",),
        [
            (None,),
            ([(0, {"a_source": 0})],),
            ([(0, {"a_source": 0}), (1, {"a_source": 1})],),
        ],
    )
    def test_commit_raises_if_nothing_to_commit(
        self,
        tmp_path,
        committed_checkpoints,
    ):
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=committed_checkpoints,
        )
        checkpoint_directory = CheckpointDirectory(tmp_path)
        with pytest.raises(
            CheckpointDirectoryInvalidOperationError,
            match="There is no uncommitted checkpoint to commit.",
        ):
            checkpoint_directory.commit()

    @pytest.mark.parametrize(
        ("committed_checkpoints", "uncommitted_checkpoint"),
        [
            (None, (0, {"a_source": 0})),
            ([(0, {"a_source": 0})], (1, {"a_source": 1})),
            ([(0, {"a_source": 0}), (1, {"a_source": 1})], (2, {"a_source": 2})),
        ],
    )
    def test_commit_works(
        self,
        tmp_path,
        committed_checkpoints,
        uncommitted_checkpoint,
    ):
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=committed_checkpoints,
            uncommitted_checkpoints=[uncommitted_checkpoint],
        )
        checkpoint_directory = CheckpointDirectory(tmp_path)
        checkpoint_directory.commit()
        assert get_checkpoint_directory_state(tmp_path) == (
            [*(committed_checkpoints or []), uncommitted_checkpoint],
            [],
        )
