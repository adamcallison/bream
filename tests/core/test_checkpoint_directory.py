from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import call, patch

import pytest

from bream._exceptions import (
    CheckpointDirectoryInvalidOperationError,
    CheckpointDirectoryValidityError,
    CheckpointFileCorruptionError,
)
from bream.core._checkpoint_directory import (
    Checkpoint,
    CheckpointDirectory,
    CheckpointStatus,
)
from tests.core.helpers import (
    freeze_default_committed_at_time,
    freeze_default_created_at_time,
    get_checkpoint_directory_state,
    make_checkpoint,
    make_committed_checkpoint,
    setup_checkpoint_directory,
)

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
            (CheckpointDirectory._raise_if_unrecoverable_invalid_state_from_paths.__name__,),  # noqa: SLF001
            (CheckpointDirectory._repair_uncommitted_duplicates_from_paths.__name__,),  # noqa: SLF001
            (CheckpointDirectory._clean_old_committed_checkpoints_from_paths.__name__,),  # noqa: SLF001
        ],
    )
    def test__init__calls_special_helper_method(self, tmp_path, special_helper_method):
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=[make_committed_checkpoint(0, 0)],
            uncommitted_checkpoints=[make_checkpoint(1, 1)],
        )
        with patch.object(CheckpointDirectory, special_helper_method) as mock_special_helper_method:
            CheckpointDirectory(tmp_path)
            assert mock_special_helper_method.call_args_list == [
                call(
                    [tmp_path / f"0.{CheckpointStatus.COMMITTED}"],
                    [tmp_path / f"1.{CheckpointStatus.UNCOMMITTED}"],
                ),
            ]

    @pytest.mark.parametrize(
        ("special_helper_method",),
        [
            (CheckpointDirectory._raise_if_unrecoverable_invalid_state_from_paths.__name__,),  # noqa: SLF001
            (CheckpointDirectory._repair_uncommitted_duplicates_from_paths.__name__,),  # noqa: SLF001
            (CheckpointDirectory._clean_old_committed_checkpoints_from_paths.__name__,),  # noqa: SLF001
        ],
    )
    def test_method_calls_call_special_helper_method(self, tmp_path, special_helper_method):
        class CheckpointDirectoryWithNewMethod(CheckpointDirectory):
            def a_new_method(self) -> None: ...

        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=[make_committed_checkpoint(0, 0)],
            uncommitted_checkpoints=[make_checkpoint(1, 1)],
        )

        with patch.object(
            CheckpointDirectoryWithNewMethod,
            special_helper_method,
        ) as mock_special_helper_method:
            checkpoing_directory = CheckpointDirectoryWithNewMethod(tmp_path)
            checkpoing_directory.a_new_method()
            assert (
                mock_special_helper_method.call_args_list
                == [
                    call(
                        [tmp_path / f"0.{CheckpointStatus.COMMITTED}"],
                        [tmp_path / f"1.{CheckpointStatus.UNCOMMITTED}"],
                    ),
                ]
                * 2
            )

    @pytest.mark.parametrize(
        ("committed_checkpoints", "uncommitted_checkpoints", "error_message"),
        [
            (
                None,
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
                "There should be at most one uncommitted checkpoint.",
            ),
            (
                [make_committed_checkpoint(0, {"a_source": 0})],
                [make_checkpoint(1, {"a_source": 1}), make_checkpoint(2, {"a_source": 2})],
                "There should be at most one uncommitted checkpoint.",
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(2, {"a_source": 2}),
                ],
                None,
                "Commited checkpoints should be consecutive.",
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(2, {"a_source": 2}),
                ],
                [make_checkpoint(3, {"a_source": 3})],
                "Commited checkpoints should be consecutive.",
            ),
            (
                None,
                [make_checkpoint(1, {"a_source": 1})],
                "Uncommitted checkpoint must be one greater than maximum committed checkpoint, "
                "or zero if there isn't one.",
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
                [make_checkpoint(3, {"a_source": 3})],
                "Uncommitted checkpoint must be one greater than maximum committed checkpoint, "
                "or zero if there isn't one.",
            ),
            (
                [
                    make_committed_checkpoint(1, {"a_source": 1}),
                    make_committed_checkpoint(2, {"a_source": 2}),
                ],
                [make_checkpoint(0, {"a_source": 0})],
                "Uncommitted checkpoint must be one greater than maximum committed checkpoint, "
                "or zero if there isn't one.",
            ),
        ],
    )
    def test_unrecoverable_invalid_state_raises(
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
            "initial_committed_checkpoints",
            "initial_uncommitted_checkpoints",
            "expected_final_committed_checkpoints",
            "expected_final_uncommitted_checkpoints",
        ),
        [
            (
                [make_committed_checkpoint(0, {"a_source": 0})],
                [make_checkpoint(0, {"a_source": 0})],
                [make_committed_checkpoint(0, {"a_source": 0})],
                [],
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
                [make_checkpoint(0, {"a_source": 0})],
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
                [],
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
                [make_checkpoint(1, {"a_source": 1})],
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
                [],
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
                [make_checkpoint(0, {"a_source": 0}), make_checkpoint(1, {"a_source": 1})],
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
                [],
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
                [make_checkpoint(1, {"a_source": 1}), make_checkpoint(2, {"a_source": 2})],
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
                [make_checkpoint(2, {"a_source": 2})],
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
                [
                    make_checkpoint(0, {"a_source": 0}),
                    make_checkpoint(1, {"a_source": 1}),
                    make_checkpoint(2, {"a_source": 2}),
                ],
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
                [make_checkpoint(2, {"a_source": 2})],
            ),
        ],
    )
    def test_recoverable_invalid_state_is_repaired(
        self,
        tmp_path,
        initial_committed_checkpoints,
        initial_uncommitted_checkpoints,
        expected_final_committed_checkpoints,
        expected_final_uncommitted_checkpoints,
    ):
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=initial_committed_checkpoints,
            uncommitted_checkpoints=initial_uncommitted_checkpoints,
        )
        CheckpointDirectory(tmp_path)
        (
            final_committed_checkpoints,
            final_uncommitted_checkpoints,
        ) = get_checkpoint_directory_state(tmp_path)
        assert final_committed_checkpoints == expected_final_committed_checkpoints
        assert final_uncommitted_checkpoints == expected_final_uncommitted_checkpoints

    @pytest.mark.parametrize(
        (
            "committed_checkpoints",
            "uncommitted_checkpoints",
            "retain_old_committed_checkpoints",
            "expected_surviving_committed_checkpoint_ints",
        ),
        [
            (None, None, 2, []),
            ([make_committed_checkpoint(0, {"a_source": 0})], None, 2, [0]),
            (
                [make_committed_checkpoint(0, {"a_source": 0})],
                [make_checkpoint(1, {"a_source": 1})],
                2,
                [0],
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
                None,
                2,
                [0, 1],
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
                [make_checkpoint(2, {"a_source": 2})],
                2,
                [0, 1],
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                    make_committed_checkpoint(2, {"a_source": 2}),
                ],
                None,
                2,
                [1, 2],
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                    make_committed_checkpoint(2, {"a_source": 2}),
                ],
                [make_checkpoint(3, {"a_source": 3})],
                2,
                [1, 2],
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                    make_committed_checkpoint(2, {"a_source": 2}),
                ],
                None,
                3,
                [0, 1, 2],
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                    make_committed_checkpoint(2, {"a_source": 2}),
                ],
                [make_checkpoint(3, {"a_source": 3})],
                3,
                [0, 1, 2],
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                    make_committed_checkpoint(2, {"a_source": 2}),
                    make_committed_checkpoint(3, {"a_source": 3}),
                ],
                None,
                3,
                [1, 2, 3],
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                    make_committed_checkpoint(2, {"a_source": 2}),
                    make_committed_checkpoint(3, {"a_source": 3}),
                ],
                [make_checkpoint(4, {"a_source": 4})],
                3,
                [1, 2, 3],
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                    make_committed_checkpoint(2, {"a_source": 2}),
                    make_committed_checkpoint(3, {"a_source": 3}),
                    make_committed_checkpoint(4, {"a_source": 4}),
                ],
                None,
                3,
                [2, 3, 4],
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                    make_committed_checkpoint(2, {"a_source": 2}),
                    make_committed_checkpoint(3, {"a_source": 3}),
                    make_committed_checkpoint(4, {"a_source": 4}),
                ],
                [make_checkpoint(5, {"a_source": 5})],
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
        surviving_committed_checkpoint_ints = [t.number for t in surviving_committed_checkpoints]
        surviving_uncommitted_checkpoint_ints = [
            t.number for t in surviving_uncommitted_checkpoints
        ]
        assert sorted(surviving_committed_checkpoint_ints) == sorted(
            expected_surviving_committed_checkpoint_ints,
        )
        assert surviving_uncommitted_checkpoint_ints == (
            [] if not uncommitted_checkpoints else [uncommitted_checkpoints[0].number]
        )

    @pytest.mark.parametrize(
        ("committed_checkpoints", "uncommitted_checkpoints", "expected_committed_checkpoint"),
        [
            (None, None, None),
            (None, [make_checkpoint(0, {"a_source": 0})], None),
            (
                [make_committed_checkpoint(0, {"a_source": 0})],
                None,
                make_committed_checkpoint(0, {"a_source": 0}),
            ),
            (
                [make_committed_checkpoint(0, {"a_source": 0})],
                [make_checkpoint(1, {"a_source": 1})],
                make_committed_checkpoint(0, {"a_source": 0}),
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
                None,
                make_committed_checkpoint(1, {"a_source": 1}),
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
                [make_checkpoint(2, {"a_source": 2})],
                make_committed_checkpoint(1, {"a_source": 1}),
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
        ("committed_checkpoints", "uncommitted_checkpoints", "expected_uncommitted_checkpoint"),
        [
            (None, None, None),
            (
                None,
                [make_checkpoint(0, {"a_source": 0})],
                make_checkpoint(0, {"a_source": 0}),
            ),
            ([make_committed_checkpoint(0, {"a_source": 0})], None, None),
            (
                [make_committed_checkpoint(0, {"a_source": 0})],
                [make_checkpoint(1, {"a_source": 1})],
                make_checkpoint(1, {"a_source": 1}),
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
                None,
                None,
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
                [make_checkpoint(2, {"a_source": 2})],
                make_checkpoint(2, {"a_source": 2}),
            ),
        ],
    )
    def test_uncommitted_correct(
        self,
        tmp_path,
        committed_checkpoints,
        uncommitted_checkpoints,
        expected_uncommitted_checkpoint,
    ):
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=committed_checkpoints,
            uncommitted_checkpoints=uncommitted_checkpoints,
        )
        checkpoint_directory = CheckpointDirectory(tmp_path)
        assert checkpoint_directory.uncommitted == expected_uncommitted_checkpoint

    @pytest.mark.parametrize(
        ("committed_checkpoints", "uncommitted_checkpoints"),
        [
            (None, [make_checkpoint(0, {"a_source": 0})]),
            (
                [make_committed_checkpoint(0, {"a_source": 0})],
                [make_checkpoint(1, {"a_source": 1})],
            ),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
                [make_checkpoint(2, {"a_source": 2})],
            ),
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
            ([make_committed_checkpoint(0, {"a_source": 0})], 1),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
                2,
            ),
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
        checkpoint_data_to_write: JsonableNonNull = {"a_source": 9001}
        with freeze_default_created_at_time():
            checkpoint_directory.create_uncommitted(checkpoint_data_to_write)
        _, actual_uncommitted_checkpoints = get_checkpoint_directory_state(tmp_path)
        assert actual_uncommitted_checkpoints == [
            make_checkpoint(expected_uncommitted_int, checkpoint_data_to_write),
        ]

    @pytest.mark.parametrize(
        ("committed_checkpoints",),
        [
            (None,),
            ([make_committed_checkpoint(0, {"a_source": 0})],),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
            ),
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
            (None, make_checkpoint(0, {"a_source": 0})),
            ([make_committed_checkpoint(0, {"a_source": 0})], make_checkpoint(1, {"a_source": 1})),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
                make_checkpoint(2, {"a_source": 2}),
            ),
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
            (committed_checkpoints or []),
            [],
        )

    @pytest.mark.parametrize(
        ("committed_checkpoints",),
        [
            (None,),
            ([make_committed_checkpoint(0, {"a_source": 0})],),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
            ),
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
            (None, make_checkpoint(0, {"a_source": 0})),
            ([make_committed_checkpoint(0, {"a_source": 0})], make_checkpoint(1, {"a_source": 1})),
            (
                [
                    make_committed_checkpoint(0, {"a_source": 0}),
                    make_committed_checkpoint(1, {"a_source": 1}),
                ],
                make_checkpoint(2, {"a_source": 2}),
            ),
        ],
    )
    def test_commit_works(
        self,
        tmp_path,
        committed_checkpoints,
        uncommitted_checkpoint: Checkpoint,
    ):
        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=committed_checkpoints,
            uncommitted_checkpoints=[uncommitted_checkpoint],
        )
        checkpoint_directory = CheckpointDirectory(tmp_path)

        with freeze_default_committed_at_time():
            checkpoint_directory.commit()
        assert get_checkpoint_directory_state(tmp_path) == (
            [
                *(committed_checkpoints or []),
                make_committed_checkpoint(
                    uncommitted_checkpoint.number,
                    uncommitted_checkpoint.checkpoint_data,
                ),
            ],
            [],
        )


class TestCheckpoint:
    @pytest.mark.parametrize(
        ("status",),
        [
            (CheckpointStatus.COMMITTED,),
            (CheckpointStatus.UNCOMMITTED,),
        ],
    )
    def test_to_json_creates_correct_file(self, tmp_path, status):
        """Test Checkpoint.to_json creates file with correct extension."""
        checkpoint = make_checkpoint(5, {"source1": 100, "source2": 200})

        checkpoint_path = tmp_path / "checkpoints"
        checkpoint.to_json(checkpoint_path, status=status)

        committed_checkpoints, uncommitted_checkpoints = get_checkpoint_directory_state(
            checkpoint_path,
        )

        assert committed_checkpoints == (
            [make_checkpoint(5, {"source1": 100, "source2": 200})]
            if status == CheckpointStatus.COMMITTED
            else []
        )
        assert uncommitted_checkpoints == (
            [make_checkpoint(5, {"source1": 100, "source2": 200})]
            if status == CheckpointStatus.UNCOMMITTED
            else []
        )

    @pytest.mark.parametrize(
        ("file_num", "status", "checkpoint_data"),
        [
            (7, CheckpointStatus.COMMITTED, {"source1": 300, "source2": 400}),
            (2, CheckpointStatus.UNCOMMITTED, {"source1": 75}),
            (0, CheckpointStatus.COMMITTED, {"test": "zero"}),
            (999, CheckpointStatus.UNCOMMITTED, {"large": "number"}),
        ],
    )
    def test_from_json_parses_correctly(
        self,
        tmp_path,
        file_num,
        status,
        checkpoint_data,
    ):
        """Test Checkpoint.from_json parses checkpoint correctly."""

        setup_checkpoint_directory(
            tmp_path,
            committed_checkpoints=(
                [
                    make_committed_checkpoint(file_num, checkpoint_data),
                ]
                if status == CheckpointStatus.COMMITTED
                else []
            ),
            uncommitted_checkpoints=(
                [
                    make_checkpoint(file_num, checkpoint_data),
                ]
                if status == CheckpointStatus.UNCOMMITTED
                else []
            ),
        )

        checkpoint = Checkpoint.from_json(tmp_path / f"{file_num}.{status}")

        expected_checkpoint = (
            make_checkpoint if status == CheckpointStatus.UNCOMMITTED else make_committed_checkpoint
        )(
            file_num,
            checkpoint_data,
        )
        assert checkpoint == expected_checkpoint

    @pytest.mark.parametrize(
        ("corrupt_content",),
        [
            ("{ invalid json content",),
            ("",),
            ('{"missing": "required_fields"}',),
        ],
    )
    def test_from_json_with_corrupted_json_raises(
        self,
        tmp_path,
        corrupt_content,
    ):
        """Test that corrupted checkpoint files raise CheckpointFileCorruptionError."""
        checkpoint_file = tmp_path / "123.json"
        checkpoint_file.write_text(corrupt_content)

        with pytest.raises(CheckpointFileCorruptionError, match="Checkpoint file .* is corrupted"):
            Checkpoint.from_json(checkpoint_file)
