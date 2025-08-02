from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from bream.core._utils import dump_json_atomically


class TestDumpJsonAtomically:
    def test_writes_json_data_to_destination_file(self, tmp_path):
        data = {"key": "value", "number": 42}
        dst = tmp_path / "output.json"
        tmp = tmp_path / "temp.json"

        dump_json_atomically(data, dst, tmp)

        assert dst.exists()
        assert not tmp.exists()
        with dst.open("r") as f:
            loaded_data = json.load(f)
        assert loaded_data == data

    def test_handles_complex_nested_data(self, tmp_path):
        data = {
            "nested": {"list": [1, 2, 3], "dict": {"inner": "value"}},
            "array": [{"item": 1}, {"item": 2}],
        }
        dst = tmp_path / "complex.json"
        tmp = tmp_path / "temp_complex.json"

        dump_json_atomically(data, dst, tmp)

        with dst.open("r") as f:
            loaded_data = json.load(f)
        assert loaded_data == data

    def test_overwrites_existing_destination_file(self, tmp_path):
        original_data = {"old": "data"}
        new_data = {"new": "data"}
        dst = tmp_path / "existing.json"
        tmp = tmp_path / "temp.json"

        # Create existing file
        with dst.open("w") as f:
            json.dump(original_data, f)

        dump_json_atomically(new_data, dst, tmp)

        with dst.open("r") as f:
            loaded_data = json.load(f)
        assert loaded_data == new_data

    def test_temporary_file_is_removed_on_success(self, tmp_path):
        data = {"test": "data"}
        dst = tmp_path / "output.json"
        tmp = tmp_path / "temp.json"

        dump_json_atomically(data, dst, tmp)

        assert not tmp.exists()
        assert dst.exists()

    def test_raises_on_invalid_json_data(self, tmp_path):
        # Using a set which is not JSON serializable
        invalid_data = {"set": {1, 2, 3}}
        dst = tmp_path / "output.json"
        tmp = tmp_path / "temp.json"

        with pytest.raises(TypeError):
            dump_json_atomically(invalid_data, dst, tmp)

    def test_destination_not_created_on_write_interruption(self, tmp_path):
        original_data = {"original": "content"}
        new_data = {"new": "content"}
        dst = tmp_path / "output.json"
        tmp = tmp_path / "temp.json"

        # Create existing destination file
        with dst.open("w") as f:
            json.dump(original_data, f)

        # Mock json.dump to raise an exception during write
        def _interrupted_json_dump(data, f) -> None:
            data_str = str(data)
            data_str_partial = data_str[: len(data_str) // 2]
            f.write(data_str_partial)
            msg = "Simulated write failure"
            raise RuntimeError(msg)

        with (
            patch("bream.core._utils.json.dump", _interrupted_json_dump),
            pytest.raises(RuntimeError, match="Simulated write failure"),
        ):
            dump_json_atomically(new_data, dst, tmp)

        # Verify original destination file is unchanged
        assert dst.exists()
        with dst.open("r") as f:
            loaded_data = json.load(f)
        assert loaded_data == original_data

        # Verify tmp file is in expected corrupt state
        assert tmp.exists()
        with tmp.open("r") as f:
            loaded_partial = f.read()
        assert loaded_partial == str(new_data)[: len(str(new_data)) // 2]
