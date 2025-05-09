from __future__ import annotations

from time import sleep, time
from typing import TYPE_CHECKING

import pytest

from bream._exceptions import StreamLogicalError
from bream.core._definitions import Batch, Batches, BatchRequest, Source, StreamOptions
from bream.core._stream import WAITHELPER_ITERATION_INTERVAL, Stream

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


def get_well_behaved_function_and_batches_seen_list() -> tuple[
    Callable[[Batches], None],
    list[Batches],
]:
    batches_seen: list[Batches] = []

    def batch_function(batches: Batches) -> None:
        nonlocal batches_seen
        batches_seen.append(batches)

    return batch_function, batches_seen


def get_flaky_function_and_batches_seen_list() -> tuple[
    Callable[[Batches], None],
    list[Batches],
]:
    batches_seen: list[Batches] = []
    call_count = 0

    def batch_function(batches: Batches) -> None:
        nonlocal batches_seen
        nonlocal call_count
        call_count += 1
        if call_count % 2 == 0:
            raise RuntimeError
        batches_seen.append(batches)

    return batch_function, batches_seen


def get_flaky_function_and_batches_seen_with_repeats_list() -> tuple[
    Callable[[Batches], None],
    list[Batches],
]:
    batches_seen: list[Batches] = []
    call_count = 0

    def batch_function(batches: Batches) -> None:
        nonlocal batches_seen
        nonlocal call_count
        call_count += 1
        batches_seen.append(batches)
        if call_count % 2 == 0:
            raise RuntimeError

    return batch_function, batches_seen

def get_slow_function_and_batches_seen_list() -> tuple[
    Callable[[Batches], None],
    list[Batches],
]:
    batches_seen: list[Batches] = []

    def batch_function(batches: Batches) -> None:
        nonlocal batches_seen
        batches_seen.append(batches)
        sleep(1)

    return batch_function, batches_seen


def get_slow_function_that_errors_and_batches_seen_list() -> tuple[
    Callable[[Batches], None],
    list[Batches],
]:
    batches_seen: list[Batches] = []

    def batch_function(batches: Batches) -> None:
        nonlocal batches_seen
        batches_seen.append(batches)
        sleep(1)
        raise RuntimeError

    return batch_function, batches_seen


class SimpleDictSource(Source):
    def __init__(self, name: str, num_dicts: int, dicts_per_batch: int) -> None:
        self.name = name
        self._dicts = [{"id": j, "data": f"{name}_data_{j}"} for j in range(num_dicts)]
        self._dicts_per_batch = dicts_per_batch

    def read(self, batch_request: BatchRequest) -> Batch | None:
        read_from = (
            (batch_request.read_from_after + 1) if batch_request.read_from_after is not None else 0
        )

        if batch_request.read_to is not None:
            read_to = batch_request.read_to
        else:
            num_dicts = len(self._dicts)
            if read_from == num_dicts:
                return None
            read_to = min(num_dicts - 1, read_from + self._dicts_per_batch - 1)

        data = self._dicts[read_from : read_to + 1]
        return Batch(data=data, read_to=read_to)


class TestStream:

    def _basic_simpledictsource_stream(
        self,
        stream_path: Path,
        source_name: str,
        num_dicts: int,
        dicts_per_batch: int,
    ) -> tuple[Source, Stream]:
        source = SimpleDictSource(source_name, num_dicts, dicts_per_batch)
        stream = Stream([source], stream_path)
        return source, stream


    def test_default_options_are_as_expected(self, tmp_path):
        source, stream = self._basic_simpledictsource_stream(tmp_path / "stream", "source", 13, 3)
        assert stream.options == StreamOptions(
            repeat_failed_batch_exactly=True,
        )

    def test_options_are_respected(self, tmp_path):
        source = SimpleDictSource("source", 13, 3)
        stream_path = tmp_path / "stream"
        stream = Stream([source], stream_path, StreamOptions(repeat_failed_batch_exactly=False))
        assert stream.options == StreamOptions(
            repeat_failed_batch_exactly=False,
        )

    def test_stop_with_blocking_blocks(self, tmp_path):
        source, stream = self._basic_simpledictsource_stream(tmp_path / "stream", "source", 13, 3)
        batch_function, _ = get_slow_function_and_batches_seen_list()
        stream.start(batch_function, 0.01)
        stream.stop(blocking=True)
        assert not stream.status.active
        assert stream.status.error is None

    def test_start_respects_min_batch_seconds(self, tmp_path: Path) -> None:
        source, stream = self._basic_simpledictsource_stream(tmp_path / "stream", "source", 13, 3)
        batch_function, batches_seen = get_well_behaved_function_and_batches_seen_list()
        stream.start(batch_function, 0.75)
        sleep(1.15)
        stream.stop(blocking=True)
        expected_number_of_batches_seen = 2
        assert len(batches_seen) == expected_number_of_batches_seen

    def test_stop_stops_during_waiting(self, tmp_path):
        source, stream = self._basic_simpledictsource_stream(tmp_path / "stream", "source", 13, 3)
        batch_function, _ = get_well_behaved_function_and_batches_seen_list()
        stime = time()
        stream.start(batch_function, 4*WAITHELPER_ITERATION_INTERVAL)
        sleep(0.1)
        stream.stop(blocking=True)
        etime = time()
        assert not stream.status.active
        assert stream.status.error is None
        assert etime - stime < 2*WAITHELPER_ITERATION_INTERVAL

    def test_stop_without_blocking_does_not_block(self, tmp_path):
        source, stream = self._basic_simpledictsource_stream(tmp_path / "stream", "source", 13, 3)
        batch_function, _ = get_slow_function_and_batches_seen_list()
        stream.start(batch_function, 0.0)
        stream.stop(blocking=False)
        assert stream.status.active
        sleep(2)
        assert not stream.status.active
        assert stream.status.error is None

    def test_stream_cannot_be_instantiated_with_duplicate_named_sources(self, tmp_path):
        source1 = SimpleDictSource("source", 13, 3)
        source2 = SimpleDictSource("source", 5, 2)
        stream_path = tmp_path / "stream"
        with pytest.raises(StreamLogicalError):
            Stream([source1, source2], stream_path)

    def test_stream_cannot_be_reinstantiated_with_more_sources(self, tmp_path):
        source1 = SimpleDictSource("source1", 13, 3)
        source2 = SimpleDictSource("source2", 5, 2)
        stream_path = tmp_path / "stream"
        stream = Stream([source1], stream_path)
        batch_function, _ = get_well_behaved_function_and_batches_seen_list()
        stream.start(batch_function, 0.01)
        stream.stop()
        with pytest.raises(StreamLogicalError):
            Stream([source1, source2], stream_path)

    def test_stream_cannot_be_reinstantiated_with_fewer_sources(self, tmp_path):
        source1 = SimpleDictSource("source1", 13, 3)
        source2 = SimpleDictSource("source2", 5, 2)
        stream_path = tmp_path / "stream"
        stream = Stream([source1, source2], stream_path)
        batch_function, _ = get_well_behaved_function_and_batches_seen_list()
        stream.start(batch_function, 0.01)
        stream.stop()
        with pytest.raises(StreamLogicalError):
            Stream([source1], stream_path)

    def test_stream_cannot_be_reinstantiated_with_differently_named_sources(self, tmp_path):
        source1 = SimpleDictSource("source1", 13, 3)
        source2 = SimpleDictSource("source2", 5, 2)
        source3 = SimpleDictSource("source3", 13, 3)
        source4 = SimpleDictSource("source4", 5, 2)
        stream_path = tmp_path / "stream"
        stream = Stream([source1, source2], stream_path)
        batch_function, _ = get_well_behaved_function_and_batches_seen_list()
        stream.start(batch_function, 0.01)
        stream.stop()
        with pytest.raises(StreamLogicalError):
            Stream([source3, source4], stream_path)

    def test_stream_cannot_be_started_twice(self, tmp_path):
        source = SimpleDictSource("source", 13, 3)
        stream_path = tmp_path / "stream"
        stream = Stream([source], stream_path)
        batch_function, _ = get_well_behaved_function_and_batches_seen_list()
        stream.start(batch_function, 0.01)
        stream.stop()
        with pytest.raises(StreamLogicalError):
            stream.start(batch_function, 0.01)

    def test_wait_is_blocking(self, tmp_path):
        source = SimpleDictSource("source", 13, 3)
        stream_path = tmp_path / "stream"
        stream = Stream([source], stream_path)
        batch_function, _ = get_slow_function_that_errors_and_batches_seen_list()
        stream.start(batch_function, 1)
        stream.wait()
        assert not stream.status.active

    @pytest.mark.parametrize(
        ("batch_function_factory", "expect_stream_restarts"),
        [
            (get_well_behaved_function_and_batches_seen_list, False),
            (get_flaky_function_and_batches_seen_list, True),
        ],
    )
    def test_stream_can_process_a_single_source(
        self,
        tmp_path,
        batch_function_factory,
        expect_stream_restarts,
    ) -> None:
        source = SimpleDictSource("source", 13, 3)
        stream_path = tmp_path / "stream"
        batches_seen: list[Batches] = []

        batch_function, batches_seen = batch_function_factory()

        stream: Stream | None = None
        batches_seen_length = len(batches_seen)
        num_stream_starts = 0
        while True:
            if not stream or not stream.status.active:
                stream = Stream([source], stream_path)
                stream.start(batch_function, 0.01)
                num_stream_starts += 1
            sleep(0.1)
            previous_length, batches_seen_length = batches_seen_length, len(batches_seen)
            if previous_length == batches_seen_length:
                break
        stream.stop()

        expected_batches_seen = [
            Batches(
                batches={
                    "source": Batch(
                        data=[
                            {"id": 0, "data": "source_data_0"},
                            {"id": 1, "data": "source_data_1"},
                            {"id": 2, "data": "source_data_2"},
                        ],
                        read_to=2,
                    ),
                },
            ),
            Batches(
                batches={
                    "source": Batch(
                        data=[
                            {"id": 3, "data": "source_data_3"},
                            {"id": 4, "data": "source_data_4"},
                            {"id": 5, "data": "source_data_5"},
                        ],
                        read_to=5,
                    ),
                },
            ),
            Batches(
                batches={
                    "source": Batch(
                        data=[
                            {"id": 6, "data": "source_data_6"},
                            {"id": 7, "data": "source_data_7"},
                            {"id": 8, "data": "source_data_8"},
                        ],
                        read_to=8,
                    ),
                },
            ),
            Batches(
                batches={
                    "source": Batch(
                        data=[
                            {"id": 9, "data": "source_data_9"},
                            {"id": 10, "data": "source_data_10"},
                            {"id": 11, "data": "source_data_11"},
                        ],
                        read_to=11,
                    ),
                },
            ),
            Batches(
                batches={
                    "source": Batch(
                        data=[
                            {"id": 12, "data": "source_data_12"},
                        ],
                        read_to=12,
                    ),
                },
            ),
        ]

        assert batches_seen == expected_batches_seen
        stream_restarts = num_stream_starts > 1
        assert stream_restarts == expect_stream_restarts

    @pytest.mark.parametrize(
        ("batch_function_factory", "expect_stream_restarts"),
        [
            (get_well_behaved_function_and_batches_seen_list, False),
            (get_flaky_function_and_batches_seen_list, True),
        ],
    )
    def test_stream_can_process_multiple_sources_of_different_length_and_rate(
        self,
        tmp_path,
        batch_function_factory,
        expect_stream_restarts,
    ) -> None:
        source1 = SimpleDictSource("source1", 13, 3)
        source2 = SimpleDictSource("source2", 5, 2)
        stream_path = tmp_path / "stream"
        batches_seen: list[Batches] = []

        batch_function, batches_seen = batch_function_factory()

        stream: Stream | None = None
        batches_seen_length = len(batches_seen)
        num_stream_starts = 0
        while True:
            if not stream or not stream.status.active:
                stream = Stream([source1, source2], stream_path)
                stream.start(batch_function, 0.01)
                num_stream_starts += 1
            sleep(0.1)
            previous_length, batches_seen_length = batches_seen_length, len(batches_seen)
            if previous_length == batches_seen_length:
                break
        stream.stop()

        expected_batches_seen = [
            Batches(
                batches={
                    "source1": Batch(
                        data=[
                            {"id": 0, "data": "source1_data_0"},
                            {"id": 1, "data": "source1_data_1"},
                            {"id": 2, "data": "source1_data_2"},
                        ],
                        read_to=2,
                    ),
                    "source2": Batch(
                        data=[
                            {"id": 0, "data": "source2_data_0"},
                            {"id": 1, "data": "source2_data_1"},
                        ],
                        read_to=1,
                    ),
                },
            ),
            Batches(
                batches={
                    "source1": Batch(
                        data=[
                            {"id": 3, "data": "source1_data_3"},
                            {"id": 4, "data": "source1_data_4"},
                            {"id": 5, "data": "source1_data_5"},
                        ],
                        read_to=5,
                    ),
                    "source2": Batch(
                        data=[
                            {"id": 2, "data": "source2_data_2"},
                            {"id": 3, "data": "source2_data_3"},
                        ],
                        read_to=3,
                    ),
                },
            ),
            Batches(
                batches={
                    "source1": Batch(
                        data=[
                            {"id": 6, "data": "source1_data_6"},
                            {"id": 7, "data": "source1_data_7"},
                            {"id": 8, "data": "source1_data_8"},
                        ],
                        read_to=8,
                    ),
                    "source2": Batch(data=[{"id": 4, "data": "source2_data_4"}], read_to=4),
                },
            ),
            Batches(
                batches={
                    "source1": Batch(
                        data=[
                            {"id": 9, "data": "source1_data_9"},
                            {"id": 10, "data": "source1_data_10"},
                            {"id": 11, "data": "source1_data_11"},
                        ],
                        read_to=11,
                    ),
                    "source2": None,
                },
            ),
            Batches(
                batches={
                    "source1": Batch(data=[{"id": 12, "data": "source1_data_12"}], read_to=12),
                    "source2": None,
                },
            ),
        ]

        assert batches_seen == expected_batches_seen
        stream_restarts = num_stream_starts > 1
        assert stream_restarts == expect_stream_restarts

    def test_stream_repeats_failed_batch_exactly_if_configured_to(
        self,
        tmp_path,
    ) -> None:

        stream_path = tmp_path / "stream"
        batch_function, batches_seen = get_flaky_function_and_batches_seen_with_repeats_list()

        source = SimpleDictSource("source", 13, 2)
        stream = Stream([source], stream_path)
        stream.start(batch_function, 0.01)
        stream.wait()

        source = SimpleDictSource("source", 13, 3)
        stream = Stream([source], stream_path, StreamOptions(repeat_failed_batch_exactly=True))
        stream.start(batch_function, 0.01)
        stream.wait()

        expected_second_and_third_batch_seen = Batches(
            batches={
                "source": Batch(
                    data=[
                        {"id": 2, "data": "source_data_2"},
                        {"id": 3, "data": "source_data_3"},
                    ],
                    read_to=3,
                ),
            },
        )
        assert batches_seen[1] == expected_second_and_third_batch_seen
        assert batches_seen[2] == expected_second_and_third_batch_seen


    def test_stream_doesnt_repeat_failed_batch_exactly_if_not_configured_to(
        self,
        tmp_path,
    ) -> None:

        stream_path = tmp_path / "stream"
        batch_function, batches_seen = get_flaky_function_and_batches_seen_with_repeats_list()

        source = SimpleDictSource("source", 13, 2)
        stream = Stream([source], stream_path)
        stream.start(batch_function, 0.01)
        stream.wait()

        source = SimpleDictSource("source", 13, 3)
        stream = Stream([source], stream_path, StreamOptions(repeat_failed_batch_exactly=False))
        stream.start(batch_function, 0.01)
        stream.wait()

        expected_second_batch_seen = Batches(
            batches={
                "source": Batch(
                    data=[
                        {"id": 2, "data": "source_data_2"},
                        {"id": 3, "data": "source_data_3"},
                    ],
                    read_to=3,
                ),
            },
        )
        expected_third_batch_seen = Batches(
            batches={
                "source": Batch(
                    data=[
                        {"id": 2, "data": "source_data_2"},
                        {"id": 3, "data": "source_data_3"},
                        {"id": 4, "data": "source_data_4"},
                    ],
                    read_to=4,
                ),
            },
        )
        assert batches_seen[1] == expected_second_batch_seen
        assert batches_seen[2] == expected_third_batch_seen
