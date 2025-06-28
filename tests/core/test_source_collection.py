import pytest

from bream.core._definitions import Batch, BatchRequest
from bream.core._source_collection import SourceCollection
from tests.core.helpers import MockSource


class TestSourceCollection:
    @pytest.mark.parametrize(
        ("source_names",),
        [
            (("a_source", "a_source"),),
            (("a_source", "a_source", "a_source"),),
            (("a_source", "a_source", "another_source"),),
            (("another_source", "a_source", "a_source"),),
            (("a_source", "another_source", "a_source"),),
        ],
    )
    def test_cannot_be_instantiated_with_duplicate_named_sources(self, source_names):
        sources = [MockSource(source_name, 1) for source_name in source_names]
        with pytest.raises(
            ValueError,
            match="SourceCollection should not have multiple sources with the same name.",
        ):
            SourceCollection(sources)

    @pytest.mark.parametrize(
        ("batch_request",),
        [
            (BatchRequest(read_from_after=None, read_to=None),),
            (BatchRequest(read_from_after={"source1": 2, "source2": None}, read_to=None),),
            (BatchRequest(read_from_after={"source1": None, "source2": 10}, read_to=None),),
            (BatchRequest(read_from_after={"source1": 2, "source2": 10}, read_to=None),),
            (BatchRequest(read_from_after=None, read_to={"source1": 12, "source2": None}),),
            (
                BatchRequest(
                    read_from_after={"source1": 2, "source2": None},
                    read_to={"source1": None, "source2": 15},
                ),
            ),
            (
                BatchRequest(
                    read_from_after={"source1": None, "source2": 10},
                    read_to={"source1": 12, "source2": 15},
                ),
            ),
        ],
    )
    def test_correctly_defers_to_inner_sources(self, batch_request):
        advances_per_batch1, advances_per_batch2 = 3, 5
        source1 = MockSource("source1", advances_per_batch=advances_per_batch1)
        source2 = MockSource("source2", advances_per_batch=advances_per_batch2)
        source_container = SourceCollection([source1, source2])
        batch = source_container.read(batch_request)

        expected_data_all = {}
        expected_read_to_all = {}
        for source_name, advances_per_batch in (
            ("source1", advances_per_batch1),
            ("source2", advances_per_batch2),
        ):
            match (batch_request.read_from_after, batch_request.read_to):
                case (None, None):
                    request_read_from_after = None
                    request_read_to = None
                    expected_read_to = advances_per_batch - 1
                case (raf, None):
                    request_read_from_after = raf[source_name]
                    request_read_to = None
                    expected_read_to = (
                        -1 if raf[source_name] is None else raf[source_name]
                    ) + advances_per_batch
                case (None, rt):
                    request_read_from_after = None
                    request_read_to = rt[source_name]
                    expected_read_to = (
                        advances_per_batch - 1 if rt[source_name] is None else rt[source_name]
                    )
                case (raf, rt):
                    request_read_from_after = raf[source_name]
                    request_read_to = rt[source_name]
                    expected_read_to = (
                        rt[source_name]
                        if rt[source_name]
                        else (-1 if raf[source_name] is None else raf[source_name])
                        + advances_per_batch
                    )
            expected_data_all[source_name] = (
                "data_from: "
                f"BatchRequest(read_from_after={request_read_from_after}, "
                f"read_to={request_read_to})"
            )
            expected_read_to_all[source_name] = expected_read_to

        expected_batch = Batch(data=expected_data_all, read_to=expected_read_to_all)

        assert batch == expected_batch

    def test_returns_no_batch_if_all_sources_empty(self):
        advances_per_batch1, advances_per_batch2 = 3, 5
        source1 = MockSource("source1", advances_per_batch=advances_per_batch1, empty=True)
        source2 = MockSource("source2", advances_per_batch=advances_per_batch2, empty=True)
        source_container = SourceCollection([source1, source2])
        batch_request = BatchRequest(read_from_after={"source1": 2, "source2": None}, read_to=None)
        batch = source_container.read(batch_request)
        assert batch is None

    @pytest.mark.parametrize(
        ("batch_request",),
        [
            (BatchRequest(read_from_after=None, read_to=None),),
            (BatchRequest(read_from_after={"source1": 2, "source2": None}, read_to=None),),
            (BatchRequest(read_from_after={"source1": None, "source2": 10}, read_to=None),),
            (BatchRequest(read_from_after={"source1": 2, "source2": 10}, read_to=None),),
            (BatchRequest(read_from_after=None, read_to={"source1": None, "source2": 10}),),
            (
                BatchRequest(
                    read_from_after={"source1": 2, "source2": None},
                    read_to={"source1": None, "source2": 15},
                ),
            ),
        ],
    )
    def test_returns_batch_if_some_but_not_all_sources_empty(self, batch_request):
        advances_per_batch1, advances_per_batch2 = 3, 5
        source1 = MockSource("source1", advances_per_batch=advances_per_batch1, empty=True)
        source2 = MockSource("source2", advances_per_batch=advances_per_batch2)
        source_container = SourceCollection([source1, source2])
        batch = source_container.read(batch_request)

        expected_data_all = {}
        expected_read_to_all = {}
        match (batch_request.read_from_after, batch_request.read_to):
            case (None, None):
                request_read_from_after2 = None
                request_read_to2 = None
                expected_read_to1 = None
                expected_read_to2 = advances_per_batch2 - 1
            case (raf, None):
                request_read_from_after2 = raf["source2"]
                request_read_to2 = None
                expected_read_to1 = raf["source1"]
                expected_read_to2 = (
                    -1 if raf["source2"] is None else raf["source2"]
                ) + advances_per_batch2
            case (None, rt):
                request_read_from_after2 = None
                request_read_to2 = rt["source2"]
                expected_read_to1 = None
                expected_read_to2 = (
                    advances_per_batch2 - 1 if rt["source2"] is None else rt["source2"]
                )
            case (raf, rt):
                request_read_from_after2 = raf["source2"]
                request_read_to2 = rt["source2"]
                expected_read_to1 = raf["source1"]
                expected_read_to2 = (
                    rt["source2"]
                    if rt["source2"]
                    else (-1 if raf["source2"] is None else raf["source2"]) + advances_per_batch2
                )

        expected_data_all = {
            # source1 absent as it returned no batch
            "source2": (
                "data_from: "
                f"BatchRequest(read_from_after={request_read_from_after2}, "
                f"read_to={request_read_to2})"
            ),
        }

        expected_read_to_all = {"source1": expected_read_to1, "source2": expected_read_to2}

        expected_batch = Batch(data=expected_data_all, read_to=expected_read_to_all)

        assert batch == expected_batch
