from __future__ import annotations

from bream.core._definitions import Batch, BatchRequest, Source


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
