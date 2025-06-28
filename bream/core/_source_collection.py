from __future__ import annotations

from typing import TYPE_CHECKING, Any

from bream.core._definitions import Batch, BatchRequest, JsonableNonNull, Source

if TYPE_CHECKING:
    from collections.abc import Sequence


class SourceCollection(Source):
    """A container for a group of sources so they can be used like a single source.

    Parameters
    ----------
    sources
        The sources of data batches that this container will wrap.

    """

    def __init__(self, sources: Sequence[Source]) -> None:
        if len(sources) != len({s.name for s in sources}):
            msg = f"{self.__class__.__name__} should not have multiple sources with the same name."
            raise ValueError(msg)
        self._sources = list(sources)
        self.name = ":".join(source.name for source in self._sources)

    def read(self, batch_request: BatchRequest) -> Batch | None:
        """Read data from the collection of sources according to the batch request.

        Parameters
        ----------
        batch_request
            A request for a batch of data, as prepared by bream.

        Returns
        -------
        Batch | None
            The batches of data if any of the sources provided one available, otherwise None.

        """
        br_read_from_after: dict[str, JsonableNonNull | None] = batch_request.read_from_after  # type: ignore[assignment]
        br_read_to: dict[str, JsonableNonNull | None] = batch_request.read_to  # type: ignore[assignment]

        sub_batches: dict[str, Batch | None] = {}
        data: dict[str, Any] = {}
        read_to: dict[str, JsonableNonNull | None] = {}
        for source in self._sources:
            source_read_from_after = br_read_from_after[source.name] if br_read_from_after else None
            source_read_to = br_read_to[source.name] if batch_request.read_to is not None else None
            if source_read_to is None or source_read_to != source_read_from_after:
                # TODO: decide if this is the right place for this if statement
                source_batch_request = BatchRequest(
                    read_from_after=(
                        br_read_from_after[source.name] if batch_request.read_from_after else None
                    ),
                    read_to=(
                        br_read_to[source.name] if batch_request.read_to is not None else None
                    ),
                )
                sub_batch = source.read(source_batch_request)
            else:
                sub_batch = None

            sub_batches[source.name] = sub_batch

        if all(v is None for v in sub_batches.values()):
            return None

        for source_name, sub_batch in sub_batches.items():
            if sub_batch is not None:
                data[source_name] = sub_batch.data
            read_to[source_name] = (
                sub_batch.read_to
                if sub_batch is not None
                else (br_read_from_after[source_name] if br_read_from_after is not None else None)
            )

        return Batch(data=data, read_to=read_to)
