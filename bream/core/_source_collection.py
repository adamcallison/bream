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

    def _get_source_boundary(
        self,
        boundaries: dict[str, JsonableNonNull | None] | None,
        source_name: str,
    ) -> JsonableNonNull | None:
        """Safely extract a boundary value for a source from a potentially None dictionary."""
        return boundaries[source_name] if boundaries is not None else None

    def _should_read_source(
        self,
        source_read_from_after: JsonableNonNull | None,
        source_read_to: JsonableNonNull | None,
    ) -> bool:
        """Determine if a source should be read based on its boundaries.

        Returns False for replay scenarios where we know the source had no data
        in the requested range (i.e., when read_to equals read_from_after).
        This optimization avoids redundant source calls during batch replay.
        """
        return source_read_to is None or source_read_to != source_read_from_after

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
        br_read_from_after: dict[str, JsonableNonNull | None] | None = batch_request.read_from_after  # type: ignore[assignment]
        br_read_to: dict[str, JsonableNonNull | None] | None = batch_request.read_to  # type: ignore[assignment]

        sub_batches: dict[str, Batch | None] = {}
        data: dict[str, Any] = {}
        read_to: dict[str, JsonableNonNull | None] = {}
        for source in self._sources:
            source_read_from_after = self._get_source_boundary(br_read_from_after, source.name)
            source_read_to = self._get_source_boundary(br_read_to, source.name)

            if self._should_read_source(source_read_from_after, source_read_to):
                source_batch_request = BatchRequest(
                    read_from_after=source_read_from_after,
                    read_to=source_read_to,
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
                read_to[source_name] = sub_batch.read_to
            else:
                read_to[source_name] = self._get_source_boundary(br_read_from_after, source_name)

        return Batch(data=data, read_to=read_to)
