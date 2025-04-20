"""Exceptions."""


class SourceProtocolError(Exception):
    """Raised when a source doesn't meet expectations."""


class StreamLogicalError(Exception):
    """Raised when a stream is handled incorrectly."""
