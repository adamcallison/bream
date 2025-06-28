"""Exceptions."""


class SourceProtocolError(Exception):
    """Raised when a source doesn't meet expectations."""


class StreamLogicalError(Exception):
    """Raised when a stream is handled incorrectly."""


class CheckpointDirectoryError(Exception):
    """Raised for errors involving a checkpoint directory."""


class CheckpointDirectoryValidityError(CheckpointDirectoryError):
    """Raised when a checkpoint directory is in an invalid state."""


class CheckpointDirectoryInvalidOperationError(CheckpointDirectoryError):
    """Raised when an invalid operation is attempted on a checkpoint directory."""
