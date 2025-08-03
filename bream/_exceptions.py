"""Exceptions."""


class BreamInternalError(Exception):
    """Base class for internal bream errors indicating bugs in bream itself.

    If you encounter these, please file a bug report.
    """


class SourceProtocolError(Exception):
    """Raised when a source doesn't meet expectations."""


class StreamLogicalError(Exception):
    """Raised when a stream is handled incorrectly."""


class StreamDefinitionCorruptionError(StreamLogicalError):
    """Raised when stream definition files are corrupted or malformed."""


class CheckpointDirectoryIntegrityError(Exception):
    """Raised when checkpoint directory integrity is compromised by external causes."""


class CheckpointDirectoryValidityError(CheckpointDirectoryIntegrityError):
    """Raised when a checkpoint directory is in an invalid state."""


class CheckpointFileCorruptionError(CheckpointDirectoryIntegrityError):
    """Raised when checkpoint files are corrupted or malformed."""


class CheckpointDirectoryInvalidOperationError(BreamInternalError):
    """Raised when an invalid operation is attempted on a checkpoint directory."""
