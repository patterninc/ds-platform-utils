"""Typed exception hierarchy for remote-step.

All errors carry a `.retriable` attribute. Metaflow's `@retry` inspects this via
the decorator wrapper — non-retriable errors are re-raised as
`MetaflowInternalError` so `@retry` skips them.
"""



from __future__ import annotations

class RemoteStepError(Exception):
    """Base class for all remote-step errors."""

    retriable: bool = False

    def __init__(self, message: str, **kwargs):
        super().__init__(message)
        self.details = kwargs


class SizingError(RemoteStepError):
    """Placement resolver refused the ask. Not retriable."""

    retriable = False


class ConfigError(RemoteStepError):
    """Environment config missing or malformed. Not retriable."""

    retriable = False


class SubmitError(RemoteStepError):
    """AWS Batch SubmitJob failed after retries. Not retriable by default."""

    retriable = False


class PendingTimeoutError(RemoteStepError):
    """Batch job stuck in RUNNABLE beyond timeout. Retriable."""

    retriable = True


class KilledByUser(RemoteStepError):
    """User Ctrl-C'd the driver. Not retriable — intent = stop."""

    retriable = False


class RunnerError(RemoteStepError):
    """Container exited non-zero. Retriable if @retry configured."""

    retriable = True

    def __init__(self, message: str, exit_code: int, cw_stream: str = "", **kwargs):
        super().__init__(message, exit_code=exit_code, cw_stream=cw_stream, **kwargs)
        self.exit_code = exit_code
        self.cw_stream = cw_stream


class SpotInterruptionError(RunnerError):
    """EC2 spot instance terminated mid-job. Retriable."""

    retriable = True


class ManifestMissingError(RemoteStepError):
    """Batch reported SUCCESS but output-manifest.json not in S3. Retriable."""

    retriable = True


class ManifestReferencesMissingError(RemoteStepError):
    """Manifest present but referenced S3 blobs are gone. Retriable."""

    retriable = True


class ArtifactLoadError(RemoteStepError):
    """RemoteArtifact.load() failed (blob gone, sha mismatch, unpickle)."""

    retriable = False
