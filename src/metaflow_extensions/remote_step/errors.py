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
    """The resource ask cannot be satisfied by any NodePool. Not retriable."""

    retriable = False


class ConfigError(RemoteStepError):
    """Environment config missing or malformed. Not retriable."""

    retriable = False


class SubmitError(RemoteStepError):
    """Creating the Kubernetes Job failed. Not retriable by default."""

    retriable = False


class PendingTimeoutError(RemoteStepError):
    """Pod never started within the timeout. Retriable.

    Two distinguishable causes, both reported in the message: the Workload
    was never admitted by Kueue (the team's ClusterQueue is at quota), or it
    was admitted but Karpenter could not provide a node (no capacity in any
    listed instance family, in any AZ the NodePool can reach).
    """

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


class NodeLostError(RunnerError):
    """The pod's node disappeared mid-step. Retriable.

    We run no spot capacity, so the causes are node expiry (Karpenter's
    `expireAfter: 24h`), consolidation racing a step, or instance failure.
    """

    retriable = True


class ManifestMissingError(RemoteStepError):
    """Pod exited 0 but output-manifest.json is not in S3. Retriable."""

    retriable = True


class ManifestReferencesMissingError(RemoteStepError):
    """Manifest present but referenced S3 blobs are gone. Retriable."""

    retriable = True


class ArtifactLoadError(RemoteStepError):
    """RemoteArtifact.load() failed (blob gone, sha mismatch, unpickle)."""

    retriable = False
