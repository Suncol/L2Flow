class L2FlowFactorError(Exception):
    """Base class for fail-closed Phase 7 runtime errors."""


class ValidationError(L2FlowFactorError, ValueError):
    """A public object failed its frozen V1 validation contract."""


class AttachError(L2FlowFactorError):
    """Canonical bytes cannot be attached under the expected identity."""


class BatchClosedError(L2FlowFactorError, RuntimeError):
    """A batch-backed view was accessed outside its context lifetime."""


class CursorError(L2FlowFactorError):
    """A batch cursor is duplicated inconsistently or is not contiguous."""


class ShardOwnershipError(L2FlowFactorError):
    """A runtime was asked to mutate an instrument owned by another shard."""


class PluginExecutionError(L2FlowFactorError):
    """A plugin transaction failed and its staged state was rolled back."""


class OutputConflictError(L2FlowFactorError):
    """The same idempotency key was reused with different content."""


class DurabilityBarrierError(L2FlowFactorError):
    """At least one exact Raw namespace has not durably covered its input."""


class CheckpointError(L2FlowFactorError):
    """A checkpoint failed validation, publication, or restoration."""


class NativeProofUnavailable(L2FlowFactorError):
    """A required native no-lookahead proof helper is unavailable."""
