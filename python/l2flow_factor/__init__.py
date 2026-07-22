"""L2Flow Phase 7 repository-local factor runtime primitives.

The five named factors exported here are explicitly non-mathematical
``PASSTHROUGH_PLACEHOLDER`` implementations.
"""

from .canonical import (
    BatchMetadata,
    CANONICAL_DTYPE_SHA256,
    CANONICAL_DTYPE_SHA256_HEX,
    CANONICAL_SCHEMA_SHA256,
    CANONICAL_SCHEMA_SHA256_HEX,
    CANONICAL_SNAPSHOT_DTYPE,
    CANONICAL_TICK_DTYPE,
    ConsumerAttachSpec,
    InputFamily,
    MdlBatchView,
    canonical_record_dtype,
)
from .checkpoint import (
    AtomicCheckpointStore,
    FactorCheckpoint,
    decode_checkpoint,
    encode_checkpoint,
)
from .errors import *
from .native_mux import FailClosedNativeMux, NATIVE_MUX, NativeMuxAdapter
from .placeholders import (
    BookImbalancePassthroughPlaceholder,
    CancelRatePassthroughPlaceholder,
    MicropricePassthroughPlaceholder,
    PLACEHOLDER_FACTOR_IDS,
    PLACEHOLDER_FACTOR_TYPES,
    PassthroughPlaceholderFactor,
    PlaceholderResult,
    PlaceholderStatus,
    TradeImbalancePassthroughPlaceholder,
    TradeIntensityPassthroughPlaceholder,
)
from .runtime import (
    DecodedRuntimeCheckpoint,
    FactorTransactionRuntime,
    GenerationTag,
    InstrumentShardRouter,
    LOGICAL_FACTOR_SHARDS,
    RUNTIME_STATE_CODEC_V1,
    RunIdentity,
    RuntimeOutput,
    ShardRouter,
    TransactionRuntime,
    decode_runtime_checkpoint,
)
from .spec import (
    ClockSemantics,
    EpochPolicy,
    FactorSpec,
    GapPolicy,
    InputMode,
    InputSpec,
    OutputCadence,
    Warmup,
    WarmupSpec,
    WindowKind,
    WindowSpec,
)
from .watermark import (
    DurabilityBarrierResult,
    DurabilityFailure,
    FactorInputWatermark,
    FactorInputWatermarkSet,
    INPUT_IDENTITY_DOMAIN_V1,
    RawNamespace,
    WatermarkEntry,
    WatermarkSet,
    check_durability_barrier,
)
from .windows import (
    EventWindow,
    IncrementalEventWindow,
    IncrementalTimeWindow,
    TimeWindow,
)


__version__ = "0.1.0"
