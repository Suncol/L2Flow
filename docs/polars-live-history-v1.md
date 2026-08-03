# Python Polars Live History V1 契约

## 1. 范围与状态

本文描述当前 Python 包 `l2flow_realtime` 已实现的 Polars 接口及其一致性契约。这里的 “live history” 指后台维护的、可反复取得最新不可变快照的 `DataFrame`/`LazyFrame`；它不表示一个已返回的 Polars 对象会原地变化。

当前契约覆盖四个互相独立的数据产品：

- 单标的 FAST raw Tick History；
- 单标的 FAST-derived Event History；
- 全局 CERTIFIED Event history-to-tail；
- 全局 CERTIFIED Tick append-only history-to-tail。

另外，`PolarsClient` 也提供 latest Snapshot、latest Tick、latest KLine 的直接 DataFrame 适配。这些 point-read API 不是历史缓存，不能据此声明完整日覆盖。

本文不承诺以下尚未实现的能力：

- 已返回的 `DataFrame` 或 `LazyFrame` 自动变成新一代数据；
- 从可覆盖的 native FAST ring 直接零拷贝持有 slot；
- Python cache 的自动磁盘分层、内存行淘汰或滚动时间窗淘汰；
- 通过 Parquet spill 扩展 native journal/ring 容量；
- spill 文件或目录的 `fsync` crash durability；
- Polars 层自动发现 online-recovery socket；
- 跨交易日拼接，或把 process-start partial 与 from-open 数据简单追加成同一数据集；
- 固定的吞吐量、p99 延迟或 CPU 亲和性 SLO。

Polars 是可选依赖：

```bash
pip install "l2flow-realtime[polars]"
```

当前 optional dependency 约束为 `polars>=1.32,<2`，Python 要求为 3.10 或更高版本。仅导入 `l2flow_realtime` 不会导入 Polars；调用 `as_polars()`、导入 `l2flow_realtime.polars` 中需要 Polars 的对象，或调用 frame/schema helper 时才会加载它。可用 `PolarsClient.warmup()` 提前完成导入和空 schema frame 构造。

## 2. 入口与产品/API 矩阵

公共入口是：

```python
import polars as pl

from l2flow_realtime import as_polars, connect

fast = connect("/run/l2flow/realtime.sock")
frames = as_polars(fast)
```

`as_polars(fast)` 返回 `l2flow_realtime.polars.PolarsClient`。它不复制或接管传入的 `L2FlowClient`；调用方仍负责关闭原始 client。

### 2.1 历史产品

| 产品 | 范围 | `PolarsClient` API | 默认返回类型 | 可选 live 返回类型 | coverage |
|---|---:|---|---|---|---|
| FAST raw Tick | 单标的 | `open_instrument_tick_history()` | `PolarsInstrumentTickHistory` | `PolarsFastTickHistory`，需 `live_tail=True` | from-open；partial 需显式 opt-in |
| FAST-derived Event | 单标的 | `open_instrument_derived_event_history()` | `PolarsInstrumentDerivedEventHistory` | `PolarsFastDerivedEventHistory`，需 `live_tail=True` 和 `event_control_socket_path` | from-open；partial 需显式 opt-in |
| CERTIFIED Event | 全局 | `open_certified_order_event_history()` | `PolarsCertifiedOrderEventHistory` | 同一 handle 已包含 history-to-tail | 仅 from-open，必须从 event sequence 1 开始 |
| CERTIFIED Tick | 全局 | `open_certified_tick_history()` | `PolarsCertifiedTickHistory` | 同一 handle 已包含 history-to-tail | 仅 from-open，必须从 canonical apply sequence 1 开始 |

四种产品有不同的 continuity domain，不能混用它们的 sequence：

- FAST raw 使用全局 `tick_stream_sequence` 与 EOF-verified raw checkpoint；
- FAST-derived History 的 `derived_event_sequence` 是单标的 History 产品内序号；
- FAST live Event ring 有自己的全局 Event cursor；
- CERTIFIED Event 使用自己的 dense Event sequence；
- CERTIFIED Tick 使用 dense `canonical_apply_sequence`。

只有明确的 reconciliation contract 才能跨产品对账。尤其不能把 FAST-derived History 的 `derived_event_sequence` 与 FAST live Event cursor 当成同一序列。

### 2.2 point read 与 batch adapter

| 功能 | DataFrame API | LazyFrame API | 说明 |
|---|---|---|---|
| latest Snapshot | `latest_snapshot()` / `latest_snapshots()` | `latest_snapshot_lazy()` / `latest_snapshots_lazy()` | 一次 Wire V2 point-read cut |
| latest Tick | `latest_tick()` / `latest_ticks()` | `latest_tick_lazy()` / `latest_ticks_lazy()` | 一次 Wire V2 point-read cut |
| latest KLine | `latest_kline()` / `latest_klines()` | `latest_kline_lazy()` / `latest_klines_lazy()` | 可附带 session KLine coverage |
| raw History batch | `raw_event_batch()` | `raw_event_batch_lazy()` | 复制 leased batch 的选定列 |
| derived batch | `derived_event_batch()` | `derived_event_batch_lazy()` | 固定 derived Event schema |
| live Event batch | `live_order_event_batch()` | `live_order_event_batch_lazy()` | 固定 derived Event schema |
| CERTIFIED Event batch | `certified_order_event_batch()` | `certified_order_event_batch_lazy()` | 增加 `canonical_apply_sequence` |
| CERTIFIED Tick batch | `certified_tick_batch()` | `certified_tick_batch_lazy()` | 可选 `wire_payload` |

这些 `*_lazy()` 接口先取得并拥有当前数据，再在该 cut 上调用 `.lazy()`；它们不是延迟到 `collect()` 时才重新读取 IPC。

## 3. 固定 schema 契约

当前 `POLARS_SCHEMA_VERSION` 为 1。schema 的列名、顺序和 dtype 都属于数据集身份的一部分。不能只因两张表“列数相同”就认为它们可 promotion 或追加。

权威 helper 为：

```python
from l2flow_realtime.polars import (
    certified_tick_schema,
    derived_event_schema,
    raw_event_schema,
)
```

### 3.1 FAST raw Tick History

raw schema 由 `columns=` 的有序 projection 固定。默认 projection 是：

| 列 | dtype |
|---|---|
| `ingress_sequence` | `UInt64` |
| `tick_stream_sequence` | `UInt64` |
| `price_p6` | `Int64` |
| `price_valid` | `UInt8` |
| `price_is_null` | `UInt8` |

注意 raw History 的 validity/null flags 保持 wire/worker 的整数编码，是 `UInt8`，不是 Polars `Boolean`。

可选 raw 列及类型映射如下；`raw_event_schema(columns)` 会拒绝未知列、重复列和空 projection。

| dtype | 可选列 |
|---|---|
| `UInt64` | `source_sequence`, `ingress_sequence`, `tick_stream_sequence`, `vendor_sequence_id`, `exchange_time_ns_since_midnight`, `quality_flags`, `market_notices` |
| `UInt32` | `record_schema_version`, `record_bytes`, `instrument_id`, `ordinal`, `source_stream_id`, `trade_date`, `vendor_local_time_raw`, `validity_bitmap`, `projection_flags` |
| `UInt8` | `source_slot`, `event_kind`, `market`, `quantity_unit`, `security_type`, `asset_scope`, `action`, `side`, `order_type`, `aggressor`, `phase`, `price_scale`, `price_valid`, `price_is_null`, `quantity_scale`, `quantity_valid`, `quantity_is_null`, `trade_amount_scale`, `trade_amount_valid`, `trade_amount_is_null`, `matched_quantity_scale`, `matched_quantity_valid`, `matched_quantity_is_null` |
| `Int64` | `event_time_unix_ns`, `recv_realtime_ns`, `recv_monotonic_ns`, `channel`, `native_event_sequence`, `primary_order_id`, `buy_order_id`, `sell_order_id`, `price_raw`, `price_p6`, `quantity_raw`, `trade_amount_raw`, `trade_amount_p6`, `matched_quantity_raw` |
| `Int32` | `source_raw_code_1`, `source_raw_code_2` |

同一 handle 的 projection 不会在 refresh 中改变。ordered projection 也进入 `PolarsHistoryDatasetIdentity.projection_columns`。

### 3.2 FAST-derived Event

`derived_event_schema()` 的列顺序固定。按 dtype 分组如下：

| dtype | 列 |
|---|---|
| `UInt64` | `derived_event_sequence`, `revision`, `trade_count`, `cancel_count`, `quality_flags`, `source_quality_flags`, `source_market_notices`, `source_sequence`, `ingress_sequence`, `tick_stream_sequence`, `vendor_sequence_id`, `event_time_ns_since_midnight`, `vendor_local_time_ns_since_midnight` |
| `UInt32` | `trade_date`, `instrument_id`, `vendor_local_time_raw`, `source_tick_event_ordinal` |
| `UInt8` | `market`, `event_kind`, `operation`, `finality`, `side`, `side_source`, `aggressor`, `phase`, `phase_at_first`, `phase_at_add`, `phase_at_last`, `order_type`, `order_source`, `price_source`, `original_quantity_status` |
| `Int64` | `channel`, `order_id`, `buy_order_id`, `sell_order_id`, `price_p6`, `execution_boundary_price_p6`, `quantity`, `trade_amount_p6`, `published_quantity`, `original_quantity`, `remaining_quantity`, `source_matched_quantity`, `observed_pre_add_trade_quantity`, `post_add_trade_quantity`, `total_trade_quantity`, `total_cancel_quantity`, `native_event_sequence`, `event_time_unix_ns`, `recv_realtime_ns`, `recv_monotonic_ns` |
| `Boolean` | `price_valid`, `execution_boundary_price_valid`, `trade_amount_valid`, `published_quantity_valid`, `original_quantity_valid`, `remaining_quantity_valid`, `source_matched_quantity_valid`, `add_seen`, `apply_to_book`, `referenced_order_found`, `side_from_order`, `event_time_valid`, `event_time_unix_ns_valid`, `vendor_local_time_valid` |
| `Binary` | `event_uid` |

`source_tick_event_ordinal` 和 `event_uid` 对 source-backed row 成对存在。source-free trading-day finalization row 可以同时为 null。不能用 product-local `derived_event_sequence` 替代缺失的 UID。

### 3.3 CERTIFIED Event

`derived_event_schema(certified=True)` 在上述 derived schema 最前面增加：

| 列 | dtype |
|---|---|
| `canonical_apply_sequence` | `UInt64` |

CERTIFIED Event 是全局产品；它不接受单标的参数。需要单标的结果时，应对 pinned DataFrame/LazyFrame 使用 Polars filter。

### 3.4 CERTIFIED Tick

`certified_tick_schema(include_wire_payload=False)` 的固定列为：

| dtype | 列 |
|---|---|
| `UInt64` | `canonical_apply_sequence`, `correction_epoch`, `feed_epoch`, `certified_monotonic_ns`, `source_sequence`, `ingress_sequence`, `tick_stream_sequence`, `vendor_sequence_id`, `exchange_time_ns_since_midnight`, `quality_flags`, `market_notices` |
| `UInt32` | `instrument_id`, `record_schema_version`, `record_bytes`, `ordinal`, `source_stream_id`, `trade_date`, `vendor_local_time_raw`, `projection_flags` |
| `UInt8` | `source_slot`, `event_kind`, `market`, `quantity_unit`, `security_type`, `asset_scope`, `price_scale`, `quantity_scale`, `action`, `side` |
| `Int64` | `event_time_unix_ns`, `recv_realtime_ns`, `recv_monotonic_ns`, `price_raw`, `price_p6`, `quantity_raw` |
| `Boolean` | `price_valid`, `price_is_null`, `quantity_valid`, `quantity_is_null` |

`include_wire_payload=True` 会在末尾增加 `wire_payload: Binary`。这个选择会改变 fixed schema 和 `dataset_identity`，因此带 wire payload 与不带 wire payload 的历史不能互相 promotion。

### 3.5 dataset identity

`PolarsHistoryDatasetIdentity` 至少约束：

- `product_kind`；
- ordered `projection_columns`；
- `payload_projection`；
- `polars_schema_version`。

FAST 单标的产品还约束 `instrument_id` 和非零 `catalog_digest`；FAST-derived Event 还约束 `market`。由 `PolarsClient` 创建的 CERTIFIED identity 会携带 FAST session 的 catalog digest、scope 和 version，用于 promotion 与 session 一致性校验。

run/session identity 和 temporal origin 不直接构成逻辑 dataset identity，因为合法的 online promotion 会用另一个 recovered session 替换 partial session；但 promotion 仍单独校验 trade date、coverage、catalog identity 和 fixed schema。

## 4. DataFrame、LazyFrame 与 pinned snapshot

所有 history handle 都提供以下核心调用：

```python
history.wait_ready(timeout=5.0)
snapshot = history.latest_snapshot()
df = snapshot.dataframe
lf = snapshot.lazyframe
```

语义是：

1. `latest_snapshot()` 原子地捕获一个已提交 manifest/generation。
2. `snapshot.dataframe` 返回 shallow clone；它共享不可变列 buffer，但不会把 cache 内部持有的 DataFrame 对象直接交给调用方。
3. `snapshot.lazyframe` 在已经捕获的 DataFrame 上创建 LazyFrame。
4. 后台提交新 generation 后，旧 `snapshot`、`df` 和 `lf` 仍指向旧 cut；以后执行 `lf.collect()` 不会混入新 generation。
5. 要取得新数据，必须再次调用 `latest_snapshot()`、`latest_dataframe()` 或 `latest_lazyframe()`。

因此“Polars 无感静默更新”的准确含义是：后台更新不要求调用方手工 append；下一次取 latest 时会看到最新已提交 cut。它不意味着先前返回的对象原地更新。

`latest_dataframe()` 等价于先捕获 latest snapshot 再取 `.dataframe`；`latest_lazyframe()` 同理。调用方若需要在多次操作间保持完全相同的 cut，应保存一个 snapshot，并从该 snapshot 同时取得 DataFrame、LazyFrame、coverage、checkpoint/token 和 dataset identity。

FAST/CERTIFIED generation-polled handle 支持 `consistency="cached"` 和部分产品的 `consistency="latest_published"`：

- `cached` 返回当前已经提交的 cut；
- `latest_published` 会触发/等待一次对应 reader 的 refresh 或 published-prefix drain；
- 返回后生产者仍可能继续发布新数据，所以它不是“停止世界后的最终值”。

FAST live-tail handle 的 `latest_snapshot()` 当前只接受 `allow_stale`，需要主动等待 durable refresh 时调用 handle 的 `refresh(timeout)`。

## 5. `LivePolarsHistory`：immutable chunks 与 atomic manifest

`LivePolarsHistory` 是 FAST live-tail、CERTIFIED Event 和 CERTIFIED Tick cache 使用的公共 manifest primitive。其实现契约为：

- 输入 chunk 必须是完全匹配 fixed schema 的 Polars DataFrame；
- producer 在 publication lock 外完成 owned chunk 构造、concat 和可选 rechunk；
- `publish_full()` 只允许在空 manifest 上提交完整初始 cut；
- `publish_delta()` 必须提供与当前 commit 完全相等的 `expected_base_token`；
- row count 必须等于外部 verified boundary 声明的总行数；
- generation 不能倒退；
- append 不能改变 dataset identity；
- 最后只在锁内执行单个 commit pointer replacement。

reader 因此只会看到旧 commit 或新 commit，不会看到一半 chunk、一半 checkpoint 的中间态。`LivePolarsHistorySnapshot` 保存 frame、chunk manifest、coverage、generation、continuity token、metadata、source、version、commit monotonic time 和 dataset identity。

raw generation-only `PolarsInstrumentTickHistory` 使用专用 committed-history 结构，但对外同样遵守“EOF verified 后才原子替换完整 generation”的可观察语义。

所有从可覆盖 ring、leased worker batch 或 native reader 取得的数据，在离开相应 lease/slot 前都会转成 owned Polars 列。尤其 FAST ring slot 会被覆盖，不能安全地让 DataFrame 长期引用该 slot。

## 6. FAST raw：full+delta 与可选 live tail

### 6.1 generation-only 路径

默认调用：

```python
history = frames.open_instrument_tick_history(
    instrument_id,
    columns=("tick_stream_sequence", "price_p6"),
)
```

返回 `PolarsInstrumentTickHistory`。后台 reader 先读取单标的完整 generation，消费到显式 EOF，再取得 verified `InstrumentTickDeltaCheckpoint`；只有 row count、session、trade date 和 coverage 都吻合后才发布。后续使用同一 checkpoint 读取 updates。

这个 raw 产品包含 Tick source slot 1/3 的单标的记录，不是 snapshot 历史，也不是 derived order/event 历史。

`live_tail=False` 时不会创建 FAST live ring poller，也不会创建独立 FAST read client。默认 `refresh_interval=1.0`；传 `None` 可关闭周期 refresh，之后由 `refresh()` 驱动。

### 6.2 可选低延迟 tail

```python
history = frames.open_instrument_tick_history(
    instrument_id,
    live_tail=True,
    tail_poll_interval=0.001,
    refresh_interval=1.0,
)
```

返回 `PolarsFastTickHistory`。immutable History generation 仍是完整性权威，bounded FAST ring 只是低延迟加速层：

- 初始 handoff 使用 History checkpoint 的 `tick_stream_sequence_exclusive`；
- live reader 连续扫描全局 FAST Tick ring，再筛选目标 instrument；
- live slot 先复制为 owned bytes/columns 后才发布；
- durable generation 前进时，History 覆盖的 live suffix 会在 shadow manifest 中被 authoritative History 替换；
- ring overrun 不会被忽略；handle 等待 History checkpoint 足以桥接缺口后重新打开 ring；
- repair 后通过 atomic promotion 替换完整 manifest。

通过 `PolarsClient` 打开 raw live tail 时，实现会为 poller 创建另一个 `L2FlowClient` mapping/lock domain，并在连接前后校验相同 session identity 与 trade date。这样持续 ring polling 不会因为复用原始 client 的 Python/native reader lock 而串行化调用方的 point read。该独立 client 是额外的 mapping、fd 和资源；由 `PolarsFastTickHistory.close()` 关闭。

这要求原始 `L2FlowClient` 是通过 Wire V2 control socket 打开的，并且 native library 可用于创建另一份 reader。direct-fd client 没有可复用 control socket，不能通过这个 facade 自动创建独立 raw tail client。

`batch_records` 同时控制 History worker batch 和 FAST tail read batch；`ring_slots` 控制 raw History worker 的结果 ring slot 数，不会改变生产者 FAST ring 的 native 容量。

## 7. FAST-derived Event：full+updates 与稳定 UID live tail

### 7.1 generation-only 路径

```python
events = frames.open_instrument_derived_event_history(
    instrument_id,
    maximum_order_states=1_000_000,
    page_records=4096,
)
```

返回 `PolarsInstrumentDerivedEventHistory`。native derived reader 对一个 instrument 保持 order-state machine；初始 full 和每次 updates 都必须读到显式 EOF，随后才发布 `InstrumentDerivedEventCheckpoint` 对应的完整 cut。

### 7.2 可选 FAST live Event tail

```python
events = frames.open_instrument_derived_event_history(
    instrument_id,
    live_tail=True,
    event_control_socket_path="/run/l2flow/event-delta.sock",
    tail_batch_records=4096,
    tail_poll_interval=0.001,
    refresh_interval=1.0,
)
```

返回 `PolarsFastDerivedEventHistory`。`event_control_socket_path` 是必需的显式参数；该 API 不自动猜测 Event aggregator socket。

FAST-derived History 与 global live Event ring 的 dense event sequence 属于不同产品，不能互相比较。reconciliation 只使用：

1. derived checkpoint 内 raw checkpoint 的 `tick_stream_sequence_exclusive`；
2. schema-2 stable `EventUid`。

live Event ring 的每个 source-backed row 必须提供 UID。cache 扫描 global Event ring，验证 source session/day 和 source-tick 单调性，再筛选目标 instrument。由于扫描的是全局 ring，Python 端 validation/materialization 工作量取决于全局 live Event batch，而不只取决于单标的输出行数。

当 durable History 前进时：

- `tick_stream_sequence < new_tick_stream_sequence_exclusive` 的 live row 属于 History 覆盖区；
- 每个被覆盖的 live UID 都必须在 durable History 中存在；
- retained tail 的 UID 不能与 durable UID 重复；
- 校验通过后才原子替换 shadow manifest；
- 任一 overlap UID 缺失、重复或坐标不一致都会 fail-closed。

overrun repair 必须检查当前最老 retained Event 的 source tick。只有满足：

```text
raw_checkpoint.tick_stream_sequence_exclusive > oldest_retained_tick
```

才能重新开放 tail。严格大于而不是大于等于，是因为最老 retained slot 可能位于该 source tick 的中间；exclusive frontier 等于该 tick 时并未证明这一整 tick 已由 History 覆盖。

source-free trading-day finalization row 在历史 schema 中可以没有 UID。若当前 durable History checkpoint 尚未证明该标的已经 finalized，FAST live Event tail 会因缺少跨路径身份而 fail-closed；若 checkpoint 已经证明 finalized，则该 UID-less live FINALIZE 只被视为 History 中权威 finalization 的重复副本并跳过，不进入 tail manifest。

与 raw live tail 不同，derived live tail 读取独立 Event ring handle；它不创建另一份 FAST Wire V2 shared-memory mapping。原始 client 仅用于显式 Event control attach/reconnect，不用于每批 event slot 的 point-read polling。

## 8. `EventUid` 契约

`EventUid` 当前只有 `EventUidScope.SESSION_SOURCE_TICK`。`bytes(uid)` 是固定 48-byte network-order 结构编码，不是 hash，坐标包括：

- source FAST `SessionIdentity`；
- `instrument_id`；
- 非零 `tick_stream_sequence`；
- source tick 内 deterministic emission order 的零基 `source_tick_event_ordinal`。

同一 source tick 可能产生多个 order/trade/cancel/revision row，所以仅用 tick sequence 不足以标识一个 Event。反过来，product-local `derived_event_sequence` 也不能作为 History/live 跨路径 UID。

调用方若要在 FAST live row 被 authoritative History row 替换前后保持逻辑事件身份，应使用 `event_uid`。替换后该 row 的 product-local `derived_event_sequence` 可以变成 History 产品中的 authoritative 值；这不表示 UID 对应的事件改变。

## 9. CERTIFIED Event history-to-tail

```python
cert_events = frames.open_certified_order_event_history(
    "/run/l2flow/certified.sock",
    batch_records=4096,
    poll_interval=0.001,
)
```

该接口返回全局 `PolarsCertifiedOrderEventHistory`，并强制：

- coverage 必须是 from-open；
- native reader 从 Event sequence 1 开始；
- 每批 next sequence 与 row count 对齐；
- `derived_event_sequence` 在该 CERTIFIED cursor 中 dense；
- 初始 chunk 在 reader 追到一个 coherent advertised published prefix 前保持不可见；
- 后续 append 以 immutable manifest delta 原子发布。

`latest_snapshot(consistency="cached")` 返回已提交 prefix；`consistency="latest_published"` 会读取并等待追到该次读取观测到的 advertised frontier。snapshot 的 `caught_up`、`next_event_sequence` 和 `status` 可用于审计该 cut。

## 10. CERTIFIED Tick append-only history-to-tail

```python
cert_ticks = frames.open_certified_tick_history(
    "/run/l2flow/certified.sock",
    batch_records=4096,
    include_wire_payload=False,
    poll_interval=0.001,
)
```

返回全局 `PolarsCertifiedTickHistory`。它读取独立 append-only CERTIFIED Tick journal，不读取 bounded FAST ring，并强制：

- journal V1.1 header/C session 自描述且只接受 from-open coverage，Python 还会与同 session FAST coverage 交叉校验；
- reader 从 canonical apply sequence 1 开始；
- `canonical_apply_sequence` dense；
- batch 不能越过 status 的 coherent `canonical_apply_frontier`；
- 初始 prefix 追到 coherent frontier 前不发布；
- tail 以 atomic manifest delta 发布。

`latest_snapshot(consistency="latest_published")` 会等待追到该次读取观测的 canonical frontier。snapshot 提供 `status`、`next_canonical_apply_sequence`、`caught_up` 和 dataset identity。

native CERTIFIED Tick journal 是固定容量的 full-day append-only journal，不是覆盖旧 slot 的 rolling cache。独立绑核 History writer 从已经公开的 bounded CERTIFIED ring 批量追平；它的 backing allocation 和 512-byte journal stores 不位于 bounded CERTIFIED 发布临界路径。writer retention loss、journal capacity 或 I/O failure 只 fail-close Tick History，并保留 last-good prefix；FAST 与 bounded CERTIFIED 继续运行。Polars `spill()` 不会增加 native journal 容量。

## 11. temporal coverage

History coverage 使用独立的 `HistoryCoverageInfo`，字段为：

- `run_id`；
- `session_epoch`；
- `trade_date`；
- `coverage_kind`；
- 可选 `coverage_start_unix_ns`。

它与 KLine window coverage 是两个不同契约，不能用 KLine boundary 替代 History boundary。

支持的 `TemporalCoverageKind` 为：

| kind | 含义 |
|---|---|
| `UNAVAILABLE` | 当前 session 不暴露可读 History |
| `FROM_OPEN` | 当前数据集声明从当日开盘覆盖 |
| `PROCESS_START_PARTIAL` | 只声明从当前进程启动后覆盖 |

partial 的 `coverage_start_unix_ns` 只有在对应 ABI 能给出精确边界时才存在；类型允许为 `None`，不能自行用首次读到的 row 时间冒充 process-start boundary。

FAST raw/derived API 默认 `coverage_requirement="from_open"`。要读取 partial，必须明确写出：

```python
coverage_requirement="allow_process_start_partial"
```

该选项只是允许返回 process-start partial 数据，不会把它提升成完整日数据。调用方应同时保存 snapshot 的 `history_coverage`。

CERTIFIED Event 和 CERTIFIED Tick Polars history 当前都硬性要求 from-open，不能用 partial opt-in 绕过。

## 12. fail-closed 与 stale read

以下情况会拒绝 publication 或使 handle 进入 `PolarsHistoryState.FAILED`：

- fixed schema、dtype 或 ordered projection 不一致；
- EOF row count 与 checkpoint/frontier 不一致；
- generation、continuity token 或 dense cursor 倒退/断裂；
- run/session、trade date、coverage 或 catalog identity 改变；
- FAST ring overrun 不能被 durable History 证明桥接；
- FAST Event overlap 缺 UID、重复 UID 或 UID 坐标不一致；
- CERTIFIED batch 超过 coherent frontier；
- producer/journal 报告 terminal failure；
- `maximum_rows` 或 `maximum_chunks` 被完整数据集超过；
- promotion candidate 的 product/schema/identity/date/coverage 不匹配。

默认 `latest_snapshot()` 在 FAILED 状态抛出 `PolarsHistoryRefreshError`。若已经存在 last-good commit，可显式使用 `allow_stale=True` 读取该旧 cut；调用方必须把它视为 stale，不能把它描述为最新数据。初始 commit 尚未成功时没有 stale snapshot 可读。

fatal validation failure 不会静默跳过坏行或自动降低 coverage。通常需要修复数据源/配置并重新创建 handle。

## 13. compaction、limits 与 spill

所有 live manifest handle 接受：

- `maximum_rows=None`；
- `maximum_chunks=None`；
- `compact_after_chunks=256`。

当 manifest chunk 数严格超过 `compact_after_chunks` 时，cache 对完整 frame 执行 `rechunk()`，并把 manifest 收敛成一个 immutable chunk。compaction：

- 不删除任何历史行；
- 不改变 coverage；
- 不改变 native ring/journal；
- 可能在构造 shadow frame 时暂时同时占用旧、新 buffer；
- pinned old snapshot 会继续保留旧 buffer，直到调用方释放引用。

两个 FAST live reconciler 还会按同一阈值收敛其内部保留的 durable
History prefix；CERTIFIED reader 在提交初始 prefix 后也会释放临时
batch 累积引用。否则即使公开 manifest 已 rechunk，后台线程或下一次
reconciliation 使用的私有引用仍可能让旧 buffer 常驻。live tail 为了
gap/overlap 对账而保留的坐标或 wire suffix，则只会在对应 durable
frontier 已证明覆盖后移除。

`maximum_rows` 和 `maximum_chunks` 是 fail-closed resource guards，不是 retention policy。超限时 last-good commit 保留，新增数据不会通过淘汰旧行来“勉强成功”。

`spill(path, ...)` 把一个 pinned snapshot 写到同目录临时 Parquet 文件，再用 `os.replace()` 原子替换目标路径。它：

- 在调用线程执行磁盘 I/O；
- 不从内存 manifest 移除数据；
- 不释放 native journal/ring capacity；
- 不调用文件或父目录 `fsync`，所以不是断电 crash-durability 保证；
- 支持通过 expected token/checkpoint 防止调用方意外 spill 另一个 cut，具体参数取决于 handle。

因此当前 “spill” 应理解为 atomic Parquet snapshot export，而不是自动冷热分层。

## 14. online promotion、identity 与显式 factory/socket

高层 API 为：

```python
switching = frames.auto_promoting_history(
    active_history,
    promotion_factory,
    poll_interval=0.05,
    expected_dataset_identity=None,
)
```

返回 `AutoPromotingPolarsHistory`，并立即启动其 monitor thread。公开参数名是 `promotion_factory`；当前没有名为 `socket_factory` 的公开 Polars 参数，也不会自动搜索恢复后的 socket。

`promotion_factory` 是无参数 callable。它应：

- 在 recovered control plane 尚不可用时返回 `None`，或抛出 `UnavailableError`；
- 可用时，从调用方明确选择的 recovered control socket 构造并返回一个 history handle；
- 对其他异常不做伪装；其他异常会使 promotion wrapper fail-closed。

candidate 被公开前必须：

- `wait_ready()` 成功；
- coverage 是 from-open；
- 与 preview 是同一 trade date；
- fixed DataFrame schema 完全相同；
- stable `PolarsHistoryDatasetIdentity` 完全相同；
- 跨 session 时具备可比较的 catalog identity。

partial preview 与 recovered candidate 是“整表替换”，不是 concat。pointer swap 之前 candidate 在 shadow 中完整构造；swap 之后新调用看到 candidate，之前取得的 snapshot/LazyFrame 仍 pinned 到 preview buffers。

若 `active_history=None`，构造 wrapper 时必须显式提供 `expected_dataset_identity`，否则没有安全依据判断 factory 返回的是不是目标数据集。

`AutoPromotingPolarsHistory.close()` 关闭它管理的 history handle，但不会替调用方关闭在 factory 中自行创建的顶层 `L2FlowClient`。这些 client 需要应用层单独管理。

底层 `LivePolarsHistory.promote()` 是另一层 primitive：调用方必须提供返回字面值 `True` 的 `continuity_check`；跨 session 的 partial-to-from-open replacement 还必须显式设置 `allow_session_replacement=True`。一般用户应优先使用 `auto_promoting_history()`。

## 15. 典型用法

### 15.1 from-open raw History 与 pinned LazyFrame

```python
from l2flow_realtime import as_polars, connect

fast = connect("/run/l2flow/realtime.sock")
history = None
try:
    frames = as_polars(fast)
    frames.warmup()
    history = frames.open_instrument_tick_history(
        600000,
        columns=(
            "tick_stream_sequence",
            "event_time_unix_ns",
            "price_p6",
            "quantity_raw",
        ),
        coverage_requirement="from_open",
        refresh_interval=1.0,
    )
    history.wait_ready(timeout=10.0)

    cut = history.latest_snapshot()
    frame = cut.dataframe
    lazy = cut.lazyframe.filter(
        # A LazyFrame created from this cut remains pinned to this cut.
        pl.col("price_p6") > 0
    )

    print(cut.history_coverage)
    print(frame.tail())
    print(lazy.collect().tail())
finally:
    if history is not None:
        history.close()
    fast.close()
```

### 15.2 partial raw FAST live tail

```python
history = frames.open_instrument_tick_history(
    600000,
    coverage_requirement="allow_process_start_partial",
    live_tail=True,
    tail_poll_interval=0.001,
    refresh_interval=1.0,
    maximum_rows=20_000_000,
)
history.wait_ready(timeout=10.0)

cut = history.latest_snapshot()
assert cut.history_coverage.process_start_partial
df = cut.dataframe
```

这里得到的是自该 process-start coverage 起积累的数据，不是完整日数据。通过 facade 打开这个 raw tail 还会增加一个独立 FAST client mapping。

### 15.3 单标的 FAST-derived live Event

```python
events = frames.open_instrument_derived_event_history(
    600000,
    coverage_requirement="allow_process_start_partial",
    live_tail=True,
    event_control_socket_path="/run/l2flow/event-delta.sock",
    maximum_order_states=1_000_000,
    page_records=4096,
    tail_batch_records=4096,
)
events.wait_ready(timeout=10.0)

cut = events.latest_snapshot()
event_table = cut.dataframe

# 跨 live/History replacement 的逻辑身份使用 event_uid。
uids = event_table.select("event_uid")
```

### 15.4 CERTIFIED Event 与 Tick

```python
cert_events = frames.open_certified_order_event_history(
    "/run/l2flow/certified.sock"
)
cert_ticks = frames.open_certified_tick_history(
    "/run/l2flow/certified.sock",
    include_wire_payload=False,
)
try:
    cert_events.wait_ready(timeout=10.0)
    cert_ticks.wait_ready(timeout=10.0)

    event_cut = cert_events.latest_snapshot(
        consistency="latest_published", timeout=5.0
    )
    tick_cut = cert_ticks.latest_snapshot(
        consistency="latest_published", timeout=5.0
    )
finally:
    cert_events.close()
    cert_ticks.close()
```

两个产品都是全局 from-open history-to-tail，但 sequence、schema 和 dataset identity 不同，不能互相 append。

### 15.5 显式 recovered socket promotion factory

```python
from l2flow_realtime import UnavailableError, as_polars, connect

preview = frames.open_instrument_tick_history(
    600000,
    coverage_requirement="allow_process_start_partial",
    columns=("tick_stream_sequence", "price_p6"),
)
preview.wait_ready(timeout=10.0)
expected_identity = preview.dataset_identity

# 顶层 L2FlowClient 的生命周期由应用管理。
promoted_clients = []

def promotion_factory():
    try:
        recovered = connect("/run/l2flow/recovered-realtime.sock")
    except UnavailableError:
        return None
    try:
        candidate = as_polars(
            recovered
        ).open_instrument_tick_history(
            600000,
            coverage_requirement="from_open",
            columns=("tick_stream_sequence", "price_p6"),
        )
    except BaseException:
        recovered.close()
        raise
    promoted_clients.append(recovered)
    return candidate

switching = frames.auto_promoting_history(
    preview,
    promotion_factory,
    expected_dataset_identity=expected_identity,
)
try:
    switching.wait_ready(timeout=10.0)       # preview 或 recovered 已可读
    switching.wait_promoted(timeout=120.0)  # 已切换到 from-open candidate
    complete = switching.latest_snapshot()
    assert complete.history_coverage.coverage_from_open
finally:
    switching.close()
    for client in promoted_clients:
        client.close()
```

该 factory 明确选择 recovered socket。wrapper 不会把 preview row 与 candidate row 拼接，也不会替应用关闭 `promoted_clients` 中的顶层 client。

## 16. 性能与资源注意事项

### 16.1 inactive path

不调用 `as_polars()` 或任何 history opener 时，不会启动 Python Polars
history thread。`live_tail=False` 只保证不会创建对应的 Python FAST
raw/Event live-ring poller、额外 raw FAST mapping 或 Event-ring handle。
FAST raw/Event live tail 以及两个 CERTIFIED Polars history-to-tail
consumer 都是应用层显式 opt-in。

这不表示 CERTIFIED 服务端完全没有 History 成本。为保证客户端在任意
时刻 attach 后仍能从 sequence 1 取得完整 prefix，只要 CERTIFIED service
运行，其 Tick journal writer 就会持续在配置的 Event/tick-history CPU set
上异步追平已公开的 bounded CERTIFIED slots。它不阻塞普通 bounded
CERTIFIED publication，但会消耗所配置 core 的 CPU、shared-cache、内存
容量和内存带宽；online-recovery 的冷 prefix fence 还会等待它追到固定
frontier。因此“默认 Python tail 关闭”与“整机严格零性能回归”是两件
不同的声明，后者仍必须用目标流量、绑核和 NUMA 配置完成资格测试。

### 16.2 tuning knobs

- 增大 `batch_records`/`page_records` 通常减少 Python/native 往返，但增加单批复制、验证和 publication 时间；
- 减小 `tail_poll_interval`/`poll_interval` 可缩短空闲时发现新数据的等待，但增加 wake-up 与空 poll；
- `refresh_interval` 控制 durable FAST History generation 的刷新节奏，不等同于 live ring poll interval；
- `tail_batch_records` 只控制 FAST live Event batch；
- `maximum_order_states` 是单标的 native derived state 上限，不是 Polars row retention；
- `compact_after_chunks` 较小会更频繁 rechunk，较大则保留更多 chunk；
- `include_wire_payload=True` 增加 CERTIFIED Tick 每行内存和复制量。

### 16.3 memory ownership

live ring 数据必须复制，历史 batch 也必须在 lease 结束前变成 owned columns。Polars snapshot 的 shallow clone 不复制所有列 buffer，但任何仍存活的 pinned snapshot/LazyFrame 都会延长相应 buffer 生命周期。

atomic replacement、reconciliation 和 rechunk 会在短时间内同时保留 old/new frame；大表应为这种峰值预留内存。`maximum_rows`/`maximum_chunks` 只能 fail-closed，不能降低峰值。

### 16.4 thread、mapping 与 fd

每个 autostart history handle 通常拥有一个后台 Python thread 和一个或多个 native reader/resource。raw FAST live tail 额外拥有独立 FAST client mapping；FAST-derived live tail拥有 Event ring handle，并在 overrun 时重新 control attach。大量单标的 live handle 会线性增加 thread、reader、mapping/fd 和扫描成本。

当前 Polars API 不设置这些 Python background thread 的 CPU affinity。若部署需要隔离 core，应在进程/线程调度层另行设计和验证，不能从本 API 推断已经 pin 到特定 core。

### 16.5 latency interpretation

`latest_dataframe()`/snapshot clone 主要共享 immutable buffers，但 background append/reconciliation、Polars concat、rechunk 和 spill 都有真实 CPU/内存或 I/O 成本。`latest_published` 还可能阻塞等待 reader 追上观测到的 frontier。

因此低延迟读取循环通常应使用 `cached` snapshot，并在独立控制节奏上 refresh；需要强 freshness 时再承担 `latest_published`/`refresh()` 的等待。任何 p99/吞吐结论都必须在目标 batch、poll、instrument 数、event rate、NUMA/affinity 和 table size 下实测，本文不提供硬编码性能保证。

## 17. 生命周期检查表

生产调用方至少应做到：

1. 明确选择 `from_open` 或 `allow_process_start_partial`；
2. `wait_ready()` 后再读取首个 snapshot；
3. 保存并校验 `history_coverage` 与 `dataset_identity`；
4. FAST-derived 跨 live/History 身份使用 `event_uid`；
5. 捕获 `PolarsHistoryRefreshError`，只在明确接受 stale 时使用 `allow_stale=True`；
6. 为 compaction shadow、pinned snapshots 和全局 CERTIFIED 表预留内存；
7. 把 spill 当作 snapshot export，而不是 eviction/durability；
8. promotion factory 显式选择 recovered socket，并管理它创建的顶层 client；
9. 调用 history handle 的 `close()`；
10. 分别关闭应用自己持有的 `L2FlowClient`。
