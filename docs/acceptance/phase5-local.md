# Phase 5 local acceptance record（本地代码范围）

Date: 2026-07-22
Host scope: local Linux x86-64 development container

> 2026-07-22 superseded note：本记录保留 standalone Phase 5 slice 截至
> 2026-07-22 的验收事实。`L2Flow::production` 后来通过独立授权变更切换到
> `l2flow_production`，并把 Canonical runtime 接入 fresh live 四源聚合；本文列出的
> 完整日、性能、crash 与 replay external exit 不因此完成。

## 结论

本记录中的“完成”仅指 Phase 5 仓库内 library/runtime slice；不等同于生产 feeder
切换或 `docs/design.md` 的正式 Phase 5 Exit。

| 口径 | 本记录时状态 | 判定依据 |
| --- | --- | --- |
| Phase 5 local implementation slice | 已完成 | Canonical V1 schema/validator、all-day sequence state、固定容量 mmap segment、one-shot SourceFrontier、safe mux、market/control bundle coordinator 与 committed reader 已连成一条本地链路 |
| 不重复 feeder 持久化 | 已满足 | 新 coordinator 只接 caller-fed Raw context/envelope；没有 CSV/Raw WAL/journal writer API 或路径，exact record 由 mandatory envelope verifier 对现有 feeder/Raw reader 验证 |
| Scoped local verification | 已通过 | 最终工作树 Debug、Release、ASan+UBSan 的 `phase5` 7/7 均通过；Phase 4 回归 4/4 通过，命令与限制见下文 |
| Production integration/cutover | 未完成 | 本阶段工作当时没有改 feeder callback/tail service、systemd unit 或 production alias；截至本记录日期，`L2Flow::production` 仍为 `l2flow_phase01` |
| Formal Phase 5 exit | 未完成 | 完整日 live/replay hash、真实 crash matrix、重复 mux hash、2×/5× 性能和 single-writer 运行制品尚未提供 |

## 本地实现范围

### 数据注入与全天状态

Phase 5 不重写 feeder 已有的 CSV/Raw 持久化。调用方传入
`CanonicalRawContextV1 + MarketMessageViewV1` 或已经由权威 control decoder
产生的 `ControlRecordV1`，分别调用
`CanonicalBundleCoordinatorV1::ProcessMarket` 或 `ProcessControl`；coordinator
先用 `SourceFrontier` 验证 Raw append high-water coverage，再调用 mandatory market/
control envelope verifier。Frontier 本身不能证明中间 `(ingress, WAL)` 是真实 record
boundary，也不能把该 cursor 绑定到传入内容；verifier 必须向 caller 的已排序 feeder
或 validated Raw reader 验证 exact next record 和完整 immutable envelope，不能只重复
frontier 大小比较。验证通过后才调用 normalizer 的 `Prepare` 或 `PrepareControl`。

Phase 4 `SessionRetentionModeV1::kFullSession` 承担 owned “开盘以来”解码历史；
`max_records/max_payload_bytes` 是四个配置流合计的非零 hard admission limits，达到
任一限制时拒绝当前记录并 poison 该 source，不覆盖旧记录。Phase 4 store 与 Phase 5
是两个独立注入边界：`ProcessMarket` 不会隐式填充 `MarketSessionV1`，需要两种视图时
由 feeder 显式 fan-out 并定义联合失败策略。Phase 5 vendor/exchange guard 与 SH phase
map 同样跨 reconnect 保持全天状态。Canonical mmap segment 的全部 published prefix
在 mapping 生命周期内仍可作为 physical/debug view 访问；只有 committed reader/safe
mux 提供逻辑可消费视图。Segment 是固定容量，OS page cache 也不是物理 DRAM pin。
Repository-local V1 要求 fresh normalizer、zero processed frontier 和 empty
sinks 在交易日边界一次 attach；固定容量必须覆盖完整交易日。V1 没有同 generation
原地 rotation、normalizer checkpoint codec 或中午续接，不能把 seal/processed cursor
当作可恢复全天 sequence/phase state 的 checkpoint。
attach 还要求完整 route manifest：Snapshot/Tick 各覆盖每个 configured shard，
Quality/Control 各且仅有 shard 0；缺失、重复或越界 route 在处理首条消息前拒绝。

每个 4096-byte `SourceFrontierPageV1` 只初始化一次并绑定一个 immutable Raw writer
instance/generation。callback、append、processed 和 state mutation 带 expected
writer/generation fencing；idle publication 双读并复核同一 immutable identity，旧 owner
不能写新代。`progress_generation` 覆盖
state/quality/cursor/time，`callback_generation + callback_inflight` 防止 idle 双读的
0→1→0 ABA。FATAL latch 一旦置位不可回到 HEALTHY，也不能继续 append/processed
progress。

### Canonical 与数学语义

冻结的 V1 record 大小为 Header 112、Tick 192、Snapshot 2048、Quality 192、
Control 256 bytes。schema/dtype hash、registry、normalizer build/config、capture/stream
day、Raw writer/source generation、完整 clock identity 与 Canonical generation 都进入
segment identity。

定点换算只用 checked integer arithmetic。Tick 有效 native quantity 要求 scale=0
且非负；snapshot quantity 仅在 `raw % 10^scale == 0` 时精确除法，负值和非整除值
只产生 rejection Quality，不舍入。schema validator 独立复核 scalar/十档/队列
数量、逐 producer/action validity matrix、quality type/scope 和所有 validity/reserved
bits。SH A/D/T/S、SZ 6.33、SZ 6.36 分别限制可达字段；SZ 非限价订单不能发布
factor-safe price，SZ cancel 的单侧 order reference 必须与 primary ID/side 一致，
tick business sequence 不能超出正 `int64_t` wire domain。

vendor scope 精确为 `(capture_date, source_stream_id, stream_day_id, ServiceID,
MessageID)`；service version 不在 identity 内，但 version/encoding/LocalTime/body 进入
exact duplicate evidence。SH scope 是带 market-kind 的
`(trade_date, source_stream_id, channel)`；SZ 6.33/6.36 共用一个带 SZ-kind 的同形
unified channel scope。Reconnect/connection/subscription epoch 都不重置这些 scope，
expected-first 按 exact scope 配置。poison 的 `first_bad_origin_wal_end_pos` 只在 commit 时
落状态，Abort 后重试不会残留，后续诊断始终指向第一个已提交坏 WAL。

### 跨 family 原子可见性

每条 Raw 的固定顺序为：

```text
frontier identity 与 append-prefix 校验
→ mandatory exact-record/full-envelope 验证
→ normalizer Prepare/PrepareControl
→ 所有 configured sink 完整 preflight（包括本条无输出的 sink）
→ 发布全部 record
→ 独立重算 receipt 并 CommitPublished
→ 推进所有 configured segment processed cursor
→ 最后 PublishProcessedProgress
```

最后一步是该 source 唯一跨 family 逻辑 commit，不是跨 source 事务。
底层 `PublishedRecord` 只表示物理前缀；
`ReadCommittedCanonicalRecordV1` 和 safe mux 同时校验 ingress 与 WAL coverage、Raw
writer/generation、capture/stream day 与 clock，global processed commit 前不会暴露半个
Quality+Tick bundle。每个 required mux input 必须借用 live SourceFrontier page；决策时
重新读取 sticky FATAL，不能用先前保存的 healthy snapshot 继续放行。

容量不足在 publish 前可 Abort；V1 不在原 generation 内 rotation/retry。identity、
corruption、已有 fatal 或进入 publish 后的任一失败，会先发布 SourceFrontier sticky
FATAL 作为全局撤销锚点，再 fail-stop normalizer 并向所有 configured segment fan-out
`generation_fatal`。全局 processed cursor 即使保持旧数值，也不再授权读取旧前缀：committed reader
与 safe mux 拒绝该 generation 的全部数据，包括故障前已经 committed 的记录；fatal
segment 也不能 Seal。上层必须丢弃整代，创建新的 SourceFrontier page/Canonical
generation，并在单一 Raw-writer/clock identity 内从交易日日初 Raw 重放，不能复用已
物理出现的 event ID；跨 writer/clock identity 的全天重建需要 Local V1 尚未实现的
generation-chain/state transition。当前库只提供只读
`InspectCanonicalSegmentForRecoveryV1`：它验证 sealed segment 的 header 与两个
hash（manifest 需由调用方另行校验），或对 open segment 返回
`kUnsealedDiscardWholeGeneration`。它不会自动删除、截断、修复、重放或切换
generation；也没有 local normalizer checkpoint/rotation 恢复路径。这些动作仍需上层
recovery controller 实现并留证。

低层 `CanonicalSegmentWriterV1::Seal` 仅证明单个 segment 的本地 header/records/hash，
不证明 SourceFrontier healthy/caught-up、normalizer 无活动事务或全部 sibling
family/shard 位于相同 processed cursor；不能把若干本地 Seal 当成 production
generation cutover certificate。日内 clock epoch 或 Raw writer generation 改变也没有
同代连续处理路径，Local V1 必须新建 generation 并由上层从交易日日初重放。

committed span 与 mux `READY` 都只是点时证明，不能撤销调用方已经读取的字节。factor/
consumer 必须保存参与输入的 generation identity，并在任一 live frontier 后续 FATAL 时
取消并丢弃该代派生状态。

## 定向测试

目标列表：

```text
test_phase5_canonical_schema
test_phase5_sequence_guard
test_phase5_frontier_mux
test_phase5_canonical_segment
test_phase5_canonical_normalizer
test_phase5_canonical_control_adapter
test_phase5_canonical_bundle_runtime
```

覆盖包括：schema ABI/golden/hash、sequence first/gap/duplicate/conflict/backward/
capacity、per-scope expected-first、首坏 WAL、fixed-point 数量、type/scope 与 action
matrix、SourceFrontier callback ABA/sticky FATAL、clock barrier、segment single writer/
seal/read-only recovery inspection、multi-family committed visibility、preflight
capacity、mandatory envelope rejection、partial-publish 全代 fail-stop/旧 committed
prefix 隐藏、saved queued proof 在 live FATAL 后不能继续 mux，以及 API/SYS control
的 normalizer-owned event ID。

最终工作树验证结果：

| 配置 | 命令口径 | 结果 |
| --- | --- | --- |
| Debug | `ctest --test-dir build -L phase5 --output-on-failure` | 7/7 passed |
| Release | `ctest --test-dir build-release -L phase5 --output-on-failure` | 7/7 passed |
| ASan+UBSan | `ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 ctest --test-dir build-asan -L phase5 --output-on-failure` | 7/7 passed |
| Phase 4 regression | `ctest --test-dir build -L phase4 --output-on-failure` | 4/4 passed（含 decoder fuzz smoke） |
| Phase 3 regression | `ctest --test-dir build -L phase3 --output-on-failure` | 5/5 passed |
| TSan | GCC 11 `-fsanitize=thread` 完整编译 Phase 5 后执行同一 7-test suite | 当前容器在测试主体前统一失败：`ThreadSanitizer: unexpected memory mapping`；不是通过证据，也未归因成代码 race |

冻结 golden：schema descriptor SHA-256 为
`f66cc65410a5c0862b87a2e467f13b02fe40c0910c17e418be82209b0ddf405e`，
dtype descriptor SHA-256 为
`f92a990e174f4f1aae60244fef447f007c83292f8b8aeb375cc1913f7c8ba5bd`，
tick fixture bytes SHA-256 为
`ef332b1d1a6f6868b1586111eee27612555b0d5a660f2a2ed594aee15cc99c97`。

上述结果只覆盖仓库内确定性测试。TSan 运行时不兼容已如实保留，任何未执行的
外部制品均未写成通过。

## 尚未完成的正式 Exit gate

正式 Phase 5 退出仍需至少：

1. 四源完整交易日 live 与 Raw replay 的逐 segment/global Canonical hash 一致；
2. publish/commit/progress/frontier 每个切点的 SIGKILL/crash fault matrix 与整 generation
   replay 演练；
3. receive-time safe mux 多次 replay 的全序 hash 一致；
4. 目标机 2×/5× 峰值 normalizer、mmap publish→reader p99、RSS/page-fault/NUMA；
5. 真实进程和文件锁层面的无 multiwriter 证明及 generation manifest/cutover；
6. 在真实 production feed 上证明正式 source-order pipeline 按 Raw 权威顺序调用
   coordinator、没有第二个 Raw/CSV persistence owner，并保留 envelope-verifier
   wiring 的可审计证据。

本记录当时要求上述证据完成并评审前不改变 `L2Flow::production` alias；即使完成，
也只是 production cutover 的必要证据，不会自动改 alias，仍需独立评审与授权。
后来的独立 alias 变更不追溯满足上述证据。
