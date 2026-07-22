# Production Instrument Runtime V1 决策

## 1. 状态与范围

本决策定义 `l2flow_production` 与 `mdl-production-router` 的首个正式 live
数据路由。目标是在保留每条 source 的权威输入顺序的同时，把已解码行情按
instrument 固定分配到多个 C++ worker，并在进程内保存可快速查询的全日历史。

V1 的边界是：

- 只接受交易日日初的 fresh Raw、fresh SourceFrontier、fresh Canonical 和空
  history；
- 接通四路 Raw live capture、control decoder、market decoder、Canonical、
  instrument history 与 ProductionRoute；
- 不接中途恢复、takeover 或交易所级行情 replay；
- 不发布虚构的 history/state/factor 文件端点；history 通过同一进程中的
  `ProductionServiceV1::history()` 访问；
- 不把 Phase 6 Latest State、Phase 7 factor executor 或 Phase 8 Parquet 服务
  描述成已接入。

`L2Flow::production` 指向 `l2flow_production`，但这次路由切换不等于 Phase
2–8 的完整日、目标机性能、断电、真实语料或数学因子 external exit 已完成。

## 2. 端到端顺序不变量

每条 source 都有且只有一个 `ProductionSourcePipelineV1` 调用线程，处理顺序是：

```text
SDK callback
  -> ByteRing
  -> Raw WAL append/durable
  -> source-local RawLiveTail
  -> ControlDecoderV1
  -> MarketDecoderV1
  -> Canonical bundle commit
  -> InstrumentHistoryRuntimeV1 dispatch
```

以下顺序不可交换：

1. callback 先发布 captured frontier；Raw sole writer 才可发布 append frontier；
2. pipeline 严格按该 source 的 Raw `ingress_sequence` 取下一条记录；
3. 对同一条记录先执行 control decode，再执行 market decode；
4. market message 只解码一次；Canonical bundle 成功 commit 后，才把拥有完整
   数据所有权的 event 交给 instrument history；
5. history admission 完成前，pipeline 不读取下一条 Raw。

沪市 4.24 tick decoder 含 source-local phase/sequence 状态。因此不能先按
instrument 拆分原始消息再并行解码；那会让多个 worker 各自看到不完整的状态机，
无法保持与 source 顺序等价。并行边界只能位于解码和 Canonical commit 之后。

四条 TCP/source 流之间没有权威全序。V1 不按本机接收时间、交易所时间或 worker
完成时间制造跨 source 排序。

## 3. 固定 worker 映射

instrument 路由采用两级固定映射：

```text
logical_shard = instrument_id % 16
physical_worker = configured_owner[logical_shard]
```

- logical shard 数固定为 16；
- physical worker 数由部署显式选择，合法范围为 1–16；
- 默认 owner 是 `logical_shard % physical_worker_count`，也可在构造时提供完整
  的 16 项显式映射；
- runtime 启动后映射不可变，不做基于瞬时负载的迁移；
- 每个 instrument 始终由同一个 logical shard、同一个 physical worker 追加，
  不需要对单 instrument 使用多写者锁；
- 共有 `4 sources × 16 logical shards` 条有界 SPSC admission queue；一个
  physical worker 可以轮询其拥有的多条 queue。

这些 worker 只负责 decoded history 的分发与追加，不执行 Phase 7 因子插件，
也不应称为 factor worker。

## 4. 排序、可见性与查询

history 的实际查询域是：

```text
(instrument_id, source_slot, lane)
```

lane 只有 `snapshot` 和 `tick`。深市 order 6.33 与 transaction/cancel 6.36
共享 `tick` lane；它们继续使用同一 source sequence 语义。

每条 source 的 submission 获得稠密 dispatch ticket。worker 可以并行完成不同
logical shard，但 acknowledged ticket 只推进到满足以下条件的最大连续前缀：

```text
ACK = max n, 使 ticket 1..n 均已完成
```

查询先捕获该 source 的 ACK，再只暴露 `dispatch_ticket <= ACK` 的记录。因此，
若 ticket 8 尚未完成，即使 ticket 9 已由另一个 worker 追加，ticket 9 也不能被
查询提前看到。这避免 worker 调度顺序伪造 source 顺序。

公开查询为：

- `Latest`：返回该 source/lane/instrument 在 ACK 前缀内的最后一条记录；
- `Tail`：返回同一域末尾的有界条数；
- `RangeBySourceSequence`：返回同一域中 source sequence 范围内的记录。

`Tail.count` 与 `RangeBySourceSequence.maximum_records` 还受部署项
`history.maximum_records_per_query` 的单次硬上限约束；越限在获取 shard 锁和
分配结果之前返回 `kQueryLimitExceeded`。查询只在锁内定位并固定不可变 full chunk
的精确 slice；逐记录遍历在解锁后完成。唯一可能继续 append 的末尾非满 chunk
只在锁内取得有界 handle，其预留容量保证既有记录地址不因后续 append 移动。

单域内按严格递增的 `source_sequence` 追加；sequence gap 合法，重复或倒退被
拒绝。接口不承诺 snapshot 与 tick 两个 lane 的合并顺序，也不承诺跨 source
全序。返回 handle 持有相应 append-only chunk，调用者不得把它解释成 route
撤销后的新查询授权。

Canonical commit 与异步 history append 之间允许存在至多受 admission/inflight
上界约束的短暂差距。queue 满或 inflight 达上界时，pipeline 保留尚未提交的 owned
envelope，并停止读取该 source 的下一条 Raw。需要同时读取 Canonical 与 history
的调用者必须等待精确 history barrier，不能只观察 Canonical publication。

## 5. 内存与容量

V1 是全日 append-only 内存历史，不淘汰旧记录。部署必须显式设置：

- 每条 SPSC queue 的 usable capacity；
- 每 source 最大未连续 ACK ticket 数；
- chunk record capacity；
- 每次 `Tail/Range` 最大返回记录数；
- 每 logical shard 最大记录数；
- 每 logical shard 最大 instrument 数；
- 每 logical shard owned payload bytes 硬上限。

任一硬上限耗尽都不会覆盖旧数据或静默降级。history source 被标记 Fatal，聚合
运行时撤销 Active route。Canonical segment 同样是 fixed-capacity；V1 不在日中
隐式 rotation，segment full 是 source Fatal。因此容量必须按完整交易日峰值、
热点证券偏斜、variable-depth payload 和安全余量共同估算，而不能只用平均流量。

## 6. Fresh 启动与身份闭环

正式入口固定读取 deployment directory 下的 `production-v1.tsv`，并要求调用者
提供 exact-byte SHA-256 pin。manifest、registry、endpoint contract 与 SDK library
各自使用独立 pin；credential 不进入稳定配置 hash。

V1 的持久时钟来源字符串固定为
`CLOCK_REALTIME+CLOCK_MONOTONIC_RAW:V1`：接收与 segment-age 的 monotonic
时间来自 `CLOCK_MONOTONIC_RAW`，wall time 来自 `CLOCK_REALTIME`。control.page
writer heartbeat 使用 `CLOCK_MONOTONIC` 做同机存活判断，不属于持久 clock epoch；
manifest 不能用任意标签替代真实取时钟实现。

仓库中的 `configs/production-v1.example.tsv` 只是可通过 parser 的语法模板，不是
容量建议。部署前必须替换其中全部 placeholder 与示例 path/name/hash/identity/date/
device/generation/capacity/timeout，设置 deployment directory 为 owner-only `0700`、manifest 为
`0600`、独立 credential 文件为 `0400`，最后再对 manifest 精确字节计算命令行 pin。
`--check` 不加载 vendor 代码、不获取 SourceFrontier role、不 attach coordinator、
不改变 SCAFFOLDING，也不创建 Raw/Canonical/frontier/route 制品。
它会只读检查固定 route current/tmp、owner lock/metadata/tmp、本 generation 的四个
Raw stream 目录、四个 SourceFrontier 文件及全部 Canonical segment、manifest 与
`manifest.tmp` 在当前时点均不存在；该检查不提供未来时点的权威性，各 creator 的最终
O_EXCL/identity gate 仍是权威，route 发布也仍须在进程门与 directory flock 内原子复核。
preflight 会对四个预先存在且只读的 SDK-log directory marker 获取非阻塞 flock：
`--check` 退出即释放，正常模式则把同一 lease 移交 source lifetime，不重新打开路径。

在第一个 SDK `Connect()` 之前，`ProductionServiceV1::Create()` 验证四路：

- 固定 source slot、`IngressKind` 与 `source_stream_id`；
- capture date、stream-day ID、Raw writer instance；
- SourceFrontier generation 与同一映射页指针；
- Canonical generation、trade date、schema、registry 与 normalizer identity；
- capture、pipeline、route manifest 三者的完全一致性；
- 四个 pipeline 注入的是同一个 history runtime。

当前 production pipeline 必须从 Raw next sequence 1、processed frontier 0、control
genesis、空 Canonical sinks 和空 history 开始。recovered/adopted Raw runtime 不会
提供 `TakeFreshPipelineLiveTail()`，不能绕过这一限制接入 V1。

history 保存的是 Canonical bundle 成功提交后才准入的、已拥有内存的 Phase-4 decoded
event，不是 Canonical record 的副本。Canonical-only 的 SH tick phase attribution、
sticky sequence-quality 与 Canonical event ID 不会反向写入 decoded event；依赖这些
字段的因子必须读取或 join committed Canonical projection，不能把 history 冒充为
Canonical/factor-safe 输入。

Raw descendant 创建与运行全程从 coordinator 同一个 retained Raw-root dirfd 派生，
不会重新解析配置 pathname；Canonical sink 创建及 manifest seal 同样锚定 retained
Canonical-root dirfd。fresh owner/Active 发布前，controller 在发布锁内重开
`route_root` 与 `canonical_root`，要求 owner/mode/device/inode 与 retained fd 一致；
不一致时不创建 owner 或 Active。发布后的任意 pathname 替换仍必须由稳定 mount、
独立 service UID 等部署策略禁止，因为普通进程无法冻结外部 pathname namespace。

正常模式一旦开始创建 fresh frontier/Canonical/Raw generation 制品，就不承诺失败
回滚或原地重试。后续步骤失败时保持 fail-closed，运维必须切换到重新 provision 的
新 generation 或显式 recovery/takeover 流程；因此应先运行 `--check`。

四路 capture 都返回 Start、control/readiness gate 和 history barrier 全部通过后，
才允许发布 Active route。任一身份、持久化、解码、Canonical、history 或 route
错误均 fail closed。

## 7. Route 存活与停止

route manifest 的 `state=Active` 只表示结构上有效，不能证明 owner 进程仍存活。
跨进程 consumer 必须通过 `ReadLiveProductionRouteV1At()` 获取带 guard 的 live
route，并在一次 factor batch 前后重新验证。验证同时绑定 route bytes、owner PID、
Linux boot ID、`/proc/<pid>/stat` start ticks、pidfd、lock inode 与域摘要。

如果 pidfd 被内核、seccomp 或权限策略禁止，入口必须在发布 Active 前失败。同一
owner 进程内不得另行 open/close production route lock inode：V1 使用传统
process-associated `F_SETLK`，关闭同一 inode 的任意 fd 都可能释放该进程已有锁。

正常停止顺序固定为：

```text
BeginDrain / durable route revocation
  -> Stop four Raw producers
  -> wait four source End + exact history barriers
  -> stop aggregate workers
  -> StopAndDrain history workers
```

Active 后发生异步 aggregate Fatal 时，入口必须调用 `Stop()` 完成 capture/history
清理并以非零状态退出。不能仅检查 `ProductionServiceStateV1::kActive`。

## 8. 超时边界

`activation_timeout` 从四个 capture `Start()` 全部返回后才开始，只约束 readiness、
history barrier 与 route publication。供应商 `Subscriber::Connect()` 没有 timeout
或 cancellation 契约；同理，不能假设 vendor Shutdown 必然在配置期限内返回。

需要硬启动/停止上界时必须由外部 supervisor 实施进程级 deadline：先请求正常
SIGTERM，最终期限仍未退出则升级 SIGKILL。SIGTERM 后调用 `Stop()` 也可能等待
阻塞中的 vendor 调用，不能把 SIGTERM 本身描述成有界保证。强杀后的 Raw
generation 必须走 recovery/takeover 流程，不能视为 clean stop；该流程不属于
当前 fresh-only V1。

启动期间收到 SIGINT/SIGTERM 时，入口立即调用 `Stop()` 发布 cancellation latch，
不等待四个 capture 全部进入 started。若 `Start()` 尚未返回 authoritative Active，
即使已启动 capture 的本地清理成功，也不能把已经创建的 fresh namespace 当作可重试；
入口以非零状态退出，并要求重新 provision 或进入显式 recovery/takeover。

manifest 的 `metrics_path` 当前只进入 Raw 稳定配置 hash 与路径隔离校验；正式聚合
入口尚未启动 `MetricsWorker`，也不会实际发布这些 textfile。真实流观测在补齐该
发布链之前只能使用已有进程内 snapshot、日志和外部进程指标，不能把路径存在于
manifest 当作 metrics 已落盘。

本聚合入口把 `credential_name` 作为经过校验并进入稳定配置 hash 的身份标签；SDK
实际使用的 token 来自独立 pin 的 `credential_path`。同样，
`raw.ring_stall_budget_seconds`、`raw.reserve_domain_id`、
`raw.reserve_coordinator_socket`、`raw.reserve_ack_timeout_milliseconds` 与
`raw.emergency_reserve_bytes` 会被校验并进入每路稳定 Raw 配置 hash，但本入口不会
据此启动 socket reserve client 或独立 ring-stall timer。reserve 权威来自 retained
Raw-root 下已经 provision 的 coordinator。

manifest 的 `scaffolding_allocation_cap` 与 `safe_stop_template_id` 也是对已 provision
coordinator 状态的精确身份断言，不会由入口重新配置 reserve。fresh 启动必须在任何
frontier/Canonical/Raw 变更前，核对四个 SCAFFOLDING entry 的 route、stream-day、
writer、recovery-attempt、`kFreshInit`、`kResumeConnect`、cap 与 template ID 全部一致。

## 9. 真实流调试最小清单

接入真实数据时至少记录并核对：

1. 四路 required subscription、control epoch 与 readiness；
2. callback/captured/append/durable/processed source cursor；
3. history submitted/ACK ticket、source sequence、queue full 与 inflight；
4. logical shard 分布、热点 instrument、各 worker backlog 与 CPU/NUMA 绑定；
5. history records、owned payload、RSS、page fault 与 Canonical 剩余容量；
6. route Active/Fatal 转换、owner guard 与 SIGKILL stale-Active 拒绝；
7. 正常停止的四路 End、history barrier 和 Raw clean-stop 对账。

真实流调试通过只能作为 staging 证据。完整交易日重复性、真实 corpus 逐字段对账、
目标机峰值与尾延迟、crash/power-loss、recovery/replay 和因子数学验收仍需分别完成。
