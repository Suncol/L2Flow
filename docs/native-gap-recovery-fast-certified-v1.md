# Native gap recovery：FAST fail-open 与 CERTIFIED 正确前缀

## 1. 目标与不可混淆的语义

本实现把两种读语义明确分开：

- **FAST**：沿用原有 Wire V2、C ABI、Python reader、共享内存和控制
  socket。它按实际到达/应用顺序持续发布；发现原生序号缺口、收到回补、
  重复消息或 CERTIFIED 资源耗尽，都不会调用 FAST 的
  `MarkCoverageLost()`，也不会令 FAST 的 `PublishApplied()` 返回失败。
- **CERTIFIED**：只发布已经证明连续且完成投影的原生消息前缀。缺口后的
  Tick 先留在有界 pending 区，回补后再按各自原生 channel 的序号进入
  canonical Tick、Event 与历史 Event。无法继续证明正确时，保留最后正确
  前缀并显式进入 gap/frozen 状态，不猜测、不跳号。

因此，“FAST 一直可访问”指网络缺口、乱序回补、重复/冲突及可捕获的
CERTIFIED 资源错误不会关闭 FAST。FAST 本身仍保持原有语义：回补的旧消息
也可能成为它最近一次到达的 latest；需要有序、可证明结果的读者必须读取
CERTIFIED。

上述 fail-open 是 sidecar/FAST sink 的组件契约，也是常规
`--intraday-store-from-open` 组合的服务策略。online CSV recovery 在启用
CERTIFIED 时有更强的 session promotion 契约：coordinator 会把 worker、handoff、
barrier 或 control failure 视为 recovered session 终止条件，不能把未完成证明的
完整会话降级成 FAST-only。显式 `--disable-native-gap-recovery` 才会从 online
组合中移除 sidecar。

## 2. SDK 文档依据

采用的连续性键只来自 SDK 文档明确承诺的交易所原生字段：

- 《通联数据 MDL 消息参考 959-SDK 用户版》PDF 物理页 17，
  3.5 / Shanghai NGTSTick 4.101.24：`BizIndex` 从 1 开始，并按
  `Channel` 连续。
- 同一文档 PDF 物理页 33，8.5 与 8.6：深圳逐笔委托/成交均携带
  `ChannelNo`、`ApplSeqNum`，序号从 1 开始。
- 《通联_沪深 L2 行情数据结构展示 V4.0》PDF 物理页 19、33–35
  再次给出上海 `(Channel, BizIndex)` 以及深圳同一
  `(ChannelNo, ApplSeqNum)` 内唯一、连续、从 1 开始的约束。

由此采用以下 domain 定义：

```text
Shanghai domain = (trade_date, SH, Channel, feed_epoch)
sequence         = BizIndex

Shenzhen domain = (trade_date, SZ, ChannelNo, feed_epoch)
sequence        = ApplSeqNum
message 6.101.33 与 6.101.36 共享同一个 sequence domain
```

没有使用 `MDLMessageHead::SequenceID`、接收时间或本地时间来证明交易所
连续性。参考文档也没有给出可安全推断的“重连后 replay 已完成”、序号自动
重置或 correction window 协议，因此实现不会根据超时或序号回退猜测新
epoch。需要完整 FAST/CERTIFIED 前缀的生产运行要求从
`--intraday-store-from-open` 与 `--intraday-recovery-csv-dir` 中选择一个
coverage source。两者都会重建一个
fresh session，所以原生 domain 的期望原点仍固定为 1：前者直接观察从开盘
开始的实时消息；后者由唯一 SDK owner 把 callback 写入 live journal，同时
让同日从开盘保存的 CSV 与 durable journal suffix 依次进入同一个 SDK-less
shadow Pipeline，闭合 handoff 后发布 recovered FAST/CERTIFIED 前缀。任意
晚于开盘的可信 checkpoint、上次进程状态或 ring overrun catch-up 仍不在
本协议范围内。

`--intraday-live-partial` 是第三种 startup mode，但不是 from-open coverage
source：它服务进程启动后的 latest 数据，并可通过不可变 generation 查询
同一起点的单标的 History/tick delta。canonical service 使用 bounded unknown-
origin bootstrap 生成 process-start Event journal，但 FAST 仍保持
`certified_prefix_valid=false`，Event wire 也明确标记
`PROCESS_START_PARTIAL`。它不能用启动后的 native sequence 片段伪造从开盘
完整的 CERTIFIED 前缀。

不同 channel 之间没有文档定义的交易所总序。本实现的
`canonical_apply_sequence` 只是本进程对多个已就绪 channel 的确定性发布
次序，不声明它是交易所跨 channel 顺序。

两份表都把深圳 6.33/6.36 的 `ApplSeqNum` 描述为同一 `ChannelNo` 下唯一
连续，本实现因此把两类逐笔消息合并到一个 domain。文档没有另列一句
“跨 6.33/6.36 共用计数器”；上线前仍应使用供应商确认或同时包含两类消息
的真实 replay 样本验证这个生产假设，不能用只含单一 tuple 的样本替代。

## 3. 正常路径：把额外工作限制为常数、无等待的旁路

```text
SDK callback
  ├─ 原有严格 instrument-key 校验
  ├─ [默认开启] 读取固定 native sequence 字段
  ├─ 向预分配、有界 MPMC queue 投递 Observation
  └─ 原有 decode → Store → History
                         │
                         ▼
              RealtimeCertifiedMarketServiceV1
                         │
              1. 调用原 FAST sink（必须先成功）
                         │
              2. 向同一有界 queue 投递 record 指针
                         │
                         └─ CERTIFIED worker 异步处理
```

关键点：

1. `PublishApplied()` 在任何 CERTIFIED queue、锁、投影或分配之前先调用
   原 FAST sink。
2. FAST 成功后，CERTIFIED queue 满、已 frozen 或内部错误均返回 `true`；
   只有 FAST 自己失败才返回 `false`。
3. producer 路径不做 reader 工作，不等待缺口，不排序，不构造 Event。
4. Observation 是固定大小值；Applied handoff 只携带 append-only Store
   record 指针。队列和 recovery 表在启动时预分配。
5. pipeline 未配置 native observer 时保持旧分支：不会提取原生序号；
   `--disable-native-gap-recovery` 可恢复 FAST-only 组合。
6. SDK 回调中的 observer 位于精确 instrument-key 校验之后、A-share filter
   之前。合法但被 filter 排除的消息仍占用交易所原生序号，必须成为
   output-free continuity token。

## 4. Recovery 状态与回补算法

每个原生 domain 独立维护：

- `origin_sequence`：from-open/recovered 为 1；partial 在 bounded bootstrap
  完成后取该 coverage epoch 已证明的最小序号；
- `observed_contiguous_sequence`：从原点起已经收到的稠密前缀；
- `highest_observed_sequence`；
- `certified_sequence`：已经提交到 CERTIFIED 下游的原生前缀；
- pending entry：原生 key、tuple、record class、到达次数、应用次数、
  canonical payload、Store cookie；
- 有界 duplicate retention。

主要状态为：

| 状态 | 含义 | 对 FAST 的影响 |
|---|---|---|
| `Healthy` / `CONTIGUOUS` | observed 与 certified 前缀连续 | 无 |
| `Bootstrapping` / `GAP_OPEN` | partial origin 尚未由 disorder bound 证明 | 无 |
| `Repairing` / `GAP_OPEN` | 仍有未收到的原生位置 | 无 |
| `CatchingUp` | 缺口已收到，但异步下游尚未提交完 | 无 |
| `FrozenConflict` | 同一原生 key 出现不同 canonical 业务负载 | 无 |
| `FrozenResource` | 有界容量、保留窗口或内部证明条件不足 | 无 |

partial 对每个 channel 维护最小值 `m`、最高值 `H` 和 operator 声明的最大
向后位移 `D`。当 `H-m >= D` 时，未来再到达 `u<m` 会推出
`H-u>D`，所以此时才把 `m` 确认为 origin。origin 建立后，若下一个缺失位置
为 `e`，`H-e == D` 仍允许合法晚到；只有 `H-e > D` 才冻结该 channel。
clean shutdown 在 handoff 排空后执行 `SealInput()`：连续低流量尾部从最小值
flush，真实 hole 或 observation/application 未闭合则只冻结对应 channel。
运行中不使用毫秒 timeout 猜测 origin。

以进程收到的第一条上海消息为 `BizIndex=3` 为例：

1. 不把 3 当成新原点；立即得到缺口 `[1,2]`。
2. FAST 正常发布 3；CERTIFIED Tick/Event 仍无伪造前缀。
3. 回补 1 后，CERTIFIED 可以安全提交 1，但 3 仍被阻挡。
4. 回补 2 后，按 2、3 提交，状态恢复为连续。

这不是“先错误发布 3，再回滚/重写历史”，而是把尚未证明的后缀留在
CERTIFIED 私有区；所以公开历史始终是不可变的正确前缀。

### Filter 与 duplicate

- exact instrument key 与 native sequence 固定字段有效、但 security id
  非 A-share 的 tracked Tick 进入 `Filtered` entry。其余非 A-share body
  不做完整 decode；它不生成 A-share Tick/Event，但必须经
  `PollCertified()` / `CommitCertified()` 显式推进 channel frontier。
- 同一原生 key 的 canonical 业务负载逐字节相同才幂等。比较时仅清除
  source/ingress/tick sequence、接收时钟、vendor header SequenceID /
  LocalTime、source stream/slot 等到达元数据；body 派生业务字段、显式
  nullness 与其它质量标志仍参与比较。
- 同 key 不同业务负载只 freeze 该 CERTIFIED channel，FAST 继续。
- 旧 key 已超出精确 duplicate retention 时，不能假定相等；该 channel
  进入 `FrozenResource`，而不是悄悄接受。

## 5. CERTIFIED Tick/Event 提交事务

对一个已就绪的 target Tick，单 worker 依次执行：

1. 构造并校验 envelope，预检 Tick ring/latest 槽；
2. 提交 recovery 内部 token；
3. 在 `AppendCertifiedTick()` 内先预检 Event 容量与物理 backing，再更新
   order projector 并向 canonical Event journal 追加 Event；
4. 取得与本 Tick 对应的 Event generation；
5. 发布 CERTIFIED Tick ring 与 per-instrument latest 槽；
6. 推进进程级 `canonical_apply_frontier` 和对应 channel 状态；
7. 以 aggregate header 的稳定 even tag 一次性提交公开可见 cut。

所有可能报告普通资源失败的 Event backing 预留均在 Event 有状态投影之前
完成，但发生在 recovery token 的内部提交之后。如果 Event 预检或投影仍
失败，coordinator 可能已经领先于公开 cut；服务会终止性 freeze，且不再
使用该内部进度。公开 CERTIFIED Tick/Event 始终停在共同 last-good cut，
FAST 已经先发布并继续工作。

Event 的物理写入有意先于 Tick header commit，以免覆盖旧 Tick 槽后再遇到
Event 失败。外部组合 reader 使用：

```text
coherent_canonical_apply_frontier =
    min(Tick header frontier, Event header frontier)
```

因此 Event 物理领先 Tick 时仍不可见。每个 Event row 同时保留：

- `canonical_apply_sequence`；
- 原始 native sequence/channel；
- 原始 source、ingress、tick-stream anchors。

Event 是 append-only journal，不受有限 Tick ring 回绕影响；其 V1.3 header
明确区分 from-open 与 process-start coverage。

### 并发发布协议

- Tick slot、Event slot、aggregate header 和 channel row 均使用
  `even → odd → payload → next even` seqcount。
- 写端以 `acq_rel CAS` 取得 odd epoch，payload 使用 relaxed atomic store，
  最终 even tag 使用 release store。
- 变更的 channel row 先发布到 aggregate header 的“下一 stable tag”。
  reader 读完 header、row、header 后，若 `row.publish_tag >
  header.status_publish_tag`，必须重试，不能暴露 future row。
- 未变化的旧 row tag 可以小于新 header tag，仍是当前有效状态。
- status 与单 slot 的 stable-copy reader 先写局部副本，只在完整校验成功后
  赋给调用者；Event batch reader 则允许在后续 slot 发生竞争前已经返回一段
  `written` 前缀，调用者必须同时检查返回码与 `written`。

独立 Event reader 通过新增控制 opcode 获取只读 memfd；原
`kGetSession=1` 请求和 FAST Wire V2 ABI 没有变化，Event history 使用
`kGetEventHistory=2`。

## 6. 生命周期与资源边界

默认边界：

- handoff queue：262,144；
- native pending：65,536；
- per-channel pending：16,384；
- duplicate retention：65,536；
- maximum reorder span：1,048,576；
- channel table：4,096；
- Event backing：按 64 MiB chunk 预留；
- Event 逻辑容量：生产配置按 Store 最大记录数的四倍保守推导。

Tick memfd 在 sidecar Create 阶段先 `fallocate` 全部有界 backing，再
`mmap/memset`；普通 `ENOSPC/EDQUOT` 因而成为可捕获的 Create 失败。常规
from-open 组合会降级为 FAST-only；online CSV recovery 则在 recovered control
暴露前终止 promotion。Event journal 只预留 header，随后在每次有状态投影前
按 chunk `fallocate`。

History 在所有 publisher join 后、释放 append-only Store 前调用
`QuiesceRecordReferences()`；CERTIFIED worker 先排空/停止，从而保证 queue
中借用的 record 指针不会悬空。

clean shutdown 发布 `STOPPED`，但保留 per-channel frozen/gap counters；严格
whole-market consumer 因此仍能拒绝不完整前缀。运行中单 channel freeze 的
aggregate state 是非终止 `DEGRADED`，健康 channel 可继续发布。

## 7. 生产启用方式

Native recovery / CERTIFIED 默认开启：

```text
--certified-ipc-socket PATH
    可选；默认 <ipc-socket>.certified

--disable-native-gap-recovery
    显式关闭，运行原 FAST-only 组合

--native-maximum-backward-displacement D
    partial canonical Event 必填；声明 callback 最大向后位移
```

常规 from-open 组合中，CERTIFIED Create/Start/容量配置失败只记录
`DEGRADED`，pipeline 继续使用原 FAST sink；成功时才安装 observation sink 与
FAST-first wrapper。online CSV recovery 不采用这个降级策略：启用 sidecar 时，
Create/StartWorker、handoff health、最终 prefix commit 或 StartControl 失败都会
终止 promotion，不会开放一个缺少所声明 CERTIFIED 前缀的 recovered session。
候选阶段的 exact FIFO probe 可重复且不修改 recovered coverage；
`GAP_OPEN/CATCHING_UP` 会驱动同一 online recovery session 逐条消费新的 durable
journal record，直到最早完整前缀被找到或同一个 absolute warmup deadline 到期。
`FROZEN_CONFLICT/FROZEN_RESOURCE`、handoff drop 和内部 Tick/Event frontier
不一致仍是不可重试的终态。

## 8. 验证

### 正确性

- Release 全量 CTest；
- 核心恢复、wire、reader、Event journal/history、service、pipeline、
  上海/深圳 Event projector 在 ASan+UBSan 下通过；
- native recovery、reader、Event journal 与真实 UDS service 在 TSan
  下通过；service TSan 连续 5 次通过；
- ASan+UBSan 的真实 UDS service 在修正测试 sink 的发布 frontier 后连续
  20 次通过；
- 真实 Unix domain socket fd handoff 集成通过；
- Event journal 的 10,000 次并发 append/read 测试无撕裂；
- 覆盖首包为 3、回补 1/2、gap 中 FAST 与 CERTIFIED last-good 可读、
  filtered token、exact/conflicting duplicate、retention 外 duplicate、
  channel/pending/reorder/Event 容量、applied-before-observe、深圳 33/36
  共享 domain、channel 0、shutdown 异常状态保持以及 row/header future
  gate。

### 延迟方法

机器固定在 CPU 8–15，计时器为 `CLOCK_MONOTONIC`，分位数使用 nearest
rank。每个正常路径进程含 256 warm-up 与 2,048 measured samples；下表为
5 个独立进程的分位数之中位数。

旧 FAST 公共 benchmark 使用 base commit
`82742c230da3b603c16fc994df73b5ca658cf104` 的不可变 build 与当前 build，
每个进程各含 10,000 samples：

| FAST 公共指标 | base p50/p95/p99 | 当前 p50/p95/p99 | 观察 |
|---|---:|---:|---|
| C successful latest read | 1.320 / 1.850 / 2.170 µs | 1.290 / 1.989 / 2.050 µs | −0.030 / +0.139 / −0.120 µs |
| callback origin → C latest return | 21.360 / 24.350 / 27.550 µs | 22.650 / 25.201 / 28.680 µs | +1.290 / +0.851 / +1.130 µs |
| Python public latest read | 14.900 / 19.860 / 24.530 µs | 14.751 / 19.660 / 24.860 µs | −0.149 / −0.200 / +0.330 µs |
| callback origin → Python latest | 54.540 / 69.611 / 79.410 µs | 51.741 / 66.961 / 82.291 µs | −2.799 / −2.650 / +2.881 µs |

这些结果包含明显的进程间调度/频率噪声；它们支持“没有量级或显著绝对值
退化”，不构成严格的统计等价性证明。

在同一 workload 下交替运行 recovery disabled/enabled 后，FAST-first
边界的 run-median 为：

| callback origin → FAST first visible | disabled | enabled | 差值 |
|---|---:|---:|---:|
| p50 | 9.340 µs | 9.400 µs | +0.060 µs |
| p95 | 11.790 µs | 12.110 µs | +0.320 µs |
| p99 | 15.180 µs | 13.971 µs | −1.209 µs |

这组 FAST-first 微基准使用 instrumented in-process sink，并在
CERTIFIED applied handoff 前打点；它用于隔离默认 observer/wrapper 对第一
发布边界的影响。真实 Wire V2 reader 的证据是上一张 10,000-sample 公共
benchmark 表。两者都不是持续饱和负载下的统计等价性证明。

正常连续数据下 CERTIFIED。这里的 `recv/callback-entry` 是 record 中的
`recv_monotonic_ns`：生产 SDK 路径在 `OnMessage` callback 入口取时；
本基准的 `InjectSdkMessageForTest` 路径在 `Ingest` 入口取时。它不是 NIC
硬件时间戳，也不表示线缆到达时刻。

| 指标 | p50 | p95 | p99 |
|---|---:|---:|---:|
| recv/callback-entry → certified writer stamp | 17.820 µs | 25.910 µs | 30.171 µs |
| recv/callback-entry → reader visible | 19.790 µs | 28.040 µs | 32.110 µs |
| writer stamp → reader visible | 1.380 µs | 3.991 µs | 5.700 µs |
| successful `ReadLatest` call | 0.340 µs | 0.590 µs | 0.700 µs |

回补 benchmark 先制造一个 missing position，再放入 `suffix` 个未来 Tick，
确认 gap 状态与两侧可读性，最后注入 missing Tick；`catch-up` 从该回补
callback 起点量到最后一个已缓冲 Tick 对 reader 可见。每个进程对每个规模
1 次 warm-up + 5 次 measured trial；下表再取 5 个进程结果的中位数：

| buffered suffix | 本次提交记录数 | catch-up p50 | catch-up p95 | p50 吞吐 |
|---:|---:|---:|---:|---:|
| 1 | 2 | 24.870 µs | 31.100 µs | 80,418 Tick/s |
| 32 | 33 | 81.521 µs | 86.420 µs | 404,803 Tick/s |
| 256 | 257 | 525.785 µs | 530.635 µs | 488,792 Tick/s |
| 1,024 | 1,025 | 2.474 ms | 2.501 ms | 414,313 Tick/s |

全部 20 个“规模 × 独立进程”结果都满足：

- `fast_latest_available_during_gap=1`；
- `certified_last_good_available_during_gap=1`。

回补时间呈固定调度成本加近似线性 drain 成本；没有在正常 reader 路径中
支付 reorder 或历史重建成本。

## 9. 明确限制

1. FAST 与 CERTIFIED 仍在同一进程。代码能隔离网络缺口、冲突、queue/
   capacity 和普通 backing 预留失败，但不能把进程崩溃、进程级地址空间
   耗尽或 cgroup/host OOM kill 转成“FAST 绝对继续”。需要这种故障域保证
   时，CERTIFIED worker、mapping 与 Event projector 必须迁到独立进程。
2. 没有文档证明的 replay-complete 信号，因此永久缺口不会超时跳过；
   CERTIFIED 保留 last-good，FAST 继续。
3. feed epoch 只能由可信上层显式切换；序号回退不会自动推断重连或新日。
4. trusted checkpoint 是统一配置，并且只在调用者已经恢复全部下游状态时
   才安全；当前生产 fresh-session 组合不启用 checkpoint。
5. 跨 channel canonical 顺序是本服务发布顺序，不是交易所总序。
6. Event journal 的大容量实现依赖 Linux `memfd`/`fallocate`/mmap 语义以及
   当前受支持编译器 ABI。
7. 对“已认证 key 的重复 observation 已到、但该重复到达尚未完成 canonical
   payload 比较”的短窗口，后续认证会被正确阻挡，但 channel 仍可能显示
   `CONTIGUOUS`。这不改变 last-good 数据正确性；需要把“重复验证进行中”
   作为独立运维状态时，应在下一 ABI minor 增加显式计数/状态。
