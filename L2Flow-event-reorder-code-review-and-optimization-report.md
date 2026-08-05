# L2Flow-event-reorder-33c4f5d 分支代码审查与吞吐优化报告

**审查对象**：`L2Flow-event-reorder-33c4f5d.zip`
**审查日期**：2026-08-05
**审查方式**：代码静态追踪、构建验证、仓库自带微基准运行、数据协议文档交叉核对
**原则**：不修改本分支源码；不以关闭 FAST、CERTIFIED、KLine、Factor、完整历史或 Python/Polars 接口换取性能；不把扩容队列当成顺序问题的修复；所有删除建议区分“可立即删除”和“需运行环境确认后删除”。

---

## 1. 执行结论

本分支真正的主要瓶颈，不在某一个解码函数，也不在 Python 读取端，而在 **CERTIFIED Tick/Event 单线程提交链**：一个 worker 同时承担原生序号协调、重复验证、Wire Tick 二次投影、订单状态查询和更新、Event 生成、Tick/Event 持久内存追加、channel 状态更新、全局状态扫描与共享头发布。该链路中的多个步骤又对同一条 Tick 重复执行，因此即使增加前端 decoder worker，CERTIFIED Event 吞吐也不会按 CPU 核数线性增长。

本次确认的最高优先级问题有两个：

1. **上海 `END/ENDTR` 状态处理存在全市场订单表重复扫描和全局容量高估。** 每只证券结束时都会扫描全局 `orders_`，再筛选当前证券；同时预留和预检的 Event 数量按“全市场全部订单数”计算，而不是当前证券订单数。这既是显著的收盘性能热点，也可能在实际空间仍足够时提前 fail-close。
2. **CERTIFIED worker 每条目标 Tick 至少重复投影一次 336 字节 Wire payload，并在一条串行链中执行多次 map 查找、channel snapshot、全 channel 扫描、header publish 和 generation mutex。** 仓库自带的合成基准中，较长缺口回补的中位吞吐在当前环境约趋近 **0.23M Tick/s**；这与串行提交链的结构相符。

最合理的优化顺序不是“大重写”，而是：

> 先修上海 END 的范围与容量计算；再消除 CERTIFIED 重复投影、重复 generation 获取、无变化 channel 发布和重复 header 发布；随后把订单状态和 canonical payload 改为启动期有界内存；最后才将已排序 Tick 按 instrument 分片进行 Event 计算，并由一个轻量 ordered-commit 阶段统一发布全局 Tick/Event journal。

该设计保留一个单线程 `NativeSequenceRecoveryCoordinatorV1`，因为它负责证明原生连续性；并行化发生在“顺序已经确定之后”。同一 instrument 永远由同一个 Event worker 串行处理，worker 不直接写全局 Event journal，从而既增加吞吐，也不破坏每证券状态机和全局可见顺序。

---

## 2. 审查范围、证据与限制

### 2.1 代码规模

排除厂商 SDK 目录和本次构建目录后：

| 代码面 | 文件数 | 行数 |
|---|---:|---:|
| 生产 C++：`apps/benchmarks/include/src/tools` | 125 | 93,026 |
| C++ 测试 | 58 | 67,028 |
| Python 包 | 30 | 25,838 |
| Python 测试 | 18 | 16,472 |
| 项目文档 | 13 | 7,034 |
| 主 `CMakeLists.txt` | 1 | 591 |

最大的生产文件是：

| 文件 | 行数 |
|---|---:|
| `src/runtime/realtime_pipeline_v1.cpp` | 6,284 |
| `src/ipc/realtime_shared_service_v2.cpp` | 5,783 |
| `src/recovery/mdl_csv_startup_replay_v1.cpp` | 5,282 |
| `src/ipc/realtime_certified_service_v1.cpp` | 4,681 |
| `apps/mdl_production_main.cpp` | 4,353 |
| `src/market/realtime_history_v1.cpp` | 2,898 |
| `src/market/intraday_instrument_store_v1.cpp` | 2,614 |
| `src/market/market_decoder.cpp` | 2,303 |
| `src/realtime/native_sequence_recovery_v1.cpp` | 2,273 |

这些“大文件”本身并不自动等于低性能，但它们把热路径、控制路径、恢复路径和共享内存协议放在同一实现类中，增加了重复逻辑和修改风险。后续建议会将它们按职责拆分成内部 final 组件，但不引入运行时多态或复杂依赖注入。

### 2.2 构建与测试状态

项目对 GNU 编译器明确要求 GCC 13.x，见 `CMakeLists.txt:5-30`；当前环境只有 GCC 14.2，因此未声称完成“官方生产编译器”验证。使用 Clang 17、Release、严格告警配置，已经真实构建成功：

- `mdl-production-router`
- `mdl-order-event-aggregator`
- `benchmark_realtime_certified_v1`
- `benchmark_online_recovery_fast_v1`

构建目录登记了 47 个 CTest 测试，但完整测试构建被测试源码中的 Clang 17 signedness 告警阻断：

- `tests/test_intraday_instrument_store_v1.cpp:455,1005` 等：`size_t` 到 iterator `difference_type`；
- `tests/test_daily_catalog_pipeline_v2.cpp:2274`：`uint64_t` 到 `int64_t`。

所以本报告**没有宣称全部测试通过**。正式合并前必须在 GCC 13 Release、ASan+UBSan、TSan 三个分离构建中完成全量测试。

本次提取后的源码与 ZIP 中源码逐文件 SHA-256 对比，292 个文件无缺失、无新增、无变化；只有独立的 `build-review` 构建目录和 `/mnt/data` 下审查日志。

### 2.3 基准的解释边界

运行的是仓库自带的合成 workload，不是真实全市场实盘容量测试；当前运行未固定 CPU affinity，也未隔离同机噪声。因此：

- 可用于证明某条路径可达、存在可测开销、某个方案并非总是更快；
- 不可直接宣布生产 QPS 上限，也不可据此承诺某个绝对延迟。

---

## 3. 数据协议基线：哪些顺序约束不能被“优化掉”

### 3.1 上海

当前生产使用 `4.101.24` 合并逐笔。其 `BizIndex` 从 1 开始，按 `Channel` 连续；同一产品的逐笔类消息在同一通道发布。连续竞价中，同一主动委托会先发成交，若有首次撮合后的剩余量，再发新增委托；完全成交则可能没有新增委托消息。因此上海 Event 重建必须保留 `T → A` 的特殊顺序，不能在下游按到达时间重新猜测。

SDK 版本说明还明确：新版上海 4.24 上线后，旧 4.18/4.19 下线。因此，生产路径继续保留旧上海逐笔成交/委托解析没有业务价值；但当前仓库中的旧结构主要位于 mock 和 ABI 回归测试，不在生产订阅中。

### 3.2 深圳

当前生产订阅 `6.101.33` 逐笔委托和 `6.101.36` 逐笔成交。两种消息都有 `(ChannelNo, ApplSeqNum)`；本分支将二者放入同一个 native sequence domain，以解决两个 callback 家族跨回调到达顺序不一致的问题。这一顺序必须在 Event 状态机之前恢复，不能把乱序留给 instrument worker，也不能在 Event 已经生成后再排序。

新版 SDK 文档新增了 `6.101.53` CombinedTick，但没有说明 6.33/6.36 已下线。当前代码显式订阅五个 tuple：

```text
4.101.4   上海快照
4.101.24  上海合并逐笔
6.101.28  深圳快照
6.101.33  深圳逐笔委托
6.101.36  深圳逐笔成交
```

并把 `6.101.53` 标记为 forbidden，见：

- `include/l2flow/sdk/market_message_catalog_v1.h:18-33`
- `include/l2flow/sdk/production_subscription_v1.h:15-33`
- `src/runtime/realtime_pipeline_v1.cpp:2662-2672`

这不是无用逻辑，而是防止 6.33/6.36 与 6.53 同时订阅造成重复业务流。除非生产数据源正式迁移为“只订阅 6.53”，否则不能删除 6.33/6.36 reorder/coordinator。

---

## 4. 当前执行路径与瓶颈位置

```text
SDK callback
  │
  ├─ admission_mutex：检查、提取证券键、filter、catalog、分配顺序号、复制 body
  │
  ├─ 4 条 source decoder lane / 可选 stateless decoder farm
  │
  ├─ instrument_id 分片的 Store worker
  │      ├─ Intraday Store
  │      ├─ KLine
  │      └─ FAST shared-memory latest/ring
  │
  └─ native observation + applied record pointer
         │
         ▼
  大型有界 MPMC handoff queue
         │
         ▼
  单个 CERTIFIED worker
         ├─ NativeSequenceRecoveryCoordinatorV1
         ├─ 再次投影 Wire Tick
         ├─ 上海/深圳订单状态机
         ├─ Event journal / Tick ring / latest
         ├─ channel snapshot/map
         └─ 全 channel aggregate + header publish
```

FAST 的主要路径已按 instrument 分片，CERTIFIED Event 的核心仍是单 worker。因此，前端 decoder 并行只能缓解“解码不足”，不能消除 Event 状态机、Event journal 和全局 commit 的串行部分。

---

## 5. 热点优先级总表

| 优先级 | 已确认问题 | 主要后果 | 首选修改 |
|---|---|---|---|
| P0 | 上海 END 扫描全局 `orders_`，容量按全市场订单数预检 | 收盘复杂度接近 `证券数 × 全市场订单数`；可能误报容量不足 | 使用现有有序 key 的 instrument range；按当前证券精确计数 |
| P1 | CERTIFIED 单 worker 集中承担排序、双投影、Event、共享内存和 header | 回补吞吐受单核串行链限制 | 先消除重复工作，再做 instrument Event sharding + ordered commit |
| P1 | 上海/深圳订单状态用 `std::map` | 每个新订单堆分配、`O(log N)`、缓存局部性差 | 先用有界节点池保序；后续固定容量 hash + 确定性有序索引 |
| P1 | coordinator 每 entry 使用 `std::vector<byte>` | 首次 apply 分配 336B，release 时释放；高频 malloc/free | 固定 payload slab/inline production storage |
| P1 | 每 Tick 多次 `EnsureChannel/UpdateChannel`，每 publish 扫全 channel | channel 数增加后每 Tick 成本线性增长 | 固定 channel table、变更检测、增量 aggregate counter |
| P1 | `AppendCertifiedTick` 后立即 `AcquireGeneration` 再锁一次 | 同一 writer 每 Tick 重复 publication mutex | Append 直接返回该 Tick 对应 generation 元数据 |
| P1 | 每次 queue push 都 `notify_one`；Drain 后外层再 publish header | 无效 RMW、唤醒和重复共享头提交 | 空→非空/armed waiter 通知；返回“已提交”状态 |
| P1 | callback 内二次 inspection，且 admission mutex 范围过宽 | 重复解析；阻止未来 callback/replay 并发 | `IngestInspected`；锁外做只读解析，锁内只线性化序号和 enqueue commit |
| P1/P2 | CERTIFIED handoff 默认 4,194,304 cells | 当前 ABI 仅队列约 288 MiB；掩盖处理速度不足 | 按 backlog SLA 定容；先提升消费能力，不再扩大队列 |
| P2 | FAST 全局 tick ring 的 prefix CAS/slot lock | 多 Store worker 同时发布时有全局 cache-line 竞争 | 先 profile；必要时 completion bitmap + 单 prefix owner |
| P2 | History handoff slot 懒分配，Store segment 运行中分配 | 启动后的流量爬坡有 allocator 抖动 | source-specific 小对象池；tick pool 预热，snapshot pool 单独定容；Store segment pool |
| P2 | Python/Polars 固定做 owned copy | 大批历史读取有内存带宽和 Python 对象开销 | 增大 batch；只对 pinned immutable page 使用 Arrow C Data 零拷贝 |
| P2 | 可选 parallel decoder farm 效果不稳定 | 增加线程和逻辑但不保证更快 | 继续默认关闭；以真实 backlog/affinity 基准决定启用或构建隔离 |

---

## 6. 详细发现与修改方案

## 6.1 P0：上海 END 路径既慢，又可能提前 fail-close

### 代码证据

上海订单 key 的比较顺序为：

```text
(trade_date, instrument_id, channel, order_id)
```

见 `src/market/shanghai_order_event_aggregator_v1.cpp:476-488`。也就是说，同一证券、同一 channel 的订单在 `std::map` 中本来就连续排列。

但 `ConsumeStatus()` 在 END 时：

- `reserve(1 + orders_.size())`；
- 遍历全部 `orders_`；
- 通过 `SameInstrument()` 再筛选当前证券。

见 `src/market/shanghai_order_event_aggregator_v1.cpp:1119-1160`。`orders_` 是全局 `std::map`，见 `:1183-1186`。

上层 `CertifiedOrderEventHistoryV1` 又把 END 的 `maximum_output` 算成 `shanghai_->order_count() + 1`，并用该值做 Event 容量和映射空间预检，见 `src/ipc/certified_order_event_history_v1.cpp:526-578`。

### 实际影响

假设全市场有 `N` 个订单状态、`M` 只证券，每只证券都有一个 END 状态。如果每次 END 都扫 `N`，总比较量近似 `M × N`。正确的工作量应只与当前证券订单数 `n_i` 成正比，全天合计接近 `Σn_i = N`。

`reserve(1 + orders_.size())` 不一定每个 END 都重新分配，因为 vector 会复用容量；但第一笔大 reserve 会让一个临时 vector 膨胀到全市场规模。更严重的是上层 capacity preflight：当前证券只需 `1 + n_i` 行时，代码却要求剩余空间容纳 `1 + N` 行，可能在实际可完成当前 Tick 时提前进入 `kEventCapacity` 或 `kResourceExhausted`。

这属于**可用性错误**：它不会写出错误 Event，但会过早冻结 CERTIFIED 的正确前缀。

### 最小且安全的修改

利用现有 map 顺序做范围查询，不改变状态模型，也不改变 Event 排序：

```cpp
lower = (date, instrument_id, channel, min_order_id)
upper = (date, instrument_id, channel, max_order_id)
[first, last) = orders_.lower_bound(lower), orders_.upper_bound(upper)
```

然后只遍历 `[first,last)`。因为头文件明确要求 revision 按 ascending `OrderKey` 输出，见 `include/l2flow/market/shanghai_order_event_aggregator_v1.h:327-334`，使用 map range 会自然保留该契约。

建议增加一个不修改状态的接口，例如：

```text
CountOrdersForInstrument(input)
MaximumOutputForInput(input)
```

`CertifiedOrderEventHistoryV1` 在物理空间预检前调用精确值。最小实现可以对 range 做一次 `distance`，再做一次 finalize，成本为约 `2 × n_i`；随后可维护 `instrument_order_count` 将计数降为 O(1)。

### 不应采用的修改

不能简单把 `std::map` 换成 `unordered_map` 后直接遍历，因为这会改变 END revision 的确定性顺序，破坏头文件的公开契约和 digest 稳定性。哈希表优化必须同时维护每 instrument 的确定性有序索引，或在预分配缓冲区中按 OrderKey 排序。

---

## 6.2 P1：CERTIFIED worker 重复投影同一 Tick，并串行完成过多职责

### 代码证据

每条目标 Tick 通常向同一个 MPMC queue 写入两类 handoff：

- Store 应用完成后的 `Applied`，见 `src/ipc/realtime_certified_service_v1.cpp:1845-1877`；
- native `Observation`，见 `:1906-1938`。

`HandleApplied()` 第一次调用 `ProjectRealtimeWireTickPayloadV2()`，构造 336B payload，并生成 canonical copy 给 coordinator，见 `:3039-3093`。

等 `PollCertified()` 返回 ready token 后，`DrainCertified()` 又从同一个 immutable Store record **第二次**调用 `ProjectRealtimeWireTickPayloadV2()`，见 `:3133-3157`。随后同一个线程依次执行：

1. `CommitCertified()`；
2. `event_history_->AppendCertifiedTick()`；
3. `event_history_->AcquireGeneration()`；
4. publish ring 和 latest；
5. `EnsureChannel()`、map find、`UpdateChannel()`；
6. `PublishHeader(DeriveAggregateState())`；
7. 再读 header 验证 frontier；
8. 锁 `committed_event_generation_mutex_`。

见 `src/ipc/realtime_certified_service_v1.cpp:3133-3270`。

Worker 主循环即使 `DrainCertified()` 已经提交过 header，外层仍会再次 `DeriveAggregateState()` 和 `PublishHeader()`，见 `:2746-2810`。

### 基准证据

`benchmark_realtime_certified_v1` 在当前环境的合成 Shanghai 单证券 workload 中：

| 指标 | recovery off | recovery on | 增量 |
|---|---:|---:|---:|
| callback p50 | 9.872 µs | 15.590 µs | +5.718 µs |
| callback p95 | 14.688 µs | 23.202 µs | +8.514 µs |
| callback → FAST reader p50 | 37.454 µs | 41.280 µs | +3.826 µs |

正常 CERTIFIED 路径中：

- callback p50：18.830 µs；
- callback → CERTIFIED reader visible p50/p95：49.499 / 80.058 µs。

缺口回补中位吞吐：

| catch-up Tick 数 | p50 吞吐 |
|---:|---:|
| 33 | 163,584 Tick/s |
| 257 | 208,060 Tick/s |
| 1,025 | 233,953 Tick/s |

这不能被解释为生产上限，但足以说明 CERTIFIED 串行链有可测成本，且较长 catch-up 在本 harness 中趋近约 0.23M Tick/s。

### 第一阶段：不改变线程拓扑的低风险优化

#### A. payload lease，只投影一次

在 CERTIFIED 侧预分配固定 payload slab。`HandleApplied()`：

1. 从 slab 获取 lease；
2. 把 Store record 投影为一次完整 `RealtimeWireTickPayloadV2`；
3. coordinator 仍保存严格 canonical bytes 用于重复验证；
4. `applied_cookie` 指向 payload lease，而不是原始 record；
5. ready 后直接读取 lease，不再二次投影；
6. commit 完成或 duplicate 被判定后归还 lease。

不能把完整 payload 的构造搬回 SDK callback；这会增加 FAST 回调延迟。lease 的申请、投影和 canonicalization 都应继续发生在 CERTIFIED worker 侧。

#### B. Append 直接返回 generation

`CertifiedOrderEventHistoryV1::Publish()` 已经在 writer 线程构造了完整 generation，见 `src/ipc/certified_order_event_history_v1.cpp:791-825`。但调用者随后通过 `AcquireGeneration()` 再锁一次同一个 `publication_mutex_`，见 `:455-462`、`:993-1008`。

将接口改为：

```text
AppendCertifiedTick(input, canonical_seq, out_generation)
```

writer 直接返回刚发布的 generation。外部读者的锁语义保持不变，单 writer 不再为同一 Tick 锁两次。

#### C. 合并无效 wake 和重复 header publish

`WakeWorker()` 对每次 push 都 `fetch_add + notify_one`，见 `src/ipc/realtime_certified_service_v1.cpp:2723-2725`。改成“queue 从空变非空”或“worker 已 armed 等待”时才通知，使用单调 epoch 防止 lost wake。

`DrainCertified()` 返回：

```text
made_progress
published_header
state_dirty_after_last_publish
```

外层 WorkerLoop 只有在确有 barrier、seal、freeze 或未提交状态变更时才再次 PublishHeader。

#### D. channel 变更检测

当前 `UpdateChannel()` 每次成功 snapshot 后都 `wire_dirty=true`，即使 snapshot 没变，见 `src/ipc/realtime_certified_service_v1.cpp:3291-3353`。应先比较旧/新 snapshot，只在状态、frontier、计数或 gap 标志变化时置 dirty。

### 第二阶段：真正并行化 Event 投影

只在完成上述降常数优化后，引入 instrument worker。否则并行 worker 只会更快地把压力推给仍然 O(channel) 的 global commit。

推荐拓扑：

```text
NativeSequenceRecoveryCoordinatorV1（单线程）
       │ 已证明的 next native position
       ├─ Filtered → 直接写 zero-output completion
       └─ Target   → CertifiedReadyTask
                        │
                        ├─ shard 0：instrument_id % N == 0
                        ├─ shard 1：instrument_id % N == 1
                        ├─ ...
                        └─ shard N-1

Event worker：同一 instrument 唯一 owner，串行更新订单状态
       │ 生成无全局 derived sequence 的 EventBatchLease
       ▼
Ordered completion ring[canonical_apply_sequence]
       ▼
单一 ordered commit owner
       ├─ 等待 next canonical sequence 完成
       ├─ 统一分配 dense derived_event_sequence
       ├─ 追加全局 Tick/Event journal
       ├─ 更新 latest/channel/header
       └─ 释放 payload/event batch lease
```

关键约束：

- coordinator 仍是单线程；它只证明顺序，不做重型 Event 状态计算；
- 同一 instrument 永远落到同一个 worker；
- worker 不能直接写全局 Event journal；否则完成顺序会替代 canonical 顺序；
- completion ring 必须有界。满时 CERTIFIED fail-close 在最后正确前缀，FAST 继续；
- filtered native position 也必须生成 zero-output completion，以推进 canonical frontier；
- worker 输出 batch 内部顺序保留上海 ascending OrderKey 和深圳 source-event/revision 顺序；
- ordered commit 统一赋值 Event 全局序号，避免 worker 完成快慢改变 journal 身份；
- 当前代码已经在 Event 物理写入失败时允许 coordinator 内部 token 已提交、但公开 frontier 保留最后正确值。因此“先 claim/内部 commit，再并行计算，公开 commit 后移”与现有 fail-close 事务方向一致；但必须以 bounded completion window 限制私有未公开后缀。

### 可预期的加速边界

并行收益受 ordered commit、journal copy 和 header publish 的串行比例限制。按 Amdahl 定律：

```text
S(N) = 1 / (s + (1-s)/N)
```

4 个 Event worker 时：

- 若串行比例 `s=0.4`，理论上限约 1.82×；
- 若 `s=0.2`，理论上限约 2.50×。

这只是容量规划公式，不是性能承诺。只有先把 header/channel 维护降为 O(1)，串行比例才可能足够低。

---

## 6.3 P1：订单状态的 `std::map` 是持续的 allocator 和缓存热点

### 代码证据

上海：

- 新状态通过 `orders_.emplace()`，见 `src/market/shanghai_order_event_aggregator_v1.cpp:742-751`；
- 全日状态保存在 `std::map<ShanghaiOrderKeyV1, OrderState>`，见 `:1183-1186`。

深圳：

- `std::map<ShenzhenOrderKeyV1, OrderState>`，见 `src/market/shenzhen_order_event_projector_v1.cpp:539-548`；
- 新委托 `find + emplace`，见 `:703-743`。

生产把 `maximum_order_states` 直接设为 `intraday_store_maximum_records`，见 `apps/mdl_production_main.cpp:3893-3914`，默认配置层的上限为 1,000,000，见 `include/l2flow/ipc/realtime_certified_service_v1.h:144-148`。

### 影响

每个新订单至少产生一棵红黑树节点的独立分配，并进行 `O(log N)` 比较和多次指针跳转。Event worker 是单线程，因此这些 heap/缓存开销直接进入尾延迟。订单状态全日不 erase，allocator 的释放收益很低。

### 两步修改，避免一次性改变所有语义

#### 过渡方案：有界节点池，保留 `std::map`

给 `std::map` 使用启动期分配的固定 node arena/custom allocator：

- 不改变查找、排序和 END 输出顺序；
- 消除稳态 `new`；
- 风险小，适合作为机械性能改动。

因为状态全日不 erase，可使用 monotonic/bump 风格的有界节点池；达到上限仍返回现有 `kOrderCapacity`，不能 fallback 到系统堆。

#### 目标方案：固定容量 open-addressing table + 稳定 slab

- key → 状态 slot 用 robin-hood/open addressing；
- 状态实体保存在连续 slab；
- 每 instrument 维护确定性 order index，用于上海 END ascending OrderKey；
- 预留容量在启动期完成；
- 不 rehash，不在热路径分配；
- load factor 达阈值直接 fail-close，而不是动态扩容。

这会把平均查找从树的 `O(log N)` 降为接近 O(1)，并显著改善 cache locality，但属于语义敏感改动，应在节点池方案稳定后实施。

---

## 6.4 P1：NativeSequence coordinator 的 payload 和扫描仍有线性开销

### canonical payload 每 entry 分配

`Entry` 包含 `std::vector<std::byte> canonical_payload`，见 `src/realtime/native_sequence_recovery_v1.cpp:481-498`。首次 application 使用 `assign()`，捕获 `bad_alloc`，见 `:1225-1275`；entry 释放时通过空 vector `swap` 释放，见 `:710-742`。

生产 `RealtimeWireTickPayloadV2` 固定为 336B，见 `include/l2flow/ipc/realtime_wire_v2.h:455-486`。因此生产链没有必要让每个 entry 单独 malloc/free。

**修改**：coordinator 保持通用接口，但 production adapter 使用预分配 fixed-size payload slab，entry 只保存 `{slot_id, payload_length}`。若保留通用可变 payload 测试，可把 storage policy 模板化或通过内部 final backend 分离，公开 API 不变。

### duplicate blocking 扫全 entry 表

`HasBlockingDuplicateVerification()` 在 channel 有未验证 duplicate 时遍历所有 `entries_`，见 `src/realtime/native_sequence_recovery_v1.cpp:775-790`。正常无 duplicate 时是 O(1)，但 duplicate storm 或 recovery 重复回放时，`PollCertified()` 可能反复触发 O(entry_capacity) 扫描。

**修改**：每 channel 维护 unresolved duplicate 的最小序号/frontier，以及有界 intrusive list 或小根结构。observe/apply/release 时增量更新；`PollCertified()` 只比较 `lowest_unverified_sequence <= next_sequence`。

### PollCertified 每次轮询全部 channel

`PollCertified()` 从 round-robin cursor 开始，最多检查全部 active channel，见 `src/realtime/native_sequence_recovery_v1.cpp:1991-2027`。公平性是必要的，但“每次输出一条 Tick 都扫 channel”不是唯一实现。

**修改**：维护 ready-channel bitmap/queue。channel 从 not-ready 变 ready 时入队；每次只 claim 一条，再在仍 ready 时放回队尾，保留 round-robin 公平。channel freeze/seal/duplicate frontier 变化时更新 ready 状态。

`FreezeChannel()` 全扫 entries，见 `:870-885`，属于失败冷路径，优先级较低；先不要为了优化异常路径增加正常路径复杂度。

---

## 6.5 P1：channel map 和共享头发布把每 Tick 变成 O(active_channels)

### 代码证据

- `channels_` 是 `std::map`，见 `src/ipc/realtime_certified_service_v1.cpp:4284-4288`；
- `EnsureChannel()` 先 find，再 emplace，见 `:3274-3289`；
- `UpdateChannel()` 再 find、向 coordinator 取 snapshot，并总是 dirty，见 `:3291-3353`；
- `DeriveAggregateState()` 遍历全部 channel，见 `:3481-3536`；
- `PublishHeader()` 再取 recovery snapshot、再次遍历全部 channel计算 duplicate/gap/catching/frozen，然后发布 dirty row 和 header，见 `:3539-3703`。

### 修改

1. 使用启动时固定容量的 dense/open-addressing channel table，`EnsureChannel()` 返回稳定 `channel_slot/row_index`；后续 handoff 和 ready token 缓存 slot，不重复 map find。
2. `UpdateChannel()` 只在 snapshot 有变化时 dirty。
3. 状态变更时增量维护：
   - healthy / gap / catching / frozen channel 数；
   - duplicate 总数；
   - pending 总数；
   - gap opened/recovered 计数。
4. `DeriveAggregateState()` 由计数直接 O(1) 决策。
5. `PublishHeader()` 只遍历 dirty channel row；全局字段使用缓存计数。
6. `ReadMonotonicNs()` 可保留。它提供 public heartbeat 语义，通常走 vDSO；在其他线性工作消除前不是主要问题。

需要保持当前 seqcount/odd-even tag 的读者一致性，不可为了少一次 store 放弃 coherent header cut。

---

## 6.6 P1：callback admission 存在双重 inspection，mutex 范围过宽

### 代码证据

online capture 开启时，`CaptureAndIngestLive()` 先调用一次 `InspectOwnedIngressMessageV1()`，见 `src/runtime/realtime_pipeline_v1.cpp:2577-2595`；capture 成功后又调用 `Ingest(message, ...)`，而 `Ingest()` 在拿到 `admission_mutex_` 后再次 inspection，见 `:2632-2664`。

该 mutex 从 `:2640` 持有到成功路径 `:3006`，覆盖：

- clock/trade-date；
- exact instrument key 提取；
- native sequence 提取；
- A-share filter；
- daily catalog lookup；
- sequence candidate；
- ingress pool `Acquire` 和 body copy；
- decoder queue push/commit；
- native observation；
- progress publication request。

### 影响边界

当前文档配置 SDK `multithread_callback=false`，因此普通实盘 callback 本身通常串行，mutex 竞争不一定是当前第一大热点。问题在于：

- 即使无竞争，也有重复 inspection；
- online CSV/replay 或未来多 callback 线程无法并发做只读解析；
- 一个较慢 catalog/body copy 会扩大所有 producer 的串行区。

### 最小修改

增加内部入口：

```text
IngestInspected(inspection, clocks, ...)
```

Capture 路径复用首次 inspection，不再重复解析。

随后把临界区拆为：

**锁外**：只读且不改变全局顺序的工作

- inspection；
- clock read 和 trade-date candidate；
- exact instrument key/native descriptor；
- A-share filter；
- immutable catalog lookup；
- 可预留但不 commit 的 pooled body lease。

**锁内**：必须线性化的工作

- recheck fatal/accepting/trade-date cut；
- 分配 global/source/tick sequence；
- 将 metadata 写入 lease；
- queue `TryPushWithCommit`；
- accepted frontier 和 native observation 的 commit 顺序。

不能先在锁外正式推进 sequence；filtered callback 也必须遵守现有“占 native 位置但不占 global ingress sequence”的语义。不能为普通 SDK callback增加等待式 backpressure。

---

## 6.7 P1/P2：默认 CERTIFIED queue 很大，但大队列不是吞吐修复

`apps/mdl_production_main.cpp:171` 默认 `certified_handoff_queue_records = 4,194,304`。当前 x86_64 / Clang 17 / libstdc++ ABI 实测：

```text
sizeof(NativeSequenceObservationV1) = 32 B
sizeof(HandoffEvent)                = 64 B
sizeof(MPMC Cell)                   = 72 B
4,194,304 cells                     = 301,989,888 B = 288 MiB
```

`BoundedMpmcQueue` 构造时创建全部 cell，并循环初始化每个 sequence，见 `src/ipc/realtime_certified_service_v1.cpp:248-258`。这还没有包含 Tick ring、Event journal、order state、coordinator entries 或 Store。

多数 tracked target Tick 会产生 Observation + Applied 两个 queue item，所以 queue 的“Tick backlog 能力”通常约为 cell 数的一半。扩大 queue 只延后 frozen，不增加消费速度，而且增加 resident/cache footprint。

### 修改

把容量拆开并按真实 high-water/SLA 定义：

- observation queue/window；
- applied payload lease 数；
- completion window；
- maximum pending native entries；
- maximum live orders；
- maximum derived Event rows；
- retained certified Tick 数。

当前 production 将 `maximum_order_states = intraday_store_maximum_records`、`maximum_derived_events = 4 × records`、`maximum_certified_ticks = records`，见 `apps/mdl_production_main.cpp:3893-3916`。这三个量的业务含义不同，应由实盘统计分别定容。

建议增加指标：queue current/high-water、oldest handoff age、coordinator pending、completion window occupancy、Event worker backlog、commit lag。容量调整必须基于这些指标，而不是“发生问题就再乘二”。

---

## 6.8 P2：FAST 全局 ring 有共享 cache-line 竞争，但必须最后处理

FAST `PublishApplied()` 在 Tick 路径调用 `PublishRing()` 和 latest slot，见 `src/ipc/realtime_shared_service_v2.cpp:2327-2417`。

`PublishRing()` 使用：

- 全局 contiguous/highest atomics；
- 每 ring cell `atomic_flag`；
- CAS 推进 contiguous prefix；
- 必要时 eventfd 请求控制线程继续推进。

见 `src/ipc/realtime_shared_service_v2.cpp:3503-3645`。

这些操作有明确正确性原因：Store workers 可并发完成不同 global tick sequence，需要一个连续可读前缀。不能把 FAST 改成每 worker 独立可见后再让 Python 合并，否则会改变现有 callback-order Wire 语义。

只有 perf/PMU 证明其成为热点后，才考虑：

- 每 worker 写预分配 completion bitmap/slot；
- 一个轻量 prefix owner 更新 contiguous frontier；
- 将不同 worker 的 completion flag 分散到独立 cache line；
- 保留现有 happens-before、overrun 和 fail-closed 证明。

本轮不建议先动 FAST ring，因为用户最关注的吞吐瓶颈更明确地位于 CERTIFIED Event 链，且 FAST 是最敏感的低延迟路径。

---

## 6.9 P2：History 和 Intraday Store 的运行时分配

### History handoff pool

`HistoryHandoffPool` 的 queue 只传一个 pointer，但 Slot 内嵌完整 `RealtimeHistoryEventInputV1`。当前 ABI 该 input 约 3,696B。pool 构造只 reserve 指针 vector；第一次达到新 high-water 时逐个 `new Slot`，见 `src/market/realtime_history_v1.cpp:320-421`。

默认有 `4 source × 4 Store worker` 个 pool，每个逻辑容量 32,768。把全部容量一次性预分配会非常大，因此不应简单“全量 preallocate 16 个池”。

更合理的修改：

1. 按 source 类型拆 pool：高频 Tick 使用紧凑 Tick input slot；低频 Snapshot 使用较大的独立 slot；
2. Tick pool 在 callback gate 打开前按实测 high-water 预热；
3. 保留有界的冷备用扩展，或在生产严格模式下预热不足直接启动失败；
4. 记录 pool allocated/high-water/failed acquire。

`BeginSubmission/EndSubmission` 每条消息做 CAS/fetch_sub，见 `src/market/realtime_history_v1.cpp:1029-1058`。它保护 generation cut，不能直接删除。若 profiling 证明明显，可在 source owner 串行前提下做 owner-local batch epoch gate，但必须证明 Stop/Cut 的 happens-before。

### Intraday Store segment

Store 新 segment 通过 `::operator new` 分配，见 `src/market/intraday_instrument_store_v1.cpp:264-275`、`:1991-2051`。quota 已采用 worker-local block credit，只有 refill/reclaim 才锁全局 mutex，见 `:532-621`，这一设计总体合理。

建议加入启动期 segment pool/每 worker bounded freelist，使新 segment 不调用系统 allocator。每条 append 的 accounting atomics（`:2133-2137`、`:2153-2156`）可改为 owner-local counter，在 generation/metrics cut 时发布；但 hard byte/record cap 仍由 block-credit authority 保证。

Row capture mutex 只在某 generation 第一次触发时进入，见 `:2073-2089`，不是每 Tick 热点，不应错误删除。

---

## 6.10 P2：Python/Polars 的 copy 多数是正确性成本，不是无用代码

`raw_event_batch_frame()` 明确把 worker ring column 复制进 owned C buffer，再构建 Polars Series，见 `python/l2flow_realtime/polars.py:305-377`。FAST tail 同样复制，避免 DataFrame 别名可被覆盖的 mutable ring，见 `polars_fast_tick_history.py:81-94`。

因此不能直接做“零拷贝 mutable ring → Polars”，否则用户持有的 DataFrame 可能被后续行情覆盖。

可做的优化：

- 增大 C/Python batch，摊薄 Python object/Series 构造；
- immutable History page 或被 pin 的 journal page 通过 Arrow C Data Interface 零拷贝；
- mutable live ring 继续 owned copy；
- `_wire_suffix()` 当前 Python 逐条扫描，见 `polars_fast_tick_history.py:97-107`，可移到 C++/C ABI，仅用于 overrun/reconcile 冷路径；
- 保留 `pl.concat(..., rechunk=False)` 和阈值式 compact；周期 rechunk 是控制 chunk 数的必要成本，不是死代码。

C++ hot path 优化完成前，Python 不是第一优先级。

---

## 6.11 parallel decoder farm：可达，但不是“开启即提速”

该功能不是死代码：

- 配置入口 `include/l2flow/runtime/realtime_pipeline_v1.h:83-101`；
- CLI/production wiring `apps/mdl_production_main.cpp:164-170,1233-1240,3746-3753`；
- runtime 中有完整 issue/parse/completion/committer 路径和指标。

生产默认 `parallel_decoder_workers=0`。本次合成 benchmark 的结果混合：

| 模式 | workers=0 | workers=4 | 解释 |
|---|---:|---:|---|
| ordinary callback p50 | 10.203 µs | 11.304 µs | 4 worker 更慢 |
| ordinary callback→preview p50 | 32.446 µs | 23.442 µs | preview 更快 |
| parked callback p50 | 24.150 µs | 15.694 µs | callback 更快 |
| parked recovery throughput | 81,234/s | 77,644/s | 总恢复略慢 |
| active callback p50/p95 | 8.394/23.333 µs | 9.894/34.226 µs | 4 worker 尾延迟更差 |
| active recovery throughput | 53,602/s | 50,386/s | 总恢复略慢 |

所有 history count 均完整，但这组数据说明：decoder farm 是一个 workload/CPU placement 取舍，不是普适优化。建议继续默认关闭，只有 source queue 持续超过 activation depth，且固定 affinity 的实盘 replay 证明 p99 和吞吐同时达标时再启用。

若运维从不启用，可先用 `L2FLOW_BUILD_PARALLEL_DECODER_FARM` 把实现隔离出默认生产构建，而不是直接删除。删除需要同时移除 CLI、文档、指标和相关测试。

---

## 7. 逻辑冗余与删除性重构评估

## 7.1 可立即删除

### `.tmp_instrument_full_read_probe.cpp`

- 根目录约 55KB；
- 不在 CMake target；
- 没有源码 include/call；
- 仅在 `docs/content-leakage-audit-20260728.md` 中被描述为未构建、只读临时 probe。

可直接删除，并更新 audit 文档中的工作树记录。这是本次唯一可以在不查询外部部署的情况下高置信立即删除的源码文件。

## 7.2 可在外部兼容确认后删除

### `include/l2flow/market/observed_instrument_directory_v2.h`

该文件只有一个 deprecated compatibility include，且仓库内无引用。删除前仍需确认外部 C++ 用户没有直接 include 该路径。稳妥做法是先在一个 release 中加编译期 deprecation/message，再在下一 ABI 窗口删除。

### `mdl-order-event-aggregator` standalone 工具

README 明确说明：它只是 ordered-input replay/diagnostic，production router 不启动、不 gate、不 advertise，见 `README.md:627-650`。但它仍是默认 Linux CMake target，且有进程级测试。

建议先增加：

```text
L2FLOW_BUILD_ORDER_EVENT_DIAGNOSTIC=OFF（生产默认）
```

只有确认 systemd、容器镜像、运维脚本、回放工具链和外部用户均未使用后，才删除 executable、delta control/ring 的专用表面。其核心 order projector 不能删除，因为 CERTIFIED 和 per-instrument history 仍使用。

### 旧上海 `SHL2Transaction` / `SHL2Transaction2`

生产订阅和 production decoder 不使用；仓库引用只在 `src/l2_mock.cpp` 和旧 ABI/mock tests。厂商文档已说明旧上海逐笔成交/委托下线。

如果不再需要“旧 SDK ABI/mocked feed 回归”，可从 mock descriptor、message test 和 ABI expected list 中删除；更稳妥的是放入 `L2MOCK_ENABLE_LEGACY_SHANGHAI_TICKS`，默认 OFF。不要改厂商 SDK header 本身。

## 7.3 当前不能删除

- `NativeSequenceRecoveryCoordinatorV1` 和 6.33/6.36 shared-domain path：这是现场乱序问题的正确性边界；
- `6.53 forbidden` 检查：防止 split+combined 双流；
- filtered native continuity token：被 filter 的证券不产出 A-share Tick/Event，但仍占原生序号；
- `src/sdk_compat.cpp`、`src/l2_mock.cpp`：mock 构建和 ABI 测试使用；
- online recovery live journal：可选但生产可达，且 callback capture 语义必要；
- KLine/Factor：它们消费完整 Store/CERTIFIED 历史，不应为 Event 并行化而关闭；
- Python owned copy：mutable ring 生命周期保护；
- fail-closed 校验、seqcount、capacity preflight：可以降成本，不能整体删除。

## 7.4 可合并的重复编排

至少三处代码围绕同一上海/深圳 core 重复执行“Wire Tick → market input → core consume → derived rows”：

- `CertifiedOrderEventHistoryV1`
- `OrderEventLiveAggregationEngineV1`
- `InstrumentDerivedEventHistoryV1`

证据分别见：

- `src/ipc/certified_order_event_history_v1.cpp:500-745`
- `src/ipc/order_event_live_aggregation_engine_v1.cpp:496-617`
- `src/ipc/instrument_derived_event_history_v1.cpp:495-546`

建议抽出内部无虚函数组件：

```text
OrderedTickEventProjectorV1
  input: Wire Tick + ordering mode/canonical sequence
  output: EventBatch（不负责具体 journal）
  policy: Shanghai/深圳 core、容量上界、错误映射、finalization
```

不同产品只保留：

- 输入顺序 guard；
- publication sink；
- coverage/frontier 协议；
- reader-facing API。

这能减少错误映射和容量逻辑重复，但不能把 FAST callback-order standalone 与 CERTIFIED canonical order 混成同一个运行模式。

## 7.5 删除前必须完成的判定

对任何“看起来没用”的接口，至少检查：

1. CMake target 和 compile definition；
2. public header include graph；
3. exported C ABI / Python ctypes symbol；
4. CLI、配置文件和环境变量；
5. systemd、容器 entrypoint、运维脚本；
6. `dlopen/dlsym` 或插件动态引用；
7. ABI、mock、replay、fuzz 测试；
8. 下游项目和 release deprecation 窗口。

仅凭仓库内 `grep` 不能证明外部 ABI 无人使用。

---

## 8. 建议的分阶段实施计划

## 阶段 0：建立可重复基线，不改语义

新增或补齐指标：

- callback、FAST visible、CERTIFIED visible p50/p95/p99；
- source decoder queue high-water；
- CERTIFIED handoff queue high-water和最老 age；
- coordinator pending/retained/unverified duplicate；
- payload/order/segment allocator calls；
- Event rows per Tick 分布；
- active channel 数；
- `PublishHeader`、wire projection、order projector、journal append 的 CPU 时间；
- Event worker/ordered commit lag。

在固定硬件、固定 CPU affinity、固定 governor、固定 NUMA placement 下保存基线。实盘 replay 必须同时含深圳 6.33 和 6.36，并包含跨 callback 乱序、重复、gap 和 filtered positions。

## 阶段 1：低风险、高收益、无 Wire 变化

1. 上海 END 改为 instrument range；capacity preflight 使用精确当前证券上界；
2. `CaptureAndIngestLive` 复用 inspection；
3. `AppendCertifiedTick` 直接返回 generation；
4. 消除 Drain 后无条件重复 header publish；
5. channel snapshot 变更检测，aggregate counter 增量维护；
6. worker wake coalescing；
7. 独立配置 `maximum_live_orders / maximum_events / maximum_ticks / queue_depth`；
8. 删除 `.tmp_instrument_full_read_probe.cpp`。

该阶段应单独提交，便于按改动回滚；不要同时更换容器和线程拓扑。

## 阶段 2：有界内存与 cache locality

1. coordinator canonical payload fixed slab；
2. CERTIFIED full payload lease，消除二次投影；
3. 上海/深圳 `std::map` 先换有界 node arena；
4. 再通过独立提交换 fixed hash + ordered instrument index；
5. History 拆 tick/snapshot slot pool；
6. Intraday Store segment pool；
7. 证明正常 steady state allocator call=0。

## 阶段 3：instrument-sharded Event projection

1. 单 coordinator claim canonical task；
2. `instrument_id % N` 固定分片，建议先从 N=4 起测；
3. worker 输出 EventBatchLease，不写全局 journal；
4. bounded ordered completion ring；
5. 单 commit owner 按 canonical sequence 发布 Tick/Event；
6. filtered token 产生 zero-output completion；
7. 同 Tick Event generation 与 public Tick frontier 仍事务一致；
8. completion window 满或 worker 失败时 CERTIFIED 停在最后正确前缀，FAST 不受影响。

## 阶段 4：只在证据充分时优化 FAST ring

通过 `perf stat/record`、cache miss、atomic contention 和 flame graph 证明 `PublishRing/AdvanceContiguousTickPrefix` 占比后，再实施 per-worker completion bitmap/prefix owner。该阶段必须独立，任何 FAST p99 退化可立即回滚。

## 阶段 5：删除/构建隔离和 6.53 决策

1. deprecated compatibility header 经过外部窗口后删除；
2. standalone order-event diagnostic 默认不构建，确认无人使用后再移除；
3. legacy Shanghai mock 设为 opt-in；
4. parallel decoder farm 根据真实结果选择生产化或 build-isolate；
5. 与供应商和实盘样本确认后，单独评估迁移到 6.53-only。只有迁移完成且禁止 dual feed 后，才能删除 6.33/6.36 merge/reorder path。

---

## 9. 验收标准

## 9.1 正确性

对同一输入，优化前后必须保持：

- FAST latest/ring、CERTIFIED Tick、CERTIFIED Event 的字段与 digest 一致；
- source sequence、ingress sequence、tick stream sequence、native sequence 不变；
- derived Event 全局序号仍稠密、确定；
- 同 instrument Event 顺序和最终状态一致；
- Shanghai T→A、完全成交无 A、END finalization 顺序一致；
- Shenzhen 6.33/6.36 shared `(ChannelNo, ApplSeqNum)` 顺序一致；
- filtered token 不产出业务 row但推进 native frontier；
- exact duplicate 幂等，conflicting duplicate 只冻结对应 CERTIFIED channel；
- process-start partial 不伪装成 from-open；
- FAST/CERTIFIED instrument tick/event/snapshot history、KLine、Factor、Python/Polars 正常读取；
- Wire、持久 journal、C ABI、Python public schema 不变，除非在单独版本化提交中明确升级。

专项测试必须包含：

- 6.33/6.36 跨 callback 逆序；
- gap 1、多个 gap、回补；
- duplicate before/after retention；
- channel isolation；
- completion worker 乱序完成；
- one worker stall；
- completion ring full；
- 上海多证券 END，验证每个 END 只扫描当前 range；
- 容量只够当前 instrument 但不够全市场时，修复后应成功；
- clean stop/final generation；
- history overrun/reconcile。

## 9.2 构建与工具

- GCC 13 Release 全量 build + 47 CTest；
- ASan+UBSan 独立 build/test；
- TSan 独立 build/test；
- Python 全量测试；
- ABI/layout test；
- mock/replay/fuzz smoke；
- source tree无临时 probe和未登记生成物。

## 9.3 性能

在同机、同 affinity、同输入、同编译器、同日志级别下：

- FAST callback p99 不得回退；
- FAST callback→reader p99 不得回退；
- 普通无 gap CERTIFIED p95/p99 不得回退；
- Shanghai END CPU 工作量随当前 instrument 订单数增长，不再随全市场订单数增长；
- 正常运行 warmup 后系统 allocator 调用为 0，或仅出现在明确的 control/cold path；
- CERTIFIED catch-up 以同分支基线为参照，阶段 3 的工程目标可设为至少 2×，但应以实测通过，不预先承诺；
- queue high-water 在设计 SLA 内，不能靠扩大 queue 达标；
- Event ordered commit 的串行 CPU 占比和 wait time可观测；
- Python 全历史大批读取记录 bytes copied、rows/s 和 peak RSS。

---

## 10. 风险与回滚边界

| 改动 | 主要风险 | 控制方法 | 回滚粒度 |
|---|---|---|---|
| 上海 END range | range key 边界错误 | golden replay + 多 instrument END test | 单提交 |
| 精确 capacity preflight | 上界低估导致写入后失败 | aggregator 提供单一 authoritative max-output API | 单提交 |
| payload lease | lease 生命周期/duplicate 释放错误 | generation id、poison、ASan、满池测试 | 单提交 |
| channel incremental counters | 状态计数漂移 | debug 模式周期全扫描对账 | 单提交 |
| fixed order hash | 哈希退化/输出顺序变化 | load factor hard cap；ordered index；digest test | 与节点池分开 |
| Event sharding | 同 instrument 并发、全局乱序 | 固定 shard；completion ring；单 commit owner | feature flag |
| header O(1) | reader cut 不一致 | 保留 seqcount；协议 stress test | 单提交 |
| FAST ring 改造 | callback p99 和可见顺序回退 | 独立 feature flag；原实现保留一版 | 单提交 |
| 删除 diagnostic/legacy | 外部运维依赖 | build option 和 deprecation 窗口 | release 级 |

---

## 11. 明确不建议做的事情

- 不要通过继续扩大 4M handoff queue 解决消费不足；
- 不要在 Event 状态机之后对结果排序来补救深圳乱序；
- 不要让多个 Event worker 直接 append 全局 journal；
- 不要把 coordinator 随意按 instrument 分片；原生连续性 domain 是 channel，不是 instrument；
- 不要把 `std::map` 直接换成无序容器而改变上海 END revision 顺序；
- 不要关闭 KLine、Factor、CERTIFIED history 来“释放吞吐”；
- 不要删除 filtered continuity token、duplicate 验证或 fail-closed capacity；
- 不要把 mutable shared-memory ring 直接零拷贝暴露给长期持有的 Polars frame；
- 不要因为 6.53 已存在就认定 6.33/6.36 已废弃；
- 不要把本次合成 benchmark 当作生产容量承诺。

---

## 12. 最终建议

本分支应先做一个小而明确的“性能纠错版本”：上海 END range + 精确 capacity、CERTIFIED 单次投影、generation 直接返回、channel/header O(1) 化、double inspection 移除。该版本风险低、可单独验证，且会显著降低后续并行化的串行比例。

第二个版本再做有界内存和订单容器；第三个版本实现“单 coordinator + instrument Event workers + ordered commit”。这种拆分比在当前 4,681 行 CERTIFIED service 中一次性加入多 worker 更安全，也更容易用 digest、延迟和 allocator 指标证明每一步没有破坏正确性。

删除性重构应严格限制在：临时 probe 立即删除；compat/diagnostic/legacy mock 先 build-isolate，再经过外部依赖确认删除。核心 recovery、6.33/6.36 merge、完整历史、KLine/Factor 和 Python/Polars 生命周期保护都不是冗余。

---

## 附录 A：本次生成的证据文件

- `l2flow_build_review.log`：Clang 17 全测试构建阻断详情；
- `l2flow_certified_benchmark_review.log`：CERTIFIED/FAST 合成延迟和 gap catch-up；
- `l2flow_online_recovery_benchmark_review.log`：parallel decoder 0/4 workers 对照；
- `l2flow_handoff_queue_size_review.log`：当前 ABI 下 HandoffEvent/Cell/4M queue 精确尺寸。
