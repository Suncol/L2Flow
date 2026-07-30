# 沪深逐笔订单事件重建 V1

## 实现范围

本阶段交付两个整数化、可重放的原生核心：

- `ShanghaiOrderEventAggregatorV1`：上交所 4.24 合并逐笔
  `A/D/S/T`；
- `ShenzhenOrderEventProjectorV1`：深交所 6.33 逐笔委托与
  6.36 逐笔成交/撤单。

两者接收已经完成 MDL schema 校验和固定点转换的事件，输出原始成交/撤单
事件以及带 revision 的订单快照。它们不在 SDK callback 中运行，不写
Parquet，也不提供 WAL 或崩溃恢复。

源字段语义以以下本地文档为准：

- `通联_沪深L2行情数据结构展示V4.0.pdf`；
- `通联数据MDL消息参考959-SDK用户版.pdf`。

## 数值与标识约束

- 价格和金额使用现有 decoder 的 `normalized_p6` 整数；
- 上交所 4.24、深交所 6.33/6.36 数量在 decoder 边界为 scale 0
  整数；
- 不使用浮点累计；
- 订单键包含交易日、instrument、channel 和订单号；
- `native_event_sequence` 保留 SSE `BizIndex` 或 SZSE
  `ApplSeqNum`；
- `source_sequence`、`ingress_sequence` 和
  `tick_stream_sequence` 保持各自语义，互不替代；
- 合成订单 revision 只锚定真实来源事件或显式的全零清洁边界，不伪造
  `BizIndex`/`ApplSeqNum`。

输入必须保持上游顺序。核心会拒绝：

- 非递增的全局 `tick_stream_sequence`；
- 同一 channel 中已观察消息的非递增 `BizIndex`/`ApplSeqNum`。

这里不要求相邻序号连续，因为按 instrument 或 A 股范围过滤后，同一
channel 中其他证券的记录会形成合法间隙。交易所级连续性必须在过滤前的
全 channel 流上验证；instrument history checkpoint 只能证明 Store 对已接收
记录的覆盖，不能证明 vendor 序号没有缺口。

## 上交所 4.24

### 连续竞价 `T...T -> A`

对于收到真实 A 的订单：

```text
OriginalQty = A.Qty + A.matched_quantity
Balance     = A.Qty
```

其中 `A.matched_quantity` 来自 A 消息中名为 `TradeMoney`、但语义实际为
已成交委托数量的源字段。已观察到的 A 前主动成交量只用于校验：

```text
observed_pre_A_qty ?= A.matched_quantity
```

源 matched quantity 有效时，它是 `OriginalQty` 公式的权威输入；不因本地
观察不一致而替换。发生不一致时保留源值并设置 conflict。

若 matched quantity 不可用，最强的可观测数量下界是：

```text
OriginalQtyLowerBound = A.Qty + observed_pre_A_active_fill_qty
```

不能写成两者的 `max`，因为 A.Qty 是首次撮合后的剩余量，与已观察 A 前
成交量互斥。

收到 A 后只从 A 发布的剩余量扣除后续事件：

```text
Balance =
    A.Qty
    - post_A_trade_qty
    - post_A_cancel_qty
```

A 前成交不得再次从 A.Qty 扣除。

### 集合竞价和停牌

4.24 文档说明 OCALL、SUSP、CCALL 阶段统一发布时，A.Qty 是原始委托量：

```text
OriginalQty = A.Qty
Balance     = A.Qty
```

这些阶段不套用连续竞价的 `A.Qty + matched_quantity` 公式。阶段未知、
START、CLOSE 或 ENDTR 上出现 A 时只给保守下界并设置阶段质量标志，不按
本地时钟猜测阶段。

### 完全成交且没有 A

只有带明确买卖主动标志的连续竞价 T 才创建 T-only 订单状态。对同一订单号
的所有已观察主动成交累计：

```text
ObservedOriginalQtyLowerBound = sum(fill_qty)
```

按需求给出执行价格边界：

```text
BUY  -> max(fill_price)
SELL -> min(fill_price)
```

该价格是与已观察成交一致的执行边界，不是唯一可恢复的原始限价；真正的
市价单也没有原始限价。因此输出同时携带：

- `order_source = ReconstructedFromTrades`；
- `original_quantity_status = LowerBound`；
- `price_source = BuyMaximumExecution` 或
  `SellMinimumExecution`；
- `apply_to_book = false`。

`TickBSFlag=N` 的成交原样输出，但不猜主动订单，也不创建 T-only 状态。

### 数量冲突

负余额和 source/observed prematch 不一致均原样保留并标记 conflict，绝不
截断为零。SSE T 的源 `TradeMoney` 作为成交金额保存，不用价量乘积覆盖。

## 深交所 6.33/6.36

### 输入顺序契约

两份通联文档说明同一 `ChannelNo` 下 `ApplSeqNum` 唯一连续，但没有说明
SDK 对 6.33 与 6.36 两个消息族的跨回调调度细节。本版本因此不把任意多线程
callback 到达顺序冒充交易所顺序，支持的生产契约是：上游已经按
`(ChannelNo, ApplSeqNum)` 合并交付 6.33/6.36。

物理 SDK 路径创建 `multithread_callback=false` 的 Subscriber，并强制
`io_threads=1`；同一 source lane 再保持 capture 顺序。projector 对每个 channel
检查 `ApplSeqNum` 严格递增，发现跨消息族倒序会在修改订单状态前 fail-close，
不会静默错算。上线前仍应使用 feeder CSV 做 6.33/6.36 联合单调性验证；若实际
厂商回调不满足该契约，必须改用 6.53 或在采集层增加正式归并，不能只扩大
event ring 或放宽检查。本阶段不实现这种 catch-up/恢复路径。

### 6.33 委托

`ApplSeqNum` 同时作为该消息的订单号：

```text
OriginalQty = OrderQty
Balance     = OrderQty
```

`OriginalQty` 是源值，因而是 exact。只有限价单价格有意义；市价和本方最优
订单保持 `price_valid=false`，不从后续成交反推并覆盖源订单。

Side 支持文档列出的：

- `1` 买；
- `2` 卖；
- `G` 借入；
- `F` 出借。

### 6.36 成交

每笔成交保留价格、数量及 Bid/Offer 两个订单引用。6.36 不提供：

- 主动买卖方向；
- 源成交金额。

所以输出固定为：

```text
aggressor   = UNKNOWN
amount_valid = false
```

不按订单号大小猜主动方，也不从价格乘数量伪造源金额。已知 6.33 订单按引用
扣减；未知或为零的引用只设置审计标志，不创建订单。若两个正引用相同，则
保留成交并标记 ambiguous，且不对同一状态重复扣量。

Bid 位置可引用 Buy/Borrow，Offer 位置可引用 Sell/Lend。不能把 Bid/Offer
字段名直接当成原始 6.33 Side。已知撤单的 Side 从 6.33 订单状态回填。

### 6.36 撤单

撤单必须恰有一个正引用、另一个为零。撤单价格没有业务意义，始终保持
invalid。已知订单扣减余额；未知订单只输出带质量标志的撤单审计事件。

## Revision 和最终化

订单事件使用：

```text
INSERT -> UPDATE ... -> FINALIZE
```

每次状态改变 revision 严格递增。余额恰好为零时可终结；显式清洁交易日
边界会最终化其余状态。边界可以锚定一条真实源消息，也可以使用全零锚；
全零锚只写入 FINALIZE revision，不覆盖订单的最后真实 source anchor。

日终仍有可观察余额不等于数据错误，所以单独设置
`EndedWithObservedBalance`，不把它自动升级为数量冲突。

## Instrument raw-event history

正式的 C、C++ 和 Python 全量/滚动读取接口见
`instrument-raw-event-history-api.md`。该接口复用 immutable Store
generation：

- 全量：retained origin 到固定 target generation；
- 滚动：verified checkpoint 的排他边界到新 target generation；
- 显式 EOF 对总数和分 source 计数完成核对后才返回 checkpoint；
- C++ 每页一次只读 mmap，并返回借用的连续 Wire span；
- Python 复用一个隔离 worker，并只复制调用方选择的定宽列。

其可见性延迟受 generation 发布周期约束，适合确定性 replay、校验和滚动
历史消费；逐 tick 低延迟仍应使用 global tick ring。

## Instrument derived-event history

正式的 C++、稳定 C ABI 与 Python 派生事件接口见
`instrument-derived-event-history-api.md`。每个 reader 固定一只证券，并在
`read_all()` 后保留同一原生沪深状态机供 `read_updates()` 使用；因此
`T` 与后到 `A` 即使跨 immutable generation，仍会从 synthetic lower-bound
revision 正确更新为 A-backed exact revision。checkpoint 只有消费显式 EOF 后
可用，且外层 derived checkpoint 只对返回它的存活 reader 有效。

## Low-latency event-delta 实时链路

正式实时链路为：

```text
Wire V2 global tick ring
  -> mdl-order-event-aggregator 独立进程
  -> Shanghai/Shenzhen native core
  -> event-delta sealed memfd ring
  -> C++ / stable C ABI / Python live reader
```

`OrderEventLiveAggregationEngineV1` 必须从
`tick_stream_sequence=1` 开始稠密消费沪深混合 global tick 流。快照等非目标
消息产生零条派生事件，但仍推进 source cursor；不能只读关心的证券或 source
slot。每个目标 tick 只调用一次 `PublishSourceTick`：

- 该 tick 的全部 320-byte event rows 先写入 slot；
- 完整 event prefix 随后 release-publish；
- 最后才 release-publish source cursor；
- 因此公开 prefix 永远结束在完整 source-tick 批次之后。

一个有限 reader buffer 可以拆分一个**已经完整提交**的 tick。需要逐 tick
原子 upsert 的客户端必须按 `tick_stream_sequence` 分组，并在看到下一 tick，
或 `next_sequence == published_event_sequence + 1` 证明已读尽公开 prefix
之后，才能提交末尾分组。

ring 自行分配跨证券稠密的 `derived_event_sequence`，不将其与交易所
`BizIndex` / `ApplSeqNum` 或本地 source/ingress/tick 序号混用。source gap、
不可容纳的单 tick 批次、slot 异常、producer failure 和 reader overrun 都永久
fail-close；V1 没有跳过缺口、overrun catch-up、reset 或恢复入口。
`STOPPED_CLEAN` 的既有只读映射仍可 drain，`FAILED` 不可继续读取。

### 独立进程与 READY

`mdl-order-event-aggregator`：

1. 通过现有 Wire V2 GET_SESSION socket 获取并严格验证 O_RDONLY sealed
   source mapping；
2. 从全局 tick sequence 1 处理 attach 时已经公开且仍完整保留的 prefix；
3. 创建独立 event session、event control socket 和 heartbeat；
4. 只有 source/event prefix 均未发生覆盖时才宣布 READY；
5. 运行时源 overrun、序号缺口、状态容量耗尽或任何投影/发布失败均关闭
   event session。

生产 router 的 `--event-aggregator-socket` 是可选兼容开关；配置后则是强制
启动门槛。router 在创建 SDK pipeline 之前等待同一
`source run_id/session_epoch/trade_date` 的 READY，并额外要求
`source_tick_consumed_sequence=0`、`event_published_sequence=0`。因此启用该
门槛时，SDK 第一条 callback 不可能先于聚合器 READY。超时由
`--event-aggregator-ready-timeout-ms` 控制。

event control 使用固定宽度 Unix `SOCK_SEQPACKET` 协议、双向 same-UID
`SO_PEERCRED` 校验和 `SCM_RIGHTS`。只有 ACTIVE 且 coverage 未丢失时才传递
O_RDONLY ring fd；source session 和 event session 是两个独立身份，重启聚合器
不会静默复用旧 event session。

源端进入 `STOPPED_CLEAN` 后，进程先读尽最终 contiguous tick prefix，再把
event ring 置为 `DRAINING/STOPPED_CLEAN`。这只是完整消费和干净进程边界，
**不等于日终订单最终化**：进程不会凭本地停止事件伪造
`BizIndex/ApplSeqNum`，深圳尚未终结的订单仍可保持 provisional。需要显式订单
日终 finalization 时使用 derived-history 的 `FinalizeTradingDay`；本阶段不做
日终 Parquet compaction。

### C++、C 与 Python reader

- C++：`OrderEventDeltaRingReaderV1`，通过
  `OrderEventDeltaControlConnectV1` 获取；
- C：`order_event_delta_ring_c_v1.h`；
- Python：`L2FlowClient.open_live_order_events(event_socket)`。

Python 控制面再次校验 source identity、协议保留字段、same-UID peer、fd
`CLOEXEC/O_RDONLY/seals/size`，随后交给 native layout/session validator。
热路径 `LiveOrderEventDeltaBatch.buffer` 是只读连续 memoryview，不构造逐行
Python 对象；`.row(i)` 只用于冷路径检查。

实时可见性不等待 immutable generation。进程在有数据时连续 drain；只有空读
才按 `--poll-ms` 休眠，因此空闲后第一批的额外检测延迟上界约为该配置值。
instrument full/update 历史接口则仍受 generation 周期约束，两者用途不能混淆。

容量至少同时满足：

```text
event_ring_capacity >=
    max(
        peak_derived_rows_per_second * maximum_reader_pause_seconds,
        maximum_rows_emitted_by_one_source_tick
    )
```

第二项尤其包括某只沪市证券的 ENDTR status row 加该证券全部待 finalization
订单 revisions。单 tick 批次不能跨多次 producer commit 拆分，容量不足会在
推进 event/source prefix 之前 fail-close。source global tick ring 也必须覆盖
聚合进程允许的最大停顿；发生 overrun 后本版本不会追赶或恢复。

样例 `/data/share/L2_example/events.parquet` 使用 p4 的 Price/Qty/Amount 与
20 个 canonical 字段；本阶段不写该文件。未来 writer 应基于派生事件做明确映射：

- `ORDER` 来自 order revision 的选定版本，不能把所有 revision 当成不同订单；
- `TRADE`/`CANCEL` 逐源事件保留，不合并成交明细；
- `EventSeqNo` 可取稠密 `derived_event_sequence`；样例语义下
  `SourceSeqNo` 应取 source anchor 的 `native_event_sequence`
  （SSE `BizIndex` / SZSE `ApplSeqNum`），不能误用本地
  `source_sequence`，三者不可互换；
- 当前沪深源价格/金额由 p3/p4 精确归一到 p6，转样例 p4 时应先校验
  `value_p6 % 100 == 0` 再精确除以 `100`；若未来出现更高精度，必须升级
  schema/声明舍入策略，不能静默整数截断。scale-0 数量转样例 p4 则用
  checked `quantity * 10000`，拒绝溢出；
- `OriginalQty` 只在 `original_quantity_valid=true` 时输出，并同时保留
  exact/lower-bound 状态；
- `MDStreamID` 当前 Wire V2 未保留，不能用 `source_stream_id` 冒充；
- 当前接口不计算样例 `TimeTaken`，缺失值策略留给后续 schema/writer 版本。

## 本阶段明确不包含

- Python Arrow/Parquet writer；
- 日终 Parquet compaction；
- Raw WAL；
- ring overrun catch-up；
- 进程崩溃恢复；
- feeder CSV 的回放恢复流程。

这些缺项不得由内存 Store、history checkpoint 或订单 revision 冒充。
