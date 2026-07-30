# Instrument Derived Event History API

## 定位与边界

该接口返回单只证券的派生订单生命周期，而不是把 raw Wire 行换一个名字：

- `ORDER_REVISION`：稳定 `OrderKey`、单订单 `revision`、`INSERT /
  UPDATE / FINALIZE`、`finality` 以及完整订单快照；
- `TRADE`、`CANCEL`：保留交易所源事件及其 source anchor；
- 上交所额外保留 `STATUS`，阶段来自 4.24 的状态传播；
- `derived_event_sequence` 只是当前进程、当前 instrument session 中的稠密
  派生顺序，不替代 `BizIndex`、`ApplSeqNum`、`source_sequence`、
  `ingress_sequence` 或 `tick_stream_sequence`。

本接口不写 Parquet，不做 WAL、overrun catch-up 或崩溃恢复，也不声称
retained origin 一定等于交易所开盘。外层派生 checkpoint 依赖仍存活的原生
订单状态机；进程重启后必须从 retained full history 重新 replay。其内嵌的
raw checkpoint 仍具有 raw history 文档定义的边界语义。

## 为什么一个 reader 固定一只证券

`InstrumentDerivedEventHistorySessionV1` 和 Python
`InstrumentDerivedEventHistoryReader` 在创建时永久绑定
`(trade_date, instrument_id, market)`。一个 reader 不能先 full 读取证券 A，
再 full 读取证券 B。

这不仅是性能选择，也是顺序正确性要求。沪深核心均校验观察到的
`tick_stream_sequence` 全局严格递增，以及同 channel 的交易所原生序号严格
递增。若用同一个 core 依次重放两只证券的日内 full history，第二次重放会把
全局 tick sequence 倒退。每 instrument 一个 core 同时隔离订单号空间并避免
跨证券状态污染。

## Full 与滚动更新

```text
create(instrument)
  -> begin/read_all(retained origin -> immutable generation G)
  -> explicit EOF
  -> checkpoint G
  -> begin/read_updates(checkpoint G -> immutable generation G+1)
  -> explicit EOF
  -> checkpoint G+1
  -> ...
```

只有独立 EOF 成功并核对 raw source counts 后才发布派生 checkpoint。最后一个
数据 page 并不等于 EOF。空 update 合法：它直接返回零事件 EOF，派生 sequence
不变。

full replay 构造“相对于当前 retained/可见源事件流”的内存状态，后续 update
必须提交同一对象上一轮返回的精确 checkpoint。这样 generation 边界可以自然
落在任意订单生命周期中间。例如：

```text
generation G:   T(order=100)      -> synthetic lower-bound revision 1
generation G+1: A(order=100)      -> source-add exact revision 2
```

第二轮不会重建或重复 revision 1。对连续竞价 A，revision 2 使用：

```text
OriginalQty = A.Qty + A.matched_quantity
```

同时保留 `observed_pre_add_trade_quantity` 进行相等性审计。无 A 的主动成交买单
使用最大成交价、卖单使用最小成交价作为 execution boundary，并明确标为
lower bound / inferred boundary；它不是可证明的原始限价或市价类型。

提前关闭 cursor，以及已经开始归约后的 Wire 投影、聚合或分配失败，都会
fail-close。这是因为核心状态有意不在每个 page 前复制一份用于回滚。
`begin_update` 的 checkpoint 不匹配发生在打开 raw read 和修改状态之前，因此
只返回 `CHECKPOINT_MISMATCH`；调用方仍可在同一 session 用正确 checkpoint
重试。

## 市场阶段与日终

immutable generation 只是发布/读取边界，绝不意味着交易阶段结束。因此午休、
一秒 generation 或某轮空 update 均不能 finalize 订单。

上交所阶段来自 4.24 `S` 及 decoder 的逐笔 phase 传播。`ENDTR` 可以让上交所
核心结束相应订单；此外 C++/Python 都提供显式
`FinalizeTradingDay`/`finalize_trading_day`，只能由调用方在确认干净交易日边界
后调用。该调用不推进 raw checkpoint，并永久封存 session。

当前生产深交所输入严格使用 **6.33 委托 + 6.36 成交/撤单** 的同 channel
`ApplSeqNum` 已归并流；不启用 6.53。两份输入文档没有证明 SDK 跨消息族的
多线程 callback 顺序，因此生产捕获强制单 I/O thread、非多线程 callback，
projector 还会对 channel 内非递增序号 fail-close；部署前应以 feeder CSV 验证
这一上游契约。6.33/6.36 不提供可供该逐笔 projector 使用的交易阶段，因此深圳
派生行不会根据本地时钟猜 phase。深圳成交也不猜主动方，金额不由价格乘数量
伪造。

## C++ 与 C/Python

- C++：
  `l2flow/ipc/instrument_derived_event_history_v1.h`，CMake 链接
  `L2Flow::derived_history`；
- 稳定 C ABI：
  `l2flow/ipc/instrument_derived_event_history_c_v1.h`
- Python：
  `L2FlowClient.open_instrument_derived_event_history(...)`

C++ 的典型生命周期是：

```cpp
l2flow::ipc::InstrumentDerivedEventHistoryConfigV1 config{/* ... */};
std::unique_ptr<
    l2flow::ipc::InstrumentDerivedEventHistorySessionV1> history;
auto error =
    l2flow::ipc::InstrumentDerivedEventHistorySessionV1::Create(
        std::move(config), &history);
error = history->BeginFull(0, 4096);

l2flow::ipc::InstrumentDerivedEventHistoryPageV1 page;
do {
    error = history->ReadPage(&page);
    consume(page.events);
} while (error ==
             l2flow::ipc::InstrumentDerivedEventHistoryErrorV1::kNone &&
         !page.eof);

l2flow::ipc::InstrumentDerivedEventCheckpointV1 checkpoint;
error = history->VerifiedCheckpoint(&checkpoint);
// 等下一 immutable generation 后：
error = history->BeginUpdate(checkpoint, 0, 4096);
```

Python 对应使用同一个 reader：

```python
with client.open_instrument_derived_event_history(1) as history:
    with history.read_all() as initial:
        for batch in initial.batches():
            consume(tuple(batch))
        checkpoint = initial.verified_checkpoint

    with history.read_updates(checkpoint) as update:
        for batch in update.batches():
            consume(tuple(batch))
        checkpoint = update.verified_checkpoint
```

C++ page 直接消费现有 immutable raw page 的 borrowed span，并把 raw Wire 行送入
同一套沪深原生 core。每个 raw page 只 map 一次；沪深临时 event vector 跨 tick
复用 capacity。一个 raw 行可能产生一个 source event 加两条 order revisions，
所以派生 page 是调用方拥有的 vector，而不是假设一进一出的 borrowed row。

C ABI 使用固定 320-byte row，支持 `BUFFER_TOO_SMALL` 无推进重试。Python 首次按
`3 * page_records` 准备数组；若实际 page 事件数更高则读取所需容量后重试同一
native page。普通目标延迟约为 immutable generation 发布间隔，加一次 raw page
映射和原生状态机归约；没有 per-generation 子进程启动。生产程序当前
`--generation-interval-ms` 默认 `1000`，允许配置为 `1..60000`；缩短周期会
增加 generation cut/发布频率，它不是逐 tick ring 的同义词。

`ENDTR` 或显式日终 finalization 可能由一条状态边界产生与当前订单状态数同阶的
FINALIZE revisions，因此不能假定派生事件始终不超过
`3 * raw_page_records`。C/Python 的重试协议保证正确性；部署容量仍应把该日终
瞬时内存纳入预算。

## 不应从字段推断的事实

- Wire V2 没有保留深圳 `MDStreamID` 文本，不能拿 `source_stream_id` 冒充；
- 深圳 6.36 没有 source amount，`trade_amount_valid=false`；
- 深圳 phase 和 aggressor 保持未知；
- 上交所无 A 的合成订单 `OriginalQty` 是成交可见下界；
- `PriceSource=BuyMaximumExecution/SellMinimumExecution` 是成交边界，不是被
  交易所直接发布的原始委托价；
- 当前接口不计算 `TimeTaken`，也不把不可观测订单类型填成 `MARKET`。
