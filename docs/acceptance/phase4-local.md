# Phase 4 local acceptance record（本地代码范围）

Date: 2026-07-22
Host scope: local Linux x86-64 development container

> 2026-07-22 superseded note：本记录保留 standalone Phase 4 slice 截至
> 2026-07-22 的验收事实。`L2Flow::production` 后来通过独立授权变更切换到
> `l2flow_production`；该目标通过 source-order pipeline 接入 decoder，并使用新的
> `InstrumentHistoryRuntimeV1`。真实 corpus 与正式 Phase 4 external exit 不因此完成。

## 结论

本记录严格区分“Phase 4 独立 library/in-memory slice 已实现并通过本地验证”与
“生产 feeder 已切换、正式 Phase 4 Exit 已完成”。本记录当时的结论如下：

| 口径 | 本记录时状态 | 判定依据 |
| --- | --- | --- |
| **Phase 4 local implementation slice** | **已完成** | `L2Flow::phase4` 包含五类核心消息安全 decoder、immutable instrument registry、直接同步注入 seam、owned event 与四源 session store |
| **Scoped local verification** | **已完成** | 最终代码的 strict Debug、Release、ASan+UBSan `phase4` 定向 CTest 均为 **4/4，0 failed** |
| **Production feeder integration/cutover** | **未完成** | 本次按范围没有改写 feeder 的 CSV/WAL sink；截至本记录日期，`L2Flow::production` 仍指向 `l2flow_phase01`，生产 callback 尚未构造和调用 `MarketSessionV1::Inject` |
| **Formal Phase 4 exit** | **未完成** | 五类消息各 10,000 条真实 Raw、独立 oracle 逐字段对账、完整手工 golden 与持续 sanitizer fuzz 制品仍不齐全 |

因此，本文中的“已完成”只指第一、二行的仓库内切片和验证范围；它在当时不构成
切换 production alias 的授权，也不把 synthetic tests 写成真实交易日验收。

## 已实现的本地切片

### 不重复持久化的同步注入

`MarketSessionV1::Inject` 接收 feeder 或已验证 Raw consumer 提供的 borrowed
head/body view。成功返回前，decoder 已完成 bounds check，并把字符串、scalar、
十档和买卖一队列复制到 owned typed event；公开 event 的 `origin.body` 被清空，
所以 callback buffer 可以立即复用。此路径不打开、不追加也不管理 CSV/WAL。

注入与已有持久化 sink 是两个明确独立的 sink，不伪造事务关系。解码、消息族、
上下文、顺序或容量失败会 poison 对应 source，而不是继续写出带隐藏缺口的内存
历史。

### 五类安全 decoder 与字段有效性

支持的 exact key 为：

```text
4.101.4   SHL2MarketData
4.101.24  NGTSTick
6.101.28  Snapshot300111_v2
6.101.33  Order300192_v2
6.101.36  Transaction300191_v2
```

实现使用 little-endian 显式 load 和 `CheckedBodyViewV1`，不解引用未对齐 packed
vendor object。固定大小、全部硬编码 offset、nested item layout、消息 key 和关键
scale 均由 SDK 2.13.234 compile-time assertions 锚定；相对 offset、count、乘加、
range overlap、固定下界和配置上限均在访问前检查。结构失败保持 caller output
不变，未知 service version 以 schema unknown fail closed。

定点数保留 exact raw/scale/null，并以 checked integer arithmetic 规范到 p6，不经
浮点往返。SH A/D/T/S、SZ side/order type、trade/cancel 的 action-specific
validity 独立发布；非正 event sequence 或 primary order ID 不会被标 valid，SZ
transaction 的负引用不能参与撤单侧推断。SDK header `LocalTime` 只投影到日内
纳秒，绝不借用 `trade_date` 伪造 Unix 日期；exchange time 才使用固定 UTC+08:00
投影。

当前仓库没有经版本和 hash 固定的 SZ 涨跌停 business-sentinel policy。因此
Phase 4 精确保留 high/low raw 与 scale，但保持 numeric invalid、semantics unknown
并发布 notice；后续因子不能把巨大哨兵误当有限价格，也不能靠 magnitude 猜测。
同样，历史上被复用过业务含义的 SH `WarLowerPri` 只以 neutral raw field 暴露并
保持 invalid，等待产品/版本 applicability policy，避免把回购指标误当权证价格。

### 全天默认驻留与可选时间窗

`SessionRetentionModeV1::kFullSession` 是默认值。它从 session construction 起保留
所有成功接纳记录，不覆盖旧记录，适合“开盘以来全部数据”的后续因子计算。
session namespace 是 exact
`(capture_date, source_stream_id, stream_day_id)`；交易日切换由控制面创建新 session，
不从 wall clock 猜测。

四条 TCP source 分别保序并分别暴露历史，不把 realtime、exchange time 或裸
sequence 伪造成跨流全序。每源 `source_sequence` 必须按 feeder/Raw 权威顺序严格
递增，允许因 control record 产生间隙。per-source mutex 防 data race，但不负责
重排并发乱序调用。

full-session 仍要求显式非零 `max_records` 与 `max_payload_bytes`。这是 admission
hard limit，不是 ring capacity；达到上限后拒绝新记录并 poison source。可选
`kRecvMonotonicWindow` 仅按各 source 自己的 nondecreasing receive-monotonic 时间
推进 inclusive window，不让另一条流驱逐本流记录。

session 使用按真实 event alternative 分配的 `RetainedMarketEventV1`，避免每条
tick 都承担最大 snapshot variant 的空间。immutable chunk snapshot/record handle
可以在 writer mutex 外读取，并在后续 append、window eviction 或 store 销毁后
保持引用稳定。

`max_payload_bytes` 是 deterministic logical payload accounting，不是 RSS/物理
DRAM pinning 保证。allocator/container metadata、OS paging，以及 reader 长期持有
snapshot 所固定的已淘汰 chunk 不计入该数字；生产容量规划必须另测完整交易日
RSS、page fault、snapshot 数量和生命周期。

## 当前定向验证结果

最终代码执行结果：

```text
strict Debug:       4/4 passed, 0 failed
strict Release:     4/4 passed, 0 failed
ASan+UBSan Debug:   4/4 passed, 0 failed
```

测试项为：

```text
test_phase4_market_decoder
test_phase4_instrument_registry
test_phase4_session_store
test_phase4_market_decoder_fuzz_smoke
```

覆盖范围包括五类消息的独立 hard-coded wire builder、fixed lower bound、schema
gate、descriptor/list 边界、10 档/50 队列 cap、null/负值/溢出、SH tick 与 SZ
order/transaction validity matrix、registry exact-byte lookup、时间和 ID domain、
phase history/cap，以及失败 output atomicity。

store 测试实际接纳并 snapshot **100,001** 条 full-session 记录，验证所有记录仍在、
首尾 source order 精确且 sequence gap 合法；同时覆盖 hard limits、per-source window、
四源 context、并发 append/snapshot、snapshot lifetime 和 callback body 被覆盖后的
直接注入 ownership。

deterministic fuzz smoke 对五类 selector、supported/unknown version 和 15 个 fixed
boundary body size 先执行 150 个定向 case，再执行固定种子的 50,000 个任意 body，
合计 **50,150** case。该 target 同时提供可选 Clang libFuzzer 入口。ASan/UBSan
运行使用：

```text
ASAN_OPTIONS=detect_leaks=0:halt_on_error=1:abort_on_error=1
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1
```

三种构建的上述 `phase4` 定向套件均无测试失败，sanitizer 构建无报告。当前环境
没有 `clang++`，所以本次没有执行持续 libFuzzer campaign；deterministic smoke
不能冒充该外部制品。

额外的 Release 全量 CTest 为 **106/107 passed**。唯一失败是既有
`test_phase0_baseline` vendor-artifact gate：checked-in
`mdl_sdk_2_13_234/libs/linux/libmdl_api.so` 仍是 134-byte Git LFS pointer，而不是
获批 ELF shared object；这与既有 Phase 2/3 acceptance 记录一致。本记录不把这次
全量运行写成全绿，也不通过放宽基线门禁来掩盖缺失的外部制品。

补充的 GCC TSan 构建可以完整编译四个 Phase 4 target，但该容器在进入测试逻辑前
即以 `ThreadSanitizer: unexpected memory mapping` 终止；单独启动 session-store
test 也得到相同 runtime 初始化错误。因此这里既不把它解释成代码 data-race
报告，也不声明 TSan 已通过；应在兼容的非受限宿主上补跑。

## 真实数据现状与未完成 Exit gate

仓库中的 60 秒 feeder CSV 共有 10,968 条 market row，分布为：

```text
4.101.4   6,864
6.101.28  4,104
4.101.24      0
6.101.33      0
6.101.36      0
```

其 comparison CSV 记录两类 snapshot 的选定字段与 backup CSV 一致，但这些是
CSV 字段对照，不是五类完整 body 的 Raw corpus，也不是独立 C++ oracle。因此它
既没有达到“每类至少 10,000 条”，也不能证明全部 scalar、nested list、validity、
null、enum 和业务适用范围已逐字段核对。

本记录当时列出的正式 Phase 4 exit 条件为：

1. 在交易时段从权威 Callback WAL/Raw 为五类 key 各抽取至少 10,000 条真实 body；
2. 使用独立实现的 C++ oracle 和人工黄金样本逐字段对账，并保留版本、hash 与
   mismatch artifact；
3. 固定正式字段/枚举/产品适用范围和 SZ limit-sentinel reference policy；
4. 对真实 corpus 执行持续 ASan/UBSan libFuzzer，并保存命令、时长、seed、crash
   corpus 和零失败结果；
5. 在生产 feeder callback 的既有 CSV sink 旁构造 `MarketSessionV1`，按每源权威
   顺序同步调用 `Inject`，完成开盘至收盘的 record/RSS/page-fault/latency/limit
   容量验收；
6. 本记录当时要求上述证据完成并评审前保持 `L2Flow::production` alias 不变；
   后来的独立 alias 变更不追溯完成这些 Phase 4 exit 证据。

详细 API 与边界合同见
[`docs/decisions/phase4.md`](../decisions/phase4.md)。
