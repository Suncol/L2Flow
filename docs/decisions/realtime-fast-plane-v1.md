# Realtime Fast Plane V1

## 目标与边界

V1 只实现从 Vendor callback 到按 instrument 分片的内存 history：

```text
Vendor callback
  -> 每 source 一个有界 SPSC ByteRing
  -> 每 source 一个顺序 decode worker（共 4 个）
  -> 16 个逻辑 instrument shard
  -> deployment 配置的 M 个 history worker
  -> ProductionServiceV1::FastLatest / FastTail
```

它不实现 Trace Plane，不新增 SDK 校验，也不改变 SDK 的线程、订阅或连接行为。现有
Raw/WAL/Canonical 路径继续运行，作为正式路径和回退依据。Fast copy 只等待 Raw ring
与 SourceFrontier 的 capture commit，不等待 WAL flush、Raw live tail 或 Canonical。

## 并发与顺序

- 四种 ingress 各有一个独立 ring 和 decoder worker，因此四个 source 可并行 decode。
- 同一个 source 的 callback 由该 `CallbackHandler` 的 admission gate 串行化，只能有
  一个 producer 写对应的 SPSC ring；不同 source 可以并行发布。同 source 再由一个
  worker 按 callback 顺序 decode，满足 decoder 和 history 的单 producer、从 fresh
  sequence 1 开始连续递增的约束，不需要额外 reorder buffer。
- history 固定使用 16 个逻辑 shard，并按既有
  `history.physical_workers` 映射到多个物理 worker。
- `FastLatest`/`FastTail` 读取 instrument 所在 shard 已完成的 append，不等待同一
  source 上无关 instrument 补齐全局 ACK prefix，从而避免跨 instrument 的
  head-of-line blocking。

Fast history 是已通过 Fast exact-dedup、成功 decode 并追加的 provisional 数据，不是
Canonical 投影。只有 manifest 中该 source 的 exact-required message tuple 会进入
decode；其他消息不会进入 Fast history。

上海阶段不根据本地墙钟、tick 时间或 SYS/control 消息推断。只有
`4.101.24 NGTSTick` 中 `Type="S"`、且已通过 Vendor 与上海 exact-dedup 的 STATUS
发布，才可能按 `TickBSFlag` 更新对应证券的阶段。同一证券、同一 channel 内，只有
`BizIndex` 高于当前 STATUS watermark 的发布可以推进 watermark；首次见到但较低的
晚到 STATUS 仍写入 history，却不能回拨该 channel 的当前阶段。普通 tick 只继承同一
证券、同一 channel 当前 watermark 对应的可识别阶段；该 channel 首次出现可识别
STATUS 前保持 `Unknown`。无法识别的较新 STATUS 自身以 `Unknown` 留存并推进该
channel 的 watermark，但不会覆盖该 channel 此前的已知阶段。这里的 phase 是处理点
上的“当前 STATUS watermark 归属”，不是按普通 tick 自身 `BizIndex` 回溯交易阶段，
也不是对延迟或批量送达事件的原始经济阶段进行重建。
可识别映射为 `START/OCALL/TRADE/SUSP/CCALL/CLOSE/ENDTR` 对应
`Start/OpeningCall/Continuous/Suspended/ClosingCall/Closed/End`。

Fast first-seen index 保留完整 evidence 并进行精确比较：

- Vendor scope 为 `(capture_date, source_stream_id, stream_day_id, ServiceID,
  MessageID)`，以 Vendor `SequenceID` 为序号；
- 上海 scope 为 `(trade_date, source_stream_id, Channel)`，以正值 `BizIndex`
  为业务序号；
- 深圳同一 channel 的 `6.101.33` Order 与 `6.101.36` Transaction 共用一个 scope，
  以正值 `ApplSeqNum` 为业务序号；snapshot 只有 Vendor exact-dedup。

Vendor evidence 是 `ServiceVersion`、encoding、Vendor `LocalTime` 与完整 body；
接收时间和 ingress sequence 不参与比较。exchange evidence 是 phase 继承前的 decoded
业务语义，避免 phase 状态变化把同一业务事件误判为 conflict。

同一 scope、同一序号且 exact evidence 相同才是 duplicate，并在写 history 前抑制。
比较直接使用 evidence 长度和完整字节，不依赖摘要。去重键不使用价格或数量，因此
价格、数量相同但业务序号不同的两笔事件都会保留。

Fast 不维护业务 high-water、`expected_first`、连续性、gap、backward、poison 或
evidence policy-capacity 状态。任何此前未见的正序号，无论向前跳号还是低于已见序号，
都会作为 first-seen 写入 history。同一序号 evidence 不同是 conflict：Fast 稳定保留
第一条 evidence，丢弃当前冲突副本并继续运行；Vendor conflict 在 decode 前丢弃，
exchange conflict 在 decode 后、phase 更新前丢弃。它们都不会撤销 Fast generation。
first-seen index 在 generation 生命周期内不淘汰、没有人工 scope/entry/payload
容量上限；真实内存分配失败仍是无法继续保持 exact-dedup 的运行时故障。phase slot
capacity 仍是启动边界：注册上海证券数超限时直接拒绝创建 Fast runtime。

索引把到达时刷新最大值的 record-high 序号保存在只追加的 deque，把低于当前最大值
但首次见到的迟到序号放入有序旁表。正常递增 first-seen 不会搬移既有 entry；迟到
first-seen 使用 `O(log N)` 查找与插入，也不会触发全量有序 vector 的中间搬移。

因此 Fast 不证明启动前、交易日开头或启动后的业务序列前缀完整，也不报告缺口闭合。
first-seen conflict 版本也不保证等同于 Canonical 最终选择。这些都是 Fast history
继续保持 provisional 的独立原因。

完成阶段归属和 exact-dedup 后，Fast history 仍然是 provisional：它不等价于完整
Canonical quality/business gates，也不携带完整 Canonical 投影语义。依赖这些语义的
因子不能把 Fast history 视为正式 history 的等价替代。任一 Fast fatal 会撤销整个
Fast generation，之后所有 Fast 查询统一失败；此前返回的 handle 仍然内存安全，但在
逻辑上已经失效，不得继续驱动因子或交易决策。

生产内的查询必须经由 `ProductionServiceV1::FastLatest` 或 `FastTail`。该 facade
同时检查 Fast generation 与正式 route 是否仍为 ACTIVE，避免绕过 route/fatal
边界。正式 `history()` 的查询语义没有改变，不能使用 provisional 可见性。

## 背压、失败与停止

Vendor callback 上的 Fast 操作只有原子状态检查与一次有界 ring copy。ring 满或输入
namespace 不匹配时，callback 只做原子 fatal latch 并立即返回；history 撤销由 worker
异步执行，callback 不获取 history mutex。

若 Raw callback 或 SourceFrontier capture fail-stop，同一个 callback 会先通过共享的
Fast sink control hook 原子撤销 Fast generation；Fast 查询不依赖后台 monitor 才发现
正式 source 已失效。

history queue 暂时满时，所属 decoder worker 进行有界队列重试，其他 source 继续独立
工作。如果 callback ring 最终耗尽，Fast generation fail-closed，但
`--fast-plane-shadow` 不撤销、不停止正式 Raw/Canonical route。

停止顺序固定为：

1. 关闭四个 callback admission gate；
2. 等待 Vendor callback quiesce；
3. 优先完成既有正式 Raw/Canonical 路径的 drain/stop；
4. 停止并排空 Fast decoder ring；
5. 停止 Fast history worker。

Production builder 不为 Fast history 安装测试/telemetry hook。Fast runtime 的
`StopAndDrain()` 与 history runtime 一样，要求任何显式注入的 hook 都遵守契约并及时
返回；阻塞 hook 只用于受控测试，在释放前不得调用停止流程。

最终 snapshot 中 `clean_drain=true` 表示每个 source 都满足：

- `last_processed_sequence == last_captured_sequence`；
- `captured_records == ignored_records + history_submissions +
  vendor_duplicate_records + exchange_duplicate_records +
  vendor_conflict_dropped_records + exchange_conflict_dropped_records`；
- `decoded_records == history_submissions + exchange_duplicate_records +
  exchange_conflict_dropped_records`；
- history submitted ticket 已全部 ACK；
- worker 已退出且 source 未 fatal。

## 实盘 shadow 测试

先按现有 production manifest 和 fresh-only 流程完成部署准备，然后显式加入
`--fast-plane-shadow`：

```bash
./build/mdl-production-router \
  --deployment-dir /absolute/production/deployment \
  --manifest-sha256 <production-v1.tsv 的 64 位小写 SHA-256> \
  --fast-plane-shadow \
  --run-seconds 300 \
  --evidence-json /absolute/production/deployment/fast-plane-300s.json
```

`evidence-json` 必须是 deployment directory 的直接子文件且预先不存在。一次干净的
Fast Plane 测试应同时满足：

- 进程退出码为 0；
- 每个 sample 的 `fast_plane.enabled=true`、`fatal=false`；
- post-stop snapshot 的 `fast_plane.stopped=true`、
  `fast_plane.clean_drain=true`；
- 四个 source 的 `terminal_prefix_complete=true`；
- `fast_plane_fatal_observed=false`；
- `fast_plane_terminal_parity=true`（Fast capture 与四个 Raw callback 的最终
  record count 及 ingress sequence 完全一致；duplicate/conflict 被抑制后，history
  行数可以小于该 count）。

任意 300 秒窗口若没有真实的上海 STATUS transition，只能验证 Fast 管线和延迟，不能
验证阶段切换。phase shadow 验证窗口必须覆盖实际收到的、可识别的 `Type="S"`
transition，并检查同一证券切换前后的 history 与 phase counters；不得用墙钟推定一个
期望阶段来代替 STATUS evidence。

若 Fast Plane 中途 fatal，正式 route 仍保持 ACTIVE 到测试窗口结束；evidence 会将
`fast_plane_fatal_observed` 置为 true，bounded run 最终返回非零，便于自动化判定
Fast 测试失败而不把 shadow 故障冒充成正式 route 故障。未传
`--fast-plane-shadow` 时，现有 production 行为不变。

## 资源预算

V1 不增加 production manifest 字段；ring/history 继续复用现有 sizing，phase slot
使用 Fast runtime 的启动硬上限，exact-dedup state 没有人工容量上限。启用后会额外
分配：

- 四个 `raw.ring_capacity_bytes` 大小的 Fast input ring；
- 一套独立、受既有 history 上限约束的内存 history；
- 四个 decoder worker；
- `history.physical_workers` 个 Fast history worker；
- 每个已注册上海证券一个 phase slot，其中 channel 状态按收到的 STATUS 延迟创建；
- Vendor 与 exchange sequence scope、索引及完整 evidence。

每个 Fast generation 会保留每条唯一 Vendor observation，以及每条唯一、带业务序号
的上海/深圳 observation 的完整 evidence，不做淘汰。因此 sequence state 的空间复杂度
为 `O(U_vendor + U_exchange)` 个 entry，加
`O(P_vendor + P_exchange)` evidence bytes，另有索引和容器开销。它们按需增长而非
启动时一次预分配，没有 scope、entry 或 evidence bytes policy cap。实盘必须据唯一
消息数量和完整 evidence 总量预留内存；若真实分配失败，Fast 无法继续保证全 generation
exact-dedup，因而仍按内部资源故障停止。注册上海证券数超过默认 `100,000` 个 phase
slot 上限则在创建时失败；phase channel 状态没有人工容量上限。

例如示例 manifest 的 Raw ring 为 256 MiB，则四个 Fast ring 本身额外占用约 1 GiB，
尚未包含 private history、phase slots 和 exact evidence。实盘前必须按预期全天唯一
消息数与 evidence payload 总量核对 NUMA、内存和 CPU 预算；如需绑核，可由部署层
使用 `taskset`/cpuset，不需要改变 SDK 配置。
