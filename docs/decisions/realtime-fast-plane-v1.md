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

Fast history 是“已成功 decode 并留存”的 provisional 数据，不是 Canonical 投影。
只有 manifest 中该 source 的 exact-required message tuple 会进入 decode；其他消息
不会进入 Fast history。上海 tick 使用 deferred phase attribution，因此依赖交易阶段
归属、Canonical 去重/冲突判定或 Canonical quality gate 的因子不能把 V1 Fast history
视为正式 history 的等价替代。任一 Fast fatal 会撤销整个 Fast generation，之后所有 Fast
查询统一失败；此前返回的 handle 仍然内存安全，但在逻辑上已经失效，不得继续驱动
因子或交易决策。

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
- `decoded_records + ignored_records == captured_records`；
- `history_submissions == decoded_records`；
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
- `fast_plane_terminal_parity=true`（Fast 与四个 Raw callback 的最终 record count
  及 ingress sequence 完全一致）。

若 Fast Plane 中途 fatal，正式 route 仍保持 ACTIVE 到测试窗口结束；evidence 会将
`fast_plane_fatal_observed` 置为 true，bounded run 最终返回非零，便于自动化判定
Fast 测试失败而不把 shadow 故障冒充成正式 route 故障。未传
`--fast-plane-shadow` 时，现有 production 行为不变。

## 资源预算

V1 复用现有 manifest 的 ring/history sizing，不增加新配置项。启用后会额外分配：

- 四个 `raw.ring_capacity_bytes` 大小的 Fast input ring；
- 一套独立、受既有 history 上限约束的内存 history；
- 四个 decoder worker；
- `history.physical_workers` 个 Fast history worker。

例如示例 manifest 的 Raw ring 为 256 MiB，则四个 Fast ring 本身额外占用约 1 GiB，
尚未包含 private history。实盘前必须据此核对 NUMA、内存和 CPU 预算；如需绑核，可
由部署层使用 `taskset`/cpuset，不需要改变 SDK 配置。
