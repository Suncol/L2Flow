# Callback 到 Event Polars 吞吐与延迟测试（2026-08-05）

## 结论

在本机、本次固定配置和 3 秒持续负载下，当前代码不能通过
400k–800k message/s 的完整链路吞吐验收。

- 400k/s 重复 3 次，严格吞吐目标通过 0/3。一次保持完整但
  all-plane 最终追平仅 387.2k/s；另外两次失去 FAST coverage。
- 400k–800k 的归因矩阵中，首次失败均为 callback admission 返回的
  `kDecoderQueueFull`。没有 owned-message pool 拒绝，也没有
  FAST/Event/KLine plane queue failure。
- FAST coverage 一旦因 ingress 缺口变为不完整，后续对应 instrument
  按 fail-close 进入不可恢复状态。这些消息不能用 Event/KLine repair
  “补回来”，所以 callback 接受速率不能当作系统吞吐。
- 250k/s 重复 3 次全部通过；all-plane 追平速率中位数为
  249.686k/s，Polars 追平速率中位数为 248.920k/s。
- 300k/s 在多轮测试中接近边界且对共享主机抖动敏感；本报告不把它
  声明为硬容量保证。

因此，本次证据支持“250k/s 在该配置下可重复稳定”，不支持
“400k/s 或以上可持续”。这不是硬件无关的产品上限。

## 测试边界

端到端起点是 `FastTickPipelineV1::IngestForTest` 的 callback-admission
起点。它与生产 SDK callback 共用 message inspection、owned ingress、
两条 decoder lane 和 FAST-first 三路 fan-out，但不包含 Vendor SDK 的
网络、调度和 callback dispatch。

Event 读取经过真实的进程内 `InstrumentDataServiceV3`：

```text
binary MDL message
  -> callback admission / owned copy
  -> decoder lane
  -> FAST-first Tick/Event/KLine workers
  -> Event stable root / INSERT CDC
  -> InstrumentDataServiceV3
  -> Python DerivedEvent validation
  -> immutable Polars blocks
  -> cumulative DataFrame tail read
```

当前 Wire V3 没有跨进程 transport adapter，因此测试不包含 socket、
shared memory 或其他跨进程序列化成本。滚动测试只覆盖正常有序的
INSERT CDC，不覆盖晚到数据的 RANGE_REPLACE 修复。

## 环境和负载

- 分支：`codex/fast-source-event-reorder`
- 构建：Release，Ubuntu 22.04 兼容的 GCC 13.4.0
- CPU：AMD EPYC 9354，进程限制到 NUMA node 0 的 CPU 0–31
- Python：3.10.12
- Polars：1.43.2
- instrument：32（16 上海、16 深圳），静态 round-robin
- worker：Tick/Event/KLine 各 8 个
- affinity：24 个 route-worker slot 各占一个独立 CPU；Event/KLine 的
  repair thread 与各自 live worker 共用该 slot（本测试中 repair thread
  停驻）；剩余 8 个 CPU 供两条 decoder lane、paced producer 和
  Python reader 使用
- queue：每条 decoder source lane 65,536 条；每个 plane
  source/worker queue 65,536 条
- Event CDC batch：1,024 条
- Polars block：4,096 行
- 持续时间：每档 3 秒
- 消息混合：50% 上海 4.101.24 Trade Tick，50% 深圳 6.101.36
  Transaction Trade；每个成功输入在本负载中产生一行 Event

主机不是专用裸机，未隔离 IRQ 或其他系统进程，所以报告重复次数和
观测范围，不把单次最好值当作容量。

## 400k–800k/s 吞吐矩阵

下表计数取自终止状态被确认时。`FAST drop` 是 FAST coverage 已失效后
的 fail-close drop，不是 FAST queue full。
“实际 callback/s”按成功 accepted message 除以 producer elapsed 计算；
目标速率仍按所有计划 callback attempt 调度。

| 目标 msg/s | 实际 callback/s | 计划消息 | callback 接受 | decoder queue full | 已 decode | FAST drop | coverage 不完整 instrument | 结果 |
|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| 400,000 | 400,004 | 1,200,000 | 1,199,993 | 7 | 1,127,568 | 72,425 | 7 | 失败 |
| 500,000 | 500,000 | 1,500,000 | 1,499,979 | 21 | 808,775 | 691,204 | 17 | 失败 |
| 600,000 | 600,001 | 1,800,000 | 1,799,976 | 24 | 840,537 | 959,439 | 19 | 失败 |
| 700,000 | 675,192 | 2,100,000 | 2,099,968 | 32 | 717,499 | 1,323,099 | 22 | 失败 |
| 800,000 | 799,932 | 2,400,000 | 2,399,967 | 33 | 680,158 | 1,719,809 | 24 | 失败 |

所有档位的以下计数均为零：

- owned ingress message rejection；
- 其他 ingress error；
- FAST plane queue failure；
- Event plane queue failure；
- KLine plane queue failure；
- Event/KLine rebuild attempt。

这证明本矩阵首先撞到的是两条 source decoder admission queue，而不是
Event 或 KLine worker queue。第一次 ingress 缺口已经足以使受影响
instrument 的原始历史不完整；随后出现的大量 fail-close drop 是结果，
不能误解为大量彼此独立的 queue overflow。

400k/s 额外重复 3 次：

| 重复 | 实际 callback/s | all-plane 追平/s | coverage 完整 | 98% 目标 |
|---:|---:|---:|:---:|:---:|
| 1 | 400,007 | 387,202 | 是 | 失败 |
| 2 | 400,004 | 无完整追平 | 否 | 失败 |
| 3 | 400,003 | 无完整追平 | 否 | 失败 |

一次短突发可以被 queue 吸收不代表可持续吞吐。判定使用最终追平速率，
并要求 callback 和 native all-plane 均达到目标的 98%。

## 稳定负载下的滚动 Event 延迟

延迟定义为每条 Event 的 callback-admission monotonic timestamp 到其所在
CDC batch 已写入累计 immutable Polars block table，并能从 cumulative
DataFrame 读到 tail 的时刻。一个 batch 内的行共享同一个可见时刻，这是
批量 publication 的真实语义。

250k/s、32 instruments、两个 reader probe、每次 3 秒，重复 3 次全部
通过。每次共有 46,876 条 probe Event 延迟样本。

| 指标 | 三次分位数的中位数 | 三次观测范围 |
|:---|---:|---:|
| p50 | 8.305 ms | 8.035–8.453 ms |
| p99 | 46.573 ms | 24.601–71.079 ms |
| p99.9 | 58.349 ms | 42.242–94.384 ms |
| max | 61.396 ms | 48.130–100.272 ms |

对应吞吐中位数：

- callback：250.004k/s；
- native Event：249.686k/s；
- native all-plane：249.686k/s；
- Polars reader 完整追平：248.920k/s。

## Event 全历史读取延迟

全历史计时从 `AcquireEventStable` 的行数查询前开始，包含：

1. stable-size 查询和 ctypes 输出数组分配；
2. `InstrumentDataServiceV3` stable root copy；
3. Python `DerivedEvent` 模型构造；
4. `EventPolarsHistory` immutable blocks 和 DataFrame tail read。

它不包含进程启动、共享库加载和 client/bridge 创建。上海总是先测，深圳
随后测，因此“最新 callback 到 full read”在深圳样本中包含前一个上海
全历史读取的等待，不适合作为两个市场的横向比较。

### 23,438 行/instrument

250k/s 稳定负载，重复 3 次：

| 数据 | full read 中位数 | 范围 | preflight+分配 | native copy | Python+Polars |
|:---|---:|---:|---:|---:|---:|
| 上海 | 311.729 ms | 310.757–315.199 ms | 1.682 ms | 10.724 ms | 299.275 ms |
| 深圳 | 292.398 ms | 281.783–292.871 ms | 0.212 ms | 3.514 ms | 288.420 ms |

第一个全历史 attach（上海）从最新 callback 到 Polars 可读的中位数为
327.465 ms。绝大部分 full-read 成本位于 Python 模型与 Polars
materialization，而不是 native stable-root copy。

### 100,000 行/instrument

250k/s 总负载、8 instruments；两个 probe 各接收 31.25k Event/s：

| 数据 | full read | preflight+分配 | native copy | Python+Polars | 最新 callback 到可读 |
|:---|---:|---:|---:|---:|---:|
| 上海（先测） | 1,460.286 ms | 6.387 ms | 57.491 ms | 1,396.407 ms | 1,585.637 ms |
| 深圳（后测） | 1,389.451 ms | 0.894 ms | 49.350 ms | 1,339.208 ms | 3,056.108 ms* |

`*` 深圳的最后一列包含先执行的上海 1.46 秒 full read，不能解释为深圳
native 或 Polars 本身慢 3 秒。

在这个更高的单-reader 更新率和更大的累计表上，滚动 callback→Polars
延迟为 p50 44.571 ms、p99 143.641 ms、p99.9 153.081 ms。这说明滚动
延迟不仅取决于总市场 message/s，也取决于一个 reader 实际订阅的
instrument 数、Event/s、batch 和累计 block 数。

## 原始结果和复现

最终原始 JSON：

- `artifacts/callback-polars-v3-400k-800k-attributed-final.json`
- `artifacts/callback-polars-v3-400k-final-rep3.json`
- `artifacts/callback-polars-v3-250k-attach-final-rep3.json`
- `artifacts/callback-polars-v3-full-history-100k-attach-final.json`

`artifacts/` 是生成目录。复现命令和完整 scope 说明见
`benchmarks/README.md`，驱动为
`benchmarks/run_callback_polars_latency.py`。

## 后续性能工作建议

本轮只测试，没有改变生产数据路径。按证据优先级，下一步应分别测量：

1. 两条 decoder lane 的 service time、queue depth 和高水位；
2. raw body inspection/copy、decode、catalog identity 和 `RouteDecoded`
   的分段周期；
3. Python model construction 与 Polars block construction 的分项 allocation；
4. 不同 batch/block 大小下的 callback→Polars 尾延迟；
5. 真实 4.24/6.33/6.36 消息比例和 instrument 热度分布；
6. 将来跨进程 Wire V3 transport 完成后的新增传输成本；
7. 晚到 Event RANGE_REPLACE 的独立延迟矩阵。
