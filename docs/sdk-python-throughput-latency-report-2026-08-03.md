# SDK callback 到 Python Polars 吞吐与延迟报告（2026-08-03）

## 结论

本机、16 个逻辑 CPU 的约束条件下，不能把“500k/s 已通过”作为整条 Event 链路的结论：

- 正式隔离 campaign 中，`from open + FAST/History` 在 300k、400k、500k message/s 均 3/3 无损通过；另一次交错补充运行显示 500k 的瞬时积压裕量并不宽，见下文局限。
- `from open + Certified Event` 在 300k、400k 均 3/3 稳态通过；500k 虽然 3/3 最终完整闭合、没有丢弃或冻结，但 Event handoff 队列持续增长，完整链路等效吞吐 p50 只有 462,445 message/s，因此严格判定 0/3。
- `partial no recovery + Partial Event V2` 在 300k、400k、500k 均把 262,144 handoff 队列打满并冻结 Event 支路，完整链路 0/3；FAST/History 仍被故障隔离并全部无损。
- `partial with online recovery` 的 65,536 条 live-journal 队列在三个目标速率下都先耗尽，完整链路 0/3。300k 时也只能接受约 7.6 万条后失败。

因此，用户提出的二分问题需要分两层回答：

1. **500k 的 Event 瓶颈不是 Partial 独有。** Certified 完整 Event 链路同样无法稳态承载 500k。
2. **Partial 还有额外且更严重的特有瓶颈。** Certified 在 300k/400k 可追平，Partial 却在 300k 已硬性队列溢出。队列容量差异不能单独解释这个差距：Certified 的所有采样积压在 300k/400k 分别不超过 4,734/1,175，远低于 Partial 的 262,144 容量；500k 的最高采样值 153,829 也低于该容量。这里的“最高”是五个时点的采样最高值，不是未插桩的连续高水位。

## 判定口径

吞吐测试使用序列化 SDK callback、one-based absolute deadline pacing，每次运行 3 秒、每个速率 3 个新进程，工作负载为五类生产 tuple 均匀循环。300k/400k/500k 每次分别计划 90 万/120 万/150 万 callback。

完整通过必须同时满足：

- 实际 offer rate 不低于目标的 98%；
- invoked、accepted、decoded、applied、Store、History 扫描条数逐项精确闭合；
- 没有 reject、queue-full、drop、fatal、coverage loss；
- pipeline 和 Event 队列在 25%/50%/75%/100% 以及 offer 结束处没有持续增长。最终积压预算为 `max(1024, ceil(target/1000))`，本矩阵中均为 1,024；
- Event 拓扑还必须核对 observation、applied handoff、processed handoff、Event generation frontier 和 derived Event 条数。

“最终能清空”不是稳态通过。特别是 Certified 500k 的三次运行都最终处理了全部 180 万 handoff，但 offer 结束时仍有十万级积压，所以仍判失败。

## 吞吐结果

三套主矩阵使用相同 Release 二进制 SHA-256：`e91542b8a8cd6070f9910fee868693f06669d7b4930114476ff4b47d38cdc764`。

### From open：FAST/History 与 Certified Event

| 拓扑 | 目标 msg/s | offered p50 | 完整链路 ready p50 msg/s | FAST/History 无损 | 完整链路通过 | offer 结束 Event 积压 |
|---|---:|---:|---:|---:|---:|---:|
| FAST/History | 300,000 | 299,999.664 | 299,805.670 | 3/3 | 3/3 | 0 |
| FAST/History | 400,000 | 399,999.658 | 399,822.609 | 3/3 | 3/3 | 0 |
| FAST/History | 500,000 | 499,999.500 | 499,717.341 | 3/3 | 3/3 | 0 |
| FAST + Certified Event | 300,000 | 299,999.545 | 299,851.669 | 3/3 | 3/3 | 0 |
| FAST + Certified Event | 400,000 | 399,999.478 | 399,755.147 | 3/3 | 3/3 | 0 |
| FAST + Certified Event | 500,000 | 499,999.351 | 462,444.793 | 3/3 | 0/3 | 103,222–153,804 |

Certified 500k 的 Event handoff 积压轨迹如下，三个重复都从前半段开始增长，而不是仅在结束瞬间出现一次抖动：

| 重复 | 25% | 50% | 75% | 100% | offer 结束 | 完整链路 msg/s |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 37,724 | 83,245 | 101,117 | 153,829 | 153,804 | 457,725.687 |
| 2 | 26,885 | 62,166 | 71,844 | 139,893 | 139,898 | 462,444.793 |
| 3 | 12,737 | 43,921 | 62,817 | 103,217 | 103,222 | 471,501.696 |

每次 Certified 500k 运行都精确闭合 150 万 callback、90 万 observation、90 万 applied handoff、180 万 processed handoff和 120 万 derived Event；drop、frozen channel、global frozen、Tick History lag 均为零。这证明失败类型是**稳态处理速率不足**，不是数据正确性或容量异常。

### Partial no recovery

| 目标 msg/s | offered p50 | FAST/History ready p50 | FAST/History 无损 | Partial 完整通过 | 成功入队 handoff 范围 | queue HWM / drop |
|---:|---:|---:|---:|---:|---:|---:|
| 300,000 | 299,999.652 | 299,810.212 | 3/3 | 0/3 | 271,153–271,243 | 262,144 / 1 |
| 400,000 | 399,999.503 | 399,823.038 | 3/3 | 0/3 | 274,461–297,367 | 262,144 / 1 |
| 500,000 | 499,999.389 | 499,679.073 | 3/3 | 0/3 | 276,745–277,661 | 262,144 / 1 |

九次运行的 Partial handoff queue high-water 都精确等于 262,144，`dropped_handoffs=1`，最终状态为 `FrozenResource`/`HandoffQueue`；Event 冻结后 FAST/History 继续运行并完整保留全部输入。这里不报告“首次 drop 对应第几个 callback”或“time-to-overflow”：native observation 在 callback 线程入队，而 applied handoff 由异步处理线程入队，两类 handoff 会交错，不能用总 handoff 数除以固定比例反推 callback 位置；当前遥测也没有记录首个 drop 的单独时间戳。

### Partial with online recovery

| 目标 msg/s | offer pacing 通过 | accepted before failure p50 | failure detection time p50 | journal HWM | 完整通过 |
|---:|---:|---:|---:|---:|---:|
| 300,000 | 3/3 | 76,033 | 254.294 ms | 65,536 | 0/3 |
| 400,000 | 3/3 | 73,217 | 183.681 ms | 65,536 | 0/3 |
| 500,000 | 3/3 | 71,169 | 143.873 ms | 65,536 | 0/3 |

这里的失败点是 active recovery 的 production-default live journal capture queue，不是 Polars，也不是上述两个 Event projector。该测试验证的是恢复期间“每个 callback 必须先被 journal 保留”的 admission 能力。表中时间从 pacing 起点算到 producer 检测 fatal 并停止，检测按固定 callback 间隔轮询，因此不是队列首次 full 的精确时间戳。

## SDK callback 到 eager Polars 延迟

延迟测试与吞吐测试使用不同的新进程，不叠加 300k–500k offer load。时钟为同机 `CLOCK_MONOTONIC`；起点紧邻 synthetic SDK `OnMessage` 调用之前，终点为 Python 构造 eager Polars DataFrame、到达显式 EOF 并完成完整性校验之后。每个进程只取 sample 0，避免把同进程重复值当成独立样本。

范围边界：本节的 from-open 延迟拓扑是 FAST/History → Python Polars，没有实例化 Certified worker；`derived order` 是 Python 从发布的 History 构造的完整订单生命周期。Partial 延迟拓扑会经过 Partial Event V2 applied/native handoff 并同时核对其健康状态，但 Polars 终点仍是 FAST History。因而本报告新增的 Certified 对照回答的是 300k–500k **吞吐**问题，不应把下表解释为 Certified Event→Polars latency。

| 场景 | 工作负载 | n | p50 | p95 R7 | p99 R7 | max |
|---|---|---:|---:|---:|---:|---:|
| from open | 4,096 records × 55 columns | 10 | 46.896 ms | 51.288 ms | 51.762 ms | 51.881 ms |
| partial no recovery | 4,096 records × 55 columns | 10 | 40.179 ms | 42.977 ms | 43.276 ms | 43.351 ms |
| from open | 4 callbacks → 6 Python derived-order rows | 10 | 24.766 ms | 26.732 ms | 27.032 ms | 27.107 ms |
| partial no recovery | 4 callbacks → 6 Python derived-order rows | 10 | 20.700 ms | 21.961 ms | 21.987 ms | 21.993 ms |

raw 端到端 p50 中，from-open 的 callback→publication 为 7.452 ms、publication→Polars 为 39.949 ms；partial 分别为 5.885 ms 和 33.961 ms。derived-order p50 中，from-open 两段为 4.185 ms + 20.522 ms，partial 为 5.385 ms + 15.186 ms。分段和总数会因每次时间边界细微差异而不严格等于四舍五入后的总 p50，不能把两个独立分布的 p50 直接相加当成总分布 p50。

Partial latency 小样本路径还验证了 4,100 个 Event-eligible applied/native observation、8,200 个 handoff 全部处理，队列 sampled/HWM 为 69、无 drop/freeze。它证明低负载功能链路完整，不能推翻高负载吞吐矩阵中的队列耗尽结论。

### Online recovery 到 Polars

恢复延迟运行先有 16 条 warmup，再在恢复进行期间执行 64 条 measured callback；提升后 Python 读取完整 replay + 80 条 live records，构造 55 列 eager Polars 并验证 dense ingress 与 EOF。每个 backlog 有 5 个新进程。

| replay backlog | records read | strict last callback→Polars p50 | p95 R7 | p99 R7 | recovery p50 | publication→Polars p50 |
|---:|---:|---:|---:|---:|---:|---:|
| 50,000 | 50,080 | 709.928 ms | 711.866 ms | 711.891 ms | 291.655 ms | 419.060 ms |
| 500,000 | 500,080 | 5,869.424 ms | 6,022.376 ms | 6,049.562 ms | 3,504.486 ms | 2,399.371 ms |

## 长时限制

另做了一次 from-open FAST-only 500k/s、计划 60 秒的补充 soak。它在约 19.021 秒、调用 9,510,400 个 callback 时因 Store `kByteCapacity` 失败，并非 CPU 速率短缺。配置的 logical byte quota 是 32 GiB；失败时已保留 record payload accounting 17,097,531,166 bytes，quota 还会计算分段分配和基础索引，因此 payload 计数低于 32 GiB 与 `kByteCapacity` 并不矛盾。该单次补充结果意味着：3 秒 500k 通过只能证明短窗处理能力，不能证明当前 32 GiB 测试配置可承载 60 秒或全天。

## 环境、局限与复现

- 主机：AMD EPYC 9354，2 sockets；测试进程限制到 CPU `8-23`（16 logical CPUs），未取得独占主机隔离。Linux 6.8.0-124，governor `schedutil`，boost 开启。
- Python 3.10.12，Polars 1.32.3；Release/GCC 13 构建。
- synthetic callback 是序列化调用，不包含真实行情网络、SDK 内部线程调度或多线程 callback 并发。
- 每个吞吐点只有 3 个重复、每次 3 秒；每个普通延迟点 10 个重复、恢复延迟点 5 个重复。R7 分位数是描述统计，不是置信区间。
- 500k Certified 的队列采样只覆盖五个时点；持续上升轨迹和最终十万级积压足以否定稳态通过，但不能替代连续 queue-depth tracing。
- 在正式 FAST-only campaign 之外，一个相同二进制、与 Partial 交错执行的补充 500k run 出现 offer 结束 pipeline backlog 1,949，超过 1,024 门槛；150 万条仍全部无损闭合，drain 约 1.63 ms。同一二进制的六次 FAST 500k run 合计为 5 次严格通过、1 次只因瞬时 backlog 门禁失败，说明非独占主机上的 500k FAST 结果存在调度抖动，不能解释成充足的生产安全余量。

最终验证：Release 全量构建成功；CTest 54/54 通过；benchmark parser/harness unittest 33/33 通过；notebook 由本地 Jupyter kernel 无错误执行；独立于 campaign parser 的第二套闭合审计重新核对了 27 条正式吞吐 run 的 ingress、tuple、History 和 Event 基数，全部通过。

正式原始结果：

- [FAST/from-open summary](../artifacts/perf-20260803-fast-current-w0/summary.json)
- [Partial Event summary](../artifacts/perf-20260803-partial-current-w0/summary.json)
- [Certified Event summary](../artifacts/perf-20260803-from-open-certified-w0/summary.json)
- [Callback→Polars summary](../artifacts/perf-20260803-sdk-python-w0/summary.json)
- [Online recovery summary](../artifacts/perf-20260803-online-recovery-w0-v2/summary.json)
- [FAST 500k 瞬时 backlog 补充日志](../artifacts/perf-20260803-fast-partial-current-w0/throughput_500000_from_open_fast_run3.log)
- [可复现分析 notebook](../benchmarks/sdk_python_performance_report.ipynb)

主要命令：

```bash
cmake --build build-online-final-native-gcc13 -j 8
ctest --test-dir build-online-final-native-gcc13 --output-on-failure
.venv/bin/python -m unittest benchmarks.test_benchmark_harnesses

.venv/bin/python benchmarks/run_startup_mode_dataflow.py \
  --binary build-online-final-native-gcc13/test_realtime_shared_service_v2 \
  --output-dir artifacts/OUTPUT --cpu-list 8-23 \
  --scenarios from_open --rates 300000 400000 500000 \
  --throughput-duration-ms 3000 --throughput-repeats 3 \
  --latency-repeats 0 --sink fast_certified \
  --record-capacity-failures

.venv/bin/python benchmarks/run_online_recovery_performance.py \
  --binary build-online-final-native-gcc13/benchmark_online_recovery_fast_v1 \
  --output-dir artifacts/OUTPUT --cpu-list 8-23 \
  --rates 300000 400000 500000 --latency-backlogs 50000 500000
```
