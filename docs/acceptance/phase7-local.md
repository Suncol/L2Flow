# Phase 7 local acceptance record（本地代码范围）

Date: 2026-07-22

> 2026-07-22 superseded note：`L2Flow::production` 已由独立授权变更切换到
> `l2flow_production`，但当前正式组合中的 C++ instrument-history workers 不是
> Phase 7 factor workers，也不运行本文的 placeholder plugins。本记录的 formal
> exit 判定不变。

## 结论

本记录只接受仓库内 native consumer/factor primitives 与 Python transaction runtime
构造切片。它不把五个 passthrough placeholder 写成数学因子，也不宣称正式 Phase 7
Exit 或 production deployment。

| 口径 | 本记录时状态 | 判定依据 |
| --- | --- | --- |
| Native Phase 7 slice | 已实现 | committed batch/C ABI、safe mux/as-of wrappers、watermark/barrier、Latest Factor、process-local table |
| Python infrastructure slice | 已实现 | immutable-buffer manual batch view、FactorSpec、16-way owner、transaction rollback、windows、checkpoint |
| 首批五个名称 | 仅占位 | 全部为 `PASSTHROUGH_PLACEHOLDER`，return 原始 leased records，factor scalar 永远 invalid |
| Scoped local verification | 已通过 | Debug/Release combined Phase 6–7 CTest 9/9；Python unittest 结果见下文 |
| Formal Phase 7 exit | 未完成 | 无五因子数学实现、完整日 equality、2x 性能、真实随机 crash 与 native Python deployment |

## Native consumer、watermark 与 Latest output

Batch attach pin event family/fixed size/schema/dtype/registry；C open 还以 exact descriptor
固定 trade/capture day、source/stream day、Raw writer/generation、full clock 和 Canonical
generation。`Peek` 通过 Phase 5 committed reader 返回 mmap-backed const span，首条
physical-but-unprocessed record 是正常 `WouldBlock`，不误报 corruption。Plugin 未
Commit 时可重取同一 `[begin,end)`；`Commit` 只推进一次，并在尾记录重读 live
SourceFrontier，later FATAL 会拒绝且 cursor 不动。Raw observer/result 还复用 Phase 2
cursor-shape validator；`observed_raw_durable` 只是 metadata。

Safe mux 与 snapshot-as-of 的 C ABI 调用 Phase 5 proof helper，没有在 Python 重写
排序。Watermark identity 的 C++/Python frozen preimage 排除 run-local ID、clock label
和 observed durable，保留 sorted exact keys、exclusive cursor、max Raw WAL、clock
algorithm/full digest 与 quality。C++ durability barrier 验证 exact Raw namespace 与
Raw control cursor shape，复杂度为 `O(A log A + N)`；Python mapping 则是 caller 已
认证 position 的信任边界。

Latest Factor 是 4096-byte/64-aligned opaque atomic-word seqlock slot，slot identity
不可重绑，as-of 单调，同 key conflicting output 被拒绝。Watermark table 是
process-local append-only structure。若 retained Latest slot 跨进程重启，必须先以
checkpoint 恢复同一 run-local ID→full map，或一起轮换到新 slot/table generation；
否则旧 ID 不可解析，当前库不会假装它持久存在。首选 C++ publish path 会解析 caller
已先 append 的 exact watermark 并拒绝 orphan ID/hash；append 仍是独立步骤，low-level
C slot ABI 的关联仍是 caller protocol。C ABI 也在写入前拒绝 slot/value/result
storage overlap。

## Python runtime 与明确限制

Python V1 定义 exact Tick/Snapshot dtype，manual attach 只接受 bytes-backed immutable
array，避免 retained writable alias 在 validation 后改 bytes。Leased subclass 的普通
访问在 context exit 后 fail closed；显式 copy 可独立存活。纯 Python 无法撤销任意
已经逃逸的 NumPy base pointer，因此真实 zero-copy path 仍需持有 native view handle
与 read-only mmap 的 adapter。

FactorSpec canonicalizes/hashes inputs、windows、validity/quality policies 与 modes。
`LIVE_LATEST` 必须显式标 non-deterministic。`FactorTransactionRuntime` 是一个 shard
owner 的 transaction primitive：plugin exception 恢复 state 且不推进 cursor；所有
allocation-bearing map/state 在 final commit callback 前 staged；跨输入 run identity
与 per-input generation 被固定；successful batch 的重复不再次执行 native Commit。
每个成功 transaction 的 run-local `watermark_set_id` 必须严格递增；runtime 不缓存
transaction 或 output object，output identity 只供外部 sink 做幂等。
它不是 receive-time mux/as-of scheduler、policy/warmup engine、timer/cadence engine，
也不启动或监管 16 个 OS workers。当前 Python native mux seam 在 adapter 缺失时明确
fail closed。

Checkpoint 不使用 pickle；canonical JSON 拒绝 duplicate/noncanonical/type-confused
input，保存 code/config/state schema/registry 与 full watermark。已知 runtime codec
把 decoded factor ID/version、exact FactorSpec hash、registry 和完整 watermark/state map
与外层 envelope 互相绑定；generic codec 必须由 caller 显式确认 binding。Publish/load
都重查逐 Raw namespace barrier。文件发布为 temp write→fsync→replace→directory fsync；
load 用 no-follow fd、fstat bound 与 exact identity。Generic durable-position provider
仍是 caller trust boundary，不是数字签名或 Raw receipt。

## 五个 placeholder

```text
book_imbalance
microprice
trade_imbalance
cancel_rate
trade_intensity
```

每个 placeholder 都返回当前 context 内的 exact 原始 record view，同时返回完整
provenance digest/idempotency key；`status=PASSTHROUGH_PLACEHOLDER`、
`factor_value=None`、`factor_value_valid=False`。没有 denominator、overflow、rounding、
rolling math 或 as-of factor calculation，因此不能把相关测试或正式 exit 写成通过。

## 定向测试与结果

Native/CTest targets：

```text
test_phase7_consumer_batch
test_phase7_consumer_c_api
test_phase7_factor_watermark
test_phase7_latest_factor
test_phase7_c_abi
test_phase7_python_runtime   # aggregates unittest discovery
```

| 配置 | 命令口径 | 结果 |
| --- | --- | --- |
| Debug | `ctest --test-dir /tmp/l2flow-phase67-debug -L phase7 --output-on-failure` | 6/6 passed |
| Release | `ctest --test-dir /tmp/l2flow-phase67-release -L phase7 --output-on-failure` | 6/6 passed |
| Python 3.10.12 + NumPy（当前容器） | `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=python python3 -m unittest discover -s tests/python -p 'test_*.py' -v` | 22/22 passed |
| Target Python 3.11 | `/opt/gurobi1201/linux64/bin/python3.11` 存在但没有 NumPy | 未执行，不是通过证据 |
| ASan+UBSan | combined Phase 6–7 CTest with `detect_leaks=0`, ASan/UBSan halt-on-error | 9/9 passed；当前 ptrace 环境不能运行 LSan，未声称 leak-check 通过 |
| TSan | strict C/C++ targets 可编译；当前容器运行时 mapping failure | 不是执行通过证据 |

## 未完成的正式 Exit gate

仍需至少：

1. 定义并实现五个因子的 reviewed math/dtype/null/rounding/as-of semantics 与测试；
2. 同一 Raw 完整日 offline/live factor equality（bitwise 或预先冻结 tolerance）；
3. 真实 C ABI/pybind11 Python reader/mux adapter 与 16 worker supervisor；
4. FactorSpec validity/quality/gap/epoch/warmup/cadence 的完整 scheduler/executor；
5. 目标机 2x peak 无持续 lag、Python hot path 无 per-event crossing 的 profile；
6. process crash/restart 最终 output hash、persistent watermark retention 与 Latest/table
   restart generation protocol。

这些证据仍是 Phase 7 external exit 的未完成项；本记录当时不据此改变
`L2Flow::production`。后来的独立 alias 变更不完成这些证据。
