# Phase 6 local acceptance record（本地代码范围）

Date: 2026-07-22

> 2026-07-22 superseded note：`L2Flow::production` 已由独立授权变更切换到
> `l2flow_production`，但当前正式组合不含 Phase 6 Latest State writer/endpoint。
> 本记录的 formal exit 判定不变。

## 结论

本记录中的“完成”只指仓库内 `L2Flow::phase6` 构造切片，不等同于
`docs/design.md` 的正式 Phase 6 Exit；截至本记录日期，它也不表示已接入默认
ingress service。

| 口径 | 本记录时状态 | 判定依据 |
| --- | --- | --- |
| Phase 6 local library slice | 已实现 | opaque Latest State slot、Snapshot/tick-quality publication、stale read、batch/local query、checkpoint codec/restore |
| Scoped local verification | 已通过 | Debug 与 Release 的 Phase 6 CTest 3/3；sanitizer 结果见下文 |
| Production integration/cutover | 未完成 | 本阶段工作当时没有 state-writer service、SHM generation cutover、UDS server 或 production alias 变更 |
| Formal Phase 6 exit | 未完成 | 1000 证券 p99、vendor snapshot shadow、真实进程 crash/recover 制品不存在 |

## 本地实现范围

`LatestStateSlotV1` 固定 4096 bytes、64-byte alignment，且是 opaque ABI。
所有并发共享 word 通过 `__atomic_*` 访问；C ABI stable-copy 和 seqlock 负责
一致 snapshot，不允许调用方并发 `memcpy` 或 reinterpret 为 `std::atomic`。
首个完整 Canonical Snapshot 初始化 slot；之后 exact config、来源与 generation
identity 不可原地更换。旧 cursor、同 cursor 冲突、错误 schema/registry/shard 均
fail closed，exact duplicate 为无写入幂等结果。

Snapshot 每次替换完整固定 payload，所以 depth 10→3、queue 50→2 和 null validity
都会清除旧尾部。Tick 只更新独立 quality lineage/cursor，不修改 snapshot payload 或
snapshot quality。Stale 只由 caller 提供的同 clock identity 当前时间与逐 phase 阈值
计算；库不自行发明交易阶段时长，返回的 `SNAPSHOT_STALE` 也不回写 slot。

`LatestStateLocalQueryV1` 提供 single/batch/market scan，但只保存排序后的
`(instrument_id, slot*)` directory；mapping 由 caller 拥有并必须更长寿。当前没有
UDS/gRPC server、Python binding、远程限流或 state-writer process。

Checkpoint 是 deterministic little-endian codec，包含固定 header、排序 slot、CRC32C、
payload SHA-256 与 logical state SHA-256。Logical market-state hash 排除 seqlock、输出
table writer/generation mechanics 和 display-only clock labels，但保留 source lineage、
clock algorithm/full digest、cursor、quality、validity 与 payload；payload hash 仍绑定
wire 中保留的 table identity/label。Restore 只接受 exact config 和 exact all-zero
target set，使用 fresh even sequence，不覆盖 live generation。

默认 checkpoint 明确 non-durable。Durable assertion 必须在 state writer quiesced 的
共同 cut 上捕获，并为每个实际 Snapshot/tick-quality Raw namespace 提供 exact numeric
barrier；位置必须来自 validated Raw journal/control authority，而不是 Canonical
processed/frontier。Codec 能检查 identity、cursor 与数值关系，但 caller 的 quiescence、
Raw authority receipt 和 table cutover 不是 non-forgeable proof。

## 定向测试与结果

CTest targets：

```text
test_phase6_latest_state
test_phase6_state_checkpoint
test_phase6_c_abi
```

覆盖 4096/64 ABI 与 lock-free preflight、concurrent writer/readers 无 torn value、完整
lineage、cursor conflict/idempotence、depth/queue/null 清尾、Snapshot 原子替换、tick
quality 分离、clock/stale policy、batch/local query、checkpoint corruption、同一 stable
slot image 的 encode/decode、writer-quiesced common cut、exact durability barrier、
clean-target restore、logical hash、replay equivalence与纯 C11 header/ABI 编译调用。

| 配置 | 命令口径 | 结果 |
| --- | --- | --- |
| Debug | `ctest --test-dir /tmp/l2flow-phase67-debug -L phase6 --output-on-failure` | 3/3 passed |
| Release | `ctest --test-dir /tmp/l2flow-phase67-release -L phase6 --output-on-failure` | 3/3 passed |
| ASan+UBSan | `ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 ctest --test-dir /tmp/l2flow-phase67-asan -L phase6 --output-on-failure` | 3/3 passed；当前 ptrace 环境不能运行 LSan，未声称 leak-check 通过 |
| TSan | strict target 可编译；当前容器运行时在测试主体前报 `ThreadSanitizer: unexpected memory mapping` | 不是执行通过证据 |

## 未完成的正式 Exit gate

仍需至少：

1. 目标机 1000-symbol batch read p99（设计目标不能由单元测试推断）；
2. 完整交易日逐字段 vendor snapshot shadow 对账；
3. 真实 SHM/state-writer 进程的随机 crash、checkpoint+tail 与从头 replay hash；
4. atomic checkpoint store、周期调度、SHM generation manifest/cutover 与 UDS wrapper；
5. live Phase 5 FATAL 后外部 owner 丢弃整代 derived state 的服务级证据。

这些制品仍是 Phase 6 external exit 的未完成项；本记录当时因此保持
`L2Flow::production` 为 `l2flow_phase01`。后来的独立 alias 变更不完成这些制品。
