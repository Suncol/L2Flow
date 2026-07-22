# Phase 3 local acceptance record（本地代码范围）

Date: 2026-07-21
Host scope: local Linux x86-64 development container

> 2026-07-22 superseded note：本记录保留截至 2026-07-21 的本地验收事实。
> `L2Flow::production` 后来通过独立授权变更切换到 `l2flow_production`，并把
> control decoder 接入 fresh live 四源聚合；本文未完成的 replay/recovery 与外部
> exit 不因此变成通过。

## 结论

本记录把“可在仓库内验证的 Phase 3 library/live bridge”与“生产 ingress 已完成
Phase 3 接入”严格分开。本记录当时的结论是：Phase 3 的独立 control decoder、固定记录、
checkpoint、readiness、`ReSubscribe` guard 和 live worker 本地实现切片及其定向
测试已完成；生产 service/controller 接入和外部 Exit 证据未完成。

| 口径 | 本记录时状态 | 判定依据 |
| --- | --- | --- |
| **Phase 3 library/local live-bridge implementation slice** | **已完成** | `L2Flow::phase3` 包含安全 API/SYS decoder、authoritative state、固定 codec、checkpoint、READY gate、guard 和单消费者 live worker |
| **Production implementation complete** | **未完成** | 截至本记录日期，`L2Flow::production` 和 `l2flow_ingress_service` 仍停留在 Phase 0–1；没有生产 controller 组合 Phase 2 recovery、SDK generation lifecycle、Phase 3 worker、derived sink 与 checkpoint restore/publication |
| **Scoped local verification** | **已完成** | 本记录所验代码的 strict Debug、Release 与 ASan+UBSan `phase3` label 定向 CTest 均为 **5/5，0 failed**；该结论只覆盖下列五个仓库内测试 |
| **Production/external Phase 3 exit** | **未完成** | 没有真实四端点、五次真实断线/重连、生产服务重启、真实 SDK data plane、目标 NVMe 或完整交易日制品；fixture 测试不能替代这些证据 |

因此，本文中的“已完成”只指第一行的独立库/本地 bridge 切片和第三行列明的
定向验证范围；它在当时不构成切换 production alias 的授权，也不把 Phase 2 或整个系统的
Implementation/Local verification/Exit 状态改为完成。

## 已实现的本地切片

### Raw 顺序上的 authoritative control state

`ControlDecoderV1` 在同一条验证后的 Raw `ingress_sequence` 顺序中处理 API、SYS
和 market 记录。安全 decoder 在访问相对 string/list 前执行完整 body 边界与
checked-arithmetic 校验，不使用 vendor accessor 充当边界检查。状态以
`(capture_date, source_stream_id, stream_day_id)` 为 namespace，并实现：

- 首次成功 LogonResponse 把 `connection_epoch` 从 0 改为 1；失败登录不递增，
  Disconnected 属于旧 epoch，下一次成功登录才递增；
- 成功 requested-key set 真正改变时，`subscription_epoch` 对每个完整响应至多
  递增一次；required 和 optional policy 保持分离；
- 登录前 market 为 epoch 0 + `SESSION_UNKNOWN`；断线窗口保留旧 epoch，并带
  `SESSION_UNKNOWN|SOURCE_DISCONNECTED`；
- malformed API/SYS control body 提交有界 `DECODE_ERROR` 诊断并永久 poison 当前
  namespace；它撤销 READY，但 decoder 继续沿 Raw 扫描，不停止或修改 Raw 捕获；
- full replay、checkpoint restore + suffix replay 和 live 使用同一个状态转移与
  canonical state hash，不依赖 wall clock、process identity 或 unordered iteration。

### 固定 ControlRecord 与生成 gate

Phase 3 使用独立的 256-byte little-endian `ControlRecordV1`，不是 Phase 5
Canonical record。reserved bytes、enum/flag、CRC、schema identity 和 golden
bytes 均由显式 codec 验证。CMake 运行
`tools/generate_phase3_control_schema.py`，同时校验
`schemas/control_record_v1.json` 与
`schemas/control_checkpoint_v1.json`，并生成编译所用 constants header。

四个冻结制品的当前 SHA-256 与
[`docs/decisions/phase3.md`](../decisions/phase3.md) 一致：

| 制品 | SHA-256 |
| --- | --- |
| `schemas/control_record_v1.json` | `0a49233912fde159bd238b38b8bf2c3aca921432826f2ea7fa4168aaed74b14b` |
| `schemas/control_checkpoint_v1.json` | `90b70205e11edcc9f01ebf5cd7f6c0090e4d03420fde355b15a23305820147a2` |
| `tools/generate_phase3_control_schema.py` | `0b776cf2ebf9edb40cb24c729da75bb711ebbbe76dc06cbe7a22af3eb228cabc` |
| generated `phase3_schema_v1_generated.h` | `45653663437581331265c376f620d3572fd3ba9da9be1d0585775ebbf754e53a` |

### Checkpoint 与 restart discovery

Checkpoint 是可丢弃的 replay 优化，不是 Raw authority。codec 绑定 Raw namespace、
exclusive processed record-end cursor、effective config、requested manifest、完整
有序状态与 state hash。模型验证除 wire canonicality 外还拒绝计数、epoch、事件
拓扑或 cursor 不可达的 hash-consistent 状态。

POSIX store 使用 owner-only retained directory、`O_EXCL` temporary、完整 write、
file `fsync`、no-replace rename 和实际 parent-directory `fsync`。restart discovery
只读扫描并校验 inode/mode/link/filesystem、canonical bytes、模型化 filename、
durable upper bound 和严格 cursor chain。成功 discovery 或 publication receipt
都不授权跳过 Raw：caller 仍须定位准确的 validating durable Raw boundary，再调用
`ControlDecoderV1::Restore`。

### READY、`ReSubscribe` 与 live worker

READY gate 使用 authoritative control state、当前 Connect generation 的登录和
required-market evidence、decoder/capture health，以及一次 fresh coherent Raw
append/durable sample。读取 Raw control 失败时明确 NOT_READY，不能复用缓存结果。

Phase-3 V1 的 `ReSubscribe` guard 会验证 maintenance window、durable audit receipt
和 authorization，但由于 Raw 中尚无完整 proposed replacement manifest/policy 的
有序 intent，最终仍返回 `NO_REPLAYABLE_MANIFEST_TRANSITION`，不会调用 SDK。这是
避免 live/replay 分叉的 fail-closed 结果，不是 production re-subscription 能力。

`ControlLiveWorkerV1` 完成以下本地 live seam：

- construction fresh-sample 必须与 tail attach 和 decoder cursor 精确相接；旧 backlog、
  fatal/畸形 Raw frontier、zero decoder 非 genesis attach 均拒绝；
- 前一 SDK generation 的 callback 必须先静默，`Create` 成功后才允许新的 Connect；
  当前 generation 登录证据还要求 Raw origin 不早于 attach first sequence 且 control
  generation 新于 construction sample；
- 每个 emitted record 先编码为 canonical 256-byte wire，再由同步 sink 按绝对
  monotonic deadline 确认 replay-safe retention；conflict/failure/late return fail-stop；
- sink acknowledgement in-flight、未健康启动或 fatal worker 不暴露 checkpoint；
- `StopAt` 只接受精确 terminal cursor 并在 catch-up 后正常退出，`Abort` 用于异常
  退出；service 必须 join `Run` 后再销毁 worker 和 borrowed Raw source。

这些是 production-capable component contract，不是 production service 已经完成
construction、thread ownership、SDK Connect/Shutdown、monitor 或 restart orchestration
的声明。

## 当前定向验证结果

当前最终代码分别在 strict Debug、Release 和 ASan+UBSan Debug build 中执行
`phase3` label suite。结果如下：

```text
strict Debug:       5/5 passed, 0 failed
strict Release:     5/5 passed, 0 failed
ASan+UBSan Debug:   5/5 passed, 0 failed
```

测试项为：

```text
test_phase3_control_decoder
test_phase3_control_core
test_phase3_control_live_worker
test_phase3_control_checkpoint_reachability
test_phase3_control_checkpoint_posix_store
```

该组测试覆盖首次/失败登录、旧 epoch disconnect、五轮 fixture 断线重连、
required/optional 状态、malformed offset/list、live/replay/checkpoint hash、READY
撤销、fresh sample、generation 隔离、sink 幂等/冲突/超时、精确 stop，以及
stop cursor 对齐/记录字节/segment-header 算术拒绝路径和 checkpoint
wire/store/discovery/reachability 拒绝路径。五轮 fixture 的确定性断线重连从初始
epoch 1 得到最终 epoch 6、五个 disconnect 且 subscription set 不变时 epoch 不被
虚增。

ASan+UBSan 运行显式使用
`ASAN_OPTIONS=detect_leaks=0:halt_on_error=1:abort_on_error=1` 和
`UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1`，且没有 sanitizer 报告。因此该
结果不声明 LeakSanitizer、ThreadSanitizer、持续 libFuzzer、真实 SDK callback、
真实 filesystem power loss 或 production service E2E；只有实际执行并保留独立
制品的 suite 才能写入对应声明。

## Production implementation 与外部 Exit 阻断项

以下条件截至本记录日期均未完成，且不能由上述 5 个测试替代：

1. 截至本记录日期，`L2Flow::production` alias 仍指向 `l2flow_phase01`，
   `l2flow_ingress_service` 仍链接 Phase 0–1；四个 ingress 未构造 Phase 2 Raw
   production runtime + Phase 3 worker。
2. 没有 service-level controller 串联 startup route/recovery、callback-quiescence、
   Connect generation、derived-record sink、checkpoint restore/publication、READY
   monitor、exact stop/abort 和 restart。
3. checked-in `mdl_sdk_2_13_234/libs/linux/libmdl_api.so` 当时是 134-byte Git LFS
pointer，不是真实 vendor shared object；因此本记录时的仓库状态不能提供真实 SDK
   runtime/link 验收。
4. 没有对真实 endpoint 连续执行五次断线/重连并核对 Raw、ControlRecord、epoch、
   READY 与 service lifecycle。单元 fixture 的五轮状态机测试不是该制品。
5. 没有生产 required-subscription failure 注入、真实 malformed control 隔离、正常
   service restart/checkpoint suffix replay 或四流完整交易日证据。
6. Phase 2 的 production cutover、10,000 seeds、目标 NVMe、cold-cache 5×、reboot、
   deterministic power-loss 和目标环境 power-cut 条件仍未完成；Phase 3 不能绕过
   其上游 durability authority。

本记录据此不声明 Production implementation complete 或 Production/external Phase 3
exit complete，当时也不授权切换 production alias。后来的独立 alias 变更不追溯
改变这些 exit 判定。
