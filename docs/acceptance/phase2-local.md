# Phase 2 local acceptance record（进行中）

Date: 2026-07-22
Host scope: local Linux x86-64 development container

## 结论

本记录严格采用
[`docs/design.md`](../design.md#分层完成口径与退出条件)
中的三层口径。当前源码包含较完整的 Phase 2 Raw 库、POSIX 持久化组件和定向
测试；当前另有一个显式运行的 production-composition live runner，把真实 SDK、
Raw runtime、Phase 3 controller、checkpoint 与 recovered restart 串在同一条验收
链路中。它没有替换四个 production ingress service，也没有覆盖 Local
verification 合同规定的全部 crash/reconciliation 条件和外部验收。因此：

| 口径 | 当前状态 | 判定依据 |
| --- | --- | --- |
| **Implementation complete** | **未完成** | production-composition runner 已存在并完成真实两代 SDK 验收；但 `L2Flow::production`、ingress service 和四个 ingress 仍走 Phase 0–1 shadow 路径，coordinator IPC、Raw service monitor 和 RunManifest service publication 尚未接入 |
| **Local verification complete** | **未完成** | Debug/Release/ASan+UBSan 全量 CTest 与默认 deterministic corpus/property driver 已通过；但设计规定的全部 crash window、固定 `SIGKILL` 矩阵、精确 clean-stop/crash-range 对账和尚未完成的 power-loss oracle 仍未覆盖 |
| **Phase 2 exit complete** | **未完成** | Phase 1 外部退出条件、10,000 seeds、目标 NVMe、真实四流完整交易日、cold-cache 5×、真实 reboot/power-cut 等证据均不存在 |

本文中的“存在”或“已实现”只描述可从当前源码和测试入口审计的局部能力，不表示
上述任一层已经完成。

## 当前可审计的实现与定向测试

### Raw V1、WAL、reader、recovery 与持久 sidecar

[`CMakeLists.txt`](../../CMakeLists.txt) 已把下列组件编入独立的
`l2flow_phase2` / `L2Flow::phase2` 库：

- Raw V1 schema identity、显式 codec、CRC-32C、writer、reader、segment
  accumulator、POSIX WAL stream 和 writer lease；
- durable journal/marker、namespace、recovery、recovery executor 和 typed
  maintenance report；
- control page/file、sparse index、Raw manifest、sealed certificate、live tail；
- clean-stop gate、readiness observer/worker、Raw capture worker、fault
  transformer、InjectedRawV1；
- reserve state/header、coordinator gate、emergency transition、finalization
  report/archive、RunManifestV1 codec 和 POSIX store。

对应的定向测试入口包括：

```text
test_phase2_raw_schema
test_phase2_raw_v1
test_phase2_raw_wal_writer
test_phase2_raw_wal_stream
test_phase2_raw_wal_stream_posix
test_phase2_raw_reader
test_phase2_raw_recovery
test_phase2_raw_recovery_executor
test_phase2_raw_recovery_posix
test_phase2_raw_index_v1
test_phase2_raw_control_file
test_phase2_raw_manifest_v1
test_phase2_raw_manifest_store
test_phase2_raw_manifest_transition
test_phase2_raw_live_tail
test_phase2_raw_live_tail_posix
test_phase2_raw_clean_stop_gate
test_phase2_run_manifest_v1
test_phase2_run_manifest_posix_store
```

Raw validating reader 要求合法 Raw V1 header/schema/framing，而新增 replay CLI
还要求规范的 `segment-NNNNNNNN.raw` 名称和调用者显式提供 durable logical
end；这些边界不会把 Phase 1 native-endian shadow 文件提升为 Raw V1 或 durable
证据。

### Raw replay library 与 CLI

[`src/ingress/raw_replay.cpp`](../../src/ingress/raw_replay.cpp) 提供只基于 Raw
元数据的 replay engine，覆盖：

- as-fast-as-possible、原始 monotonic 间隔和固定倍率三种 pace；
- stream/date、`ServiceID`/`ServiceVersion`/`MessageID`、WAL、ingress sequence
  和 receive realtime 范围过滤；
- clock-epoch boundary、monotonic regression、pause/resume 和 single-step；
- durable 与显式 `RECOVERED_APPEND_ONLY` provenance 分离；
- 由 owned backing storage 保持生命周期的 `RawRecordView`。

[`tools/l2flow_raw_replay.cpp`](../../tools/l2flow_raw_replay.cpp) 已作为
`l2flow-raw-replay` 构建目标接入。该 CLI：

- 从 retained root 逐级使用 no-follow descriptor 打开规范 segment；
- 要求每个输入显式给出 exclusive durable logical end，不从文件长度或 pathname
  推断 durability；
- 在产生 stdout 前验证整个输入 prefix、record boundary、namespace 和多 segment
  连续性；
- 输出稳定的 input digest、durable frontier、record locator、metadata 和
  provenance 文本；
- 拒绝 continuation segment，因为该 CLI 没有绑定 DONE reserve/report 证据；
- 明确不发布 RunManifestV1，stdout 只是诊断输出。

定向入口为 `test_phase2_raw_replay` 和
`test_phase2_raw_replay_cli`。这些测试入口不等同于退出条件要求的 replay/live
在真实 durable 范围上的完整 byte/order 对账。

### Emergency reserve capacity probe

[`src/ingress/raw_emergency_reserve_capacity_probe_posix.cpp`](../../src/ingress/raw_emergency_reserve_capacity_probe_posix.cpp)
实现了 descriptor-bound POSIX capacity probe。可审计边界包括：

- 绑定 retained Raw root 的 device/inode、owner-only mode、filesystem identity
  和配置的 mount identity；
- 用固定 domain 编码并校验有序 user/group/project quota vector identity；
- 通过 `fstatvfs`/`fstatfs` 读取 filesystem byte/inode availability；
- Linux XFS 路径用 fd-based quota status/usage 查询校验 accounting 与
  enforcement；
- 检查 reserve data/inode inventory 的 inode、mode、allocation 和 project
  identity，并用 checked arithmetic 形成 byte/inode observation；
- 任一身份、quota evidence、charge evidence 或算术条件不成立时 fail closed。

Linux generic ext4 quota API 不能直接证明 enforcement-on；默认 ext4 backend
因此明确 fail closed，不能把可查询 accounting 误写成已证明的 EDQUOT
enforcement。`test_phase2_raw_emergency_reserve_capacity_probe_posix` 使用可注入
syscall observation 覆盖 identity、quota、容量与拒绝路径；目标文件系统上的
privileged quota/EDQUOT drill 尚未执行。

### ACK、finalization continuation 与 typed COMPLETE

[`src/ingress/raw_reserve_coordinator.cpp`](../../src/ingress/raw_reserve_coordinator.cpp)
和
[`src/ingress/raw_finalization_continuation_posix.cpp`](../../src/ingress/raw_finalization_continuation_posix.cpp)
包含以下可审计能力：

- ACKED emergency grant 的 continuation cap、counter mask、original writer
  identity 和 replacement executor identity 分离；
- live continuation 从 ring 排尽后 seal，receipt 持有 writer lease；
- replacement seal-only 路径从唯一 durable candidate 恢复、截断到完整 durable
  prefix、seal，并显式报告 recovered/frozen gap；
- 已 seal orphan 的幂等 adoption，以及 candidate 消失、final+tmp 冲突和更高
  segment 候选的 fail-closed 拒绝；
- coordinator 的 typed `CompleteFinalizationContinuation` 在消费 receipt 后，
  重新校验 state generation、action、token、artifact inode/hash、cursor、
  allocation 和四维 capacity observation，再发布 `COMPLETE`；
- generic completion 不能代替 continuation-specific completion；
- receipt/body/inode/route tamper 在 capacity probe 或 `COMPLETE` 前被拒绝。

`FENCED_NO_ACK` 按设计没有 continuation capacity；replacement continuation
入口只接受 ACKED grant。上述路径的定向覆盖位于
`test_phase2_raw_ingress_app` 及其
[`raw_finalization_continuation_e2e.inc`](../../tests/raw_finalization_continuation_e2e.inc)。

### Finalization archive cleanup 与 offline reprovision

[`src/ingress/raw_emergency_reserve_posix.cpp`](../../src/ingress/raw_emergency_reserve_posix.cpp)
和
[`src/ingress/finalization_archive_source_cleanup_posix.cpp`](../../src/ingress/finalization_archive_source_cleanup_posix.cpp)
实现并测试：

- 从 durable finalization archive 恢复 capability；
- 对 live source report 做 inode-bound cleanup/absent adoption；
- 在保持 pool coordinator lease identity 不变的前提下，使用新 reserve UUID
  构造 inventory/data/state candidates；
- 发现并恢复唯一 durable reprovision candidate，拒绝零 UUID、复用 UUID、
  identity 替换、hard-link 和多 candidate 等冲突；
- 固定名字原子替换后，旧 retained fd 仍指向旧 inode，新名字选择新 inode。

`test_phase2_raw_emergency_reserve_reprovision_posix` 含一个真实
`fork` + `SIGKILL` 的两进程恢复场景：builder 在 durable candidate barriers
之后被终止，后继进程从 archive 和 namespace state 恢复并完成替换。它只证明
该特定 process-crash window；`SIGKILL` 不会模拟 kernel page-cache 丢失，因而
不是 power-loss 证据。

### Raw runtime preflight、metrics 与 sync 配置

[`src/ingress/raw_production_runtime.cpp`](../../src/ingress/raw_production_runtime.cpp)
已有 fresh、recovered-closed 和 already-active runtime builder。recovered 与
active 路径会在 SDK connect 前核对 prepared sink 的 exact open snapshot、
append/durable cursor 和 live-tail attach 条件；缺少 typed recovery report 或
身份不一致时 fail closed。定向入口为
`test_phase2_raw_production_runtime` 和
`test_phase2_raw_phase2_startup_plan`。

`RawProductionRuntimeV1::SampleReadiness()` 由 runtime 自己通过 retained POSIX
live-tail source 读取每次新的 coherent control snapshot，再用同一 snapshot
计算 observational readiness；control 读取失败不会返回缓存的 READY。对应的
production-runtime 组件测试核对 control generation、writer/stream identity
和 connect generation。

[`src/ingress/raw_ingress_app.cpp`](../../src/ingress/raw_ingress_app.cpp) 已把配置的
sync interval/bytes 映射到 Raw capture worker，并暴露 Raw runtime identity、
append/durable cursor、durability lag、ring 和 observer lag 指标。
`test_phase2_raw_ingress_app` 有 bytes threshold 与 interval 行为测试。

这些能力尚未被四个 production ingress service 构造和 monitor 调用；2026-07-22
addendum 记录的独立 live runner 已组合这些 builder，但不是 service cutover。

### 真实 feeder 到 Raw capture path 探针

[`tools/mdl_phase2_live_probe.cpp`](../../tools/mdl_phase2_live_probe.cpp) 已作为
`mdl_phase2_live_probe` CMake target 接入，输出
`mdl-phase2-live-probe`。它是显式运行、默认不联网的外部测试入口，不是依赖
实时网络的默认 CTest。每次调用只选择一个 production `IngressKind`，并把真实
SDK callback 接入：

```text
CallbackHandler
  -> ByteRing
  -> RawCaptureWorker
  -> POSIX RawWalWriter
  -> sealed segment/journal
  -> Raw validating reader
  -> Raw recovery analyzer
```

输出目录必须是尚不存在的绝对路径；探针拒绝覆盖旧目录，对 callback ring 和
Raw bytes 设置边界，使用固定的非秘密本地 feeder label，并且没有 token 命令行
参数。Data-plane 成功要求每个 required subscription 都返回 `MDLEC_OK`，且每个
required key 都至少收到配置数量的 market record。特别是 `sz-tick` 必须分别
收到 `6.101.33` 和 `6.101.36`，不能只以两者总数判断成功。停机后还必须满足
callback/append/durable record 与 vendor bytes 精确对账、ring 为空、writer
clean seal、Raw reader 全量扫描通过，以及 recovery analyzer 接受同一个 sealed
durable frontier。

2026-07-21 收盘后，在本地 cascade feeder `127.0.0.1:9112` 上对四个 ingress
kind 分别执行了三秒 control-plane-only 测试，显式使用
`--minimum-market-messages-per-key 0`。五个 required key 的订阅回执全部为
`MDLEC_OK`：

```text
ingress kind   required key(s)       vendor / framed / segment bytes
sh-snapshot    4.101.4               102 / 216 / 4312
sh-tick        4.101.24              102 / 216 / 4312
sz-snapshot    6.101.28              102 / 216 / 4312
sz-tick        6.101.33, 6.101.36    110 / 224 / 4320

每次 callback / append / durable / reader records: 1 / 1 / 1 / 1
每次 journal / recovery accepted journal bytes: 4240 / 4240
每次 segment sealed: true
每次 append / durable reconciliation exact: true / true
```

每次唯一记录都是真实 SDK control response。另一次 `sh-snapshot` 使用
`--minimum-market-messages-per-key 1` 的测试已经连接并完成订阅登录，但由于
feeder 收盘后停止推送 market record 而按预期失败。因此该批 2026-07-21 证据只
证明真实 control plane 和 Raw plumbing 接入；2026-07-22 addendum 另记录交易时段
真实行情 data plane 和两代 production composition 的通过证据。
必须在交易时段用非零 per-key minimum 重跑；四个 ingress kind 仍然是四份独立
验收证据。

该探针有意不 provision production reserve coordinator、不发布 production
manifest/certificate、不测试 rotation，也不替换 Phase 0–1 production service。
它的成功不能关闭 production cutover、完整交易日、crash、power-loss 或 Phase 2
exit 条件。

### Incremental SHA-256 修复

[`src/common/sha256.cpp`](../../src/common/sha256.cpp) 修复了 incremental
`Sha256Hasher::Update` 的 partial-buffer 路径：已有未满 block 且本次输入仍不足以
填满时，函数保留该 partial block 并返回，不再落入后续逻辑把
`buffer_size_` 错误重置为零。实现同时拒绝超过 SHA-256 64-bit bit-length
上限的输入，并保持 finalize one-shot 语义。

`test_sha256` 用标准向量、所有小 chunk size 的 incremental/one-shot 等价和
finalize 后拒绝复用覆盖该修复。

### Deterministic Raw V1 input-safety harness

默认 CTest 已登记
[`test_phase2_raw_v1_fuzz_corpus`](../../tests/test_phase2_raw_v1_fuzz_corpus.cpp)，
它复用
[`tests/fuzz/raw_v1_fuzz_harness.cpp`](../../tests/fuzz/raw_v1_fuzz_harness.cpp)
执行 8 个最小 corpus 文件和 616 个固定生成用例。覆盖范围包括 Raw V1 codec
canonical round-trip、validating reader 边界、recovery 输入边界和相同输入的
确定性结果。

[`CMakeLists.txt`](../../CMakeLists.txt) 另提供默认关闭、Clang-only 的
`L2FLOW_BUILD_RAW_V1_LIBFUZZER` target；本次三个 GCC build 均保持该选项为
`OFF`，因此没有执行持续 libFuzzer engine、coverage-guided campaign 或形成
时间/coverage 报告。上述 deterministic driver 也不构成 crash/power-loss
证据。

## 全量本机验证结果

以下是 2026-07-19 记录时的历史 full-green 结果；当前最终代码的最新矩阵在本文
末尾 2026-07-22 addendum 中列出。历史结果保留用于说明当时状态，不覆盖当前
Phase 0 artifact gate 的 fail-closed 结果：

| Build / suite | 最终结果 |
| --- | --- |
| Debug strict build + full CTest（2026-07-19） | **通过：98/98，0 failed；91.28 s** |
| Release strict build + full CTest（2026-07-19） | **通过：98/98，0 failed；28.48 s** |
| ASan+UBSan Debug full CTest（2026-07-19） | **通过：97/97，0 failed；133.98 s** |

该历史 ASan+UBSan 运行使用
`ASAN_OPTIONS=detect_leaks=0:halt_on_error=1:abort_on_error=1` 和
`UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1`。因此该结果只声明
AddressSanitizer/UndefinedBehaviorSanitizer；**不声明 LeakSanitizer**。sanitizer
build 的测试总数少一个，是因为 `mdl_vendor_minimal_link` 只在
`L2FLOW_SANITIZER_MODE=none` 时生成。三个 build 的
`L2FLOW_BUILD_RAW_V1_LIBFUZZER` 和 `L2FLOW_ENABLE_TSAN` 均为 `OFF`，所以也不
声明 libFuzzer campaign 或 ThreadSanitizer 结果。

上述历史绿色结果仍需满足下一节的完整 crash/reconciliation 条件，才能把
Local verification complete 改为完成。

## Implementation complete 的明确阻断项

1. [`CMakeLists.txt`](../../CMakeLists.txt) 仍把
   `L2Flow::production` alias 指向 `l2flow_phase01`；不能把独立
   `L2Flow::phase2` 库的存在等同于生产 cutover。
2. `l2flow_ingress_service` 仍链接 `L2Flow::phase01`，四个
   `mdl-*-ingress` 仍由 Phase 1 `IngressApp` 和 `ShadowCaptureWriter`
   构造、要求 shadow path。四流尚未用 Raw writer 替换 shadow 落盘，也未证明
   Raw 后的 Phase 1 observational readiness 已在真实 service 中保留。
3. 配置模型虽然有 `reserve_coordinator_socket`，仓库中没有对应的 coordinator
   daemon、AF_UNIX 请求/响应协议、client/server 或跨四个独立 ingress 的
   capability 交付。现有 coordinator 是进程内对象并终身持有 pool lease 的
   exclusive flock；四个 ingress 不能各自 attach。设计又要求每个 action
   holder 独立 `openat` coordinator lease，明确禁止用 `dup`、fork inheritance
   或 `SCM_RIGHTS` 共享 OFD。现有 action/ACK/receipt 还持有 process-local
   `shared_ptr`、OFD gate、retained fd 或 app/ring/writer 引用，不能直接序列化。
4. 没有 service-level startup controller 组合 zero-mutation route discovery、
   registration/takeover、recovery、header/backend/clock identity 和三条 runtime
   factory。startup planner 有意保持 mutation-free 且从不授权 Connect；factory
   也有意要求 caller 提供已经建立的 typed activation inputs。
5. 没有生产 controller 驱动 emergency stop、ACK/grant、continuation、
   finalization、archive cleanup 和 reprovision 的完整生命周期。
6. Phase 1 service monitor 尚未替换/扩展为 Raw disk-health、durability lag、
   reserve 和 fatal-state monitor；已有 `SampleReadiness()` 和
   `RawIngressApp::prometheus_metrics()` 尚未接入生产 monitor/metrics worker，
   write/sync/rotation/recovery/disk/quota/coordinator 等完整设计指标也尚未全部
   采集。
7. Raw clean-stop reconciliation、Raw-specific fatal state 和
   RunManifestV1 codec/store 均已有库/测试，但尚未接入四个 service 的 stop、
   fatal 和 run publication 路径。
8. 仓库已有 machine-readable
   [`schemas/raw_v1.json`](../../schemas/raw_v1.json)、固定 schema SHA-256 和
   complete-byte golden tests；但 Raw codec 是手写源码，未发现 codec generator、
   生成产物或 schema→generated-codec 一致性构建 gate。因此设计要求的
   “schema hash + golden bytes + generated codec 一起冻结”仍未满足。

以上任一项存在时都不能声明 **Implementation complete**，也不能把
`L2Flow::production` 切换到 Phase 2。

## Local verification complete 的明确阻断项

- 已有若干 partial-write、sync failure、torn input、recovery 和局部
  `SIGKILL` 测试，但没有覆盖设计在“Crash 与 power-loss 测试”中逐项列出的全部
  filesystem barrier/crash window，也没有完整固定小矩阵的独立结果制品。
- 没有枚举“最后成功 file sync / parent-directory sync 之后未覆盖 mutation”
  的所有允许 surviving-byte/namespace 结果；普通临时文件系统测试与
  `SIGKILL` 均不能替代 deterministic power-loss model。
- 没有完整证据证明所有 clean stop 都满足 exact record/vendor-byte/WAL-cursor
  reconciliation，也没有覆盖所有 crash case 的
  `DURABLE`、`RECOVERED_APPEND_ONLY`、`TRUNCATED_PARTIAL_TAIL`、
  `TRUNCATED_INVALID_TAIL` 或 whole-run fatal range grammar。

因此本文的存在本身不满足 **Local verification complete**。

## Phase 2 exit 的未执行外部条件

以下条件均无可引用的验收制品，当前状态全部为**未完成**：

- Phase 1 外部退出条件：真实四端点 login、required-subscription OK、首条真实
  required record、八小时 shadow、目标机 callback latency、真实 restart 和
  目标部署兼容性；
- 10,000 次固定 seed、可重放的随机 crash injection；
- 目标 NVMe 上 1/2/5/10/20 ms 与 bytes trigger 组合的 throughput、
  p99/p99.9/max sync latency 和 durability-lag 报告；
- 真实四流完整交易日 Raw clean stop 的 record、vendor bytes 和 WAL cursor
  精确对账，以及 recovery、rotation、reserve 和 disk-watermark 演练；
- 按设计固定 CPU/NUMA、I/O scheduler、并发 barrier 和完整 scan 工作量的
  cold-cache 四流 Raw reader 5× 报告；
- 在同一真实 durable 范围上 replay 与 live tail 的 `RawRecordView`
  schema/ownership、record bytes 和顺序完全一致的验收制品；
- 正常 service restart、正常 OS reboot、deterministic storage model、
  dm-flakey/fault block device、VM hard power-off 和目标
  NVMe/filesystem power-cut 的分层报告及 device cache/flush 配置。

这些条件不得用 mock、单元测试、容器临时文件系统、进程 `SIGKILL` 或一次本机
构建替代。只有三层口径各自的全部条件都有独立制品证据后，才可更新本记录中的
状态。

## 2026-07-21 addendum：本地代码收尾

本 addendum 记录 2026-07-21 的增量修复与当前回归状态；它不修改本文开头对
Phase 2 三层口径的判定，前述历史测试表也仍是其记录日期当时的结果。

### recovered cursor + zero new callback clean stop

`RawIngressApp` 的 callback/capture worker 计数是本进程 Connect generation 的
增量值，而 recovered runtime 的 append/durable/ingress cursor 是 namespace 的
绝对值。此前，当 recovered cursor 非零且本 generation 没有新 callback 时，
默认的 `captured_ingress_sequence==0` 会与非零 terminal WAL cursor 冲突，导致
本应可精确对账的空 generation 无法形成 clean-stop evidence。

当前修复在 callback 已静默后，从以
`recovered_next_ingress_sequence` 初始化的 handler 取得本 generation 的绝对
terminal ingress cursor，并写入零 callback 的 terminal evidence；
`RawIngressCleanStopEvidenceV1::exact()` 对该分支明确要求：

- callback records 和 capture-worker append/durable record counts 保持增量零；
- callback terminal ingress sequence 等于 started recovered append sequence；
- final append/durable last ingress sequence 分别保持 started recovered 的绝对
  append/durable sequence；
- 其余 writer seal、cursor、byte、queue、callback-quiescence 与 identity 条件不被
  放宽。

`test_phase2_raw_ingress_app` 新增 recovered nonzero cursor + zero new callback
场景，验证 clean stop 成功、worker 增量计数仍为零、absolute cursor 不被重置或
虚增。

### 当前验证与制品阻断

`RawLiveTail` 同时完成了 Phase 3 live bridge 所需的 Phase 2 边界收紧：fresh
control sample 每次直接读取 source，不复用 READY 缓存；tail 与 POSIX source
分别串行化其可变 source 操作；attach 与 fresh sample 都拒绝 zero/odd seqlock
generation、writer/namespace 漂移、append/durable 不同 segment base、未对齐或
不满足最小 Raw record 算术的 cursor。测试另覆盖奇数 generation fail-closed，
以及合法偶数 generation 的 attach/fresh sample。

本轮改动的定向 Phase 2 回归：

```text
test_phase2_raw_ingress_app
test_phase2_raw_live_tail
test_phase2_raw_live_tail_posix
test_phase2_raw_readiness_worker
```

strict Debug、Release 和 ASan+UBSan Debug 的上述四项定向回归均为
**4/4 tests passed，0 failed**。ASan+UBSan 使用
`ASAN_OPTIONS=detect_leaks=0:halt_on_error=1:abort_on_error=1`，因此不声明
LeakSanitizer。

当前 strict Debug build 在排除下述唯一不可执行的 Phase 0 vendor artifact gate
后，full CTest 为 **92/92 tests passed，0 failed**；Phase 3 label 是其中的
**5/5 passed**。`test_phase0_baseline` 单独执行仍按预期 fail-closed，而不是被
跳过后宣称整个 93-test suite 全绿。

Full CTest 中的 Phase 0 vendor baseline 仍受外部制品阻断：
`mdl_sdk_2_13_234/libs/linux/libmdl_api.so` 当前只有 134 bytes，内容是 Git LFS
pointer（其声明的真实 object size 为 242357680 bytes），不是可加载的 ELF shared
object。该缺失不能通过放宽 ABI/baseline gate 或把 pointer 当成 SDK library 来
规避；必须取得与 baseline 匹配的真实 vendor artifact 后重跑。

上述本地修复与定向绿色结果仍不完成以下事项：

- `L2Flow::production` 和 `l2flow_ingress_service` 尚未切换到 Phase 2，生产
  service/controller、coordinator IPC、monitor、RunManifest 和四 ingress lifecycle
  仍未接入；
- 真实四端点/交易时段 data plane、10,000 seeds、目标 NVMe、完整交易日、
  cold-cache 5×、正常 reboot、deterministic power-loss 与目标环境 power-cut 制品
  仍不存在；
- 因此 Phase 2 的 **Implementation complete**、**Local verification complete**
  和 **Phase 2 exit complete** 仍全部为**未完成**。

## 2026-07-22 addendum：真实 feeder、production composition 与两代恢复

本 addendum 更新前述“只有 control-plane plumbing、没有真实行情 data plane”和
“没有 production controller composition”的历史状态。它仍不改变本文的 service
cutover、完整交易日、crash/power-loss 和 Phase 2 exit 判定。

### 真实库与测试边界

测试连接用户已运行且未被测试进程修改的 cascade feeder
`127.0.0.1:9112`，动态加载外部真实库
`/home/sunc/L2Flow/MDL/libmdl_api.so`。该库的本次只读识别结果为：

```text
ELF64 x86-64
size:      245709040 bytes
Build ID:  f9cd4310b672e83c9f3436972eb73c42680d3117
SHA-256:   85b69d495e4a9d2e212342c887138599bd913b7d7426f4241d2d12f7c012116a
```

live runner 只使用固定的非秘密本地 client label，命令行不接受 credential；本文也
不记录 feeder credential。仓库内
`mdl_sdk_2_13_234/libs/linux/libmdl_api.so` 仍是 134-byte Git LFS pointer，外部
真实库不会自动修正 frozen Phase 0 baseline，也没有据此修改 baseline。

### Phase 2 shutdown callback 窗口

external authoritative consumer 的正常停机顺序现在固定为：

```text
SDK Shutdown（callback handler 仍 accepting）
-> handler.BeginStopping()
-> handler.Quiesce()
-> Raw StopAndDrain / seal
-> Phase 3 StopAt exact terminal cursor
```

这样 SDK `Shutdown()` 内同步或尾部触发的 callback 仍可进入 Raw；handler 只在 SDK
完成 shutdown 后停止接收。`test_phase2_raw_ingress_app` 增加了 Shutdown 内注入
callback 的场景，并要求最终 `callbacks_after_stop=0`。真实 `sz-tick` Phase 2
重测目录为：

```text
/tmp/l2flow-live-20260722-sz-tick-shutdown-fixed-1021
```

结果为 callback/append/durable/reader records 全部 `125880`，append/durable WAL
均为 `27671344`，`callbacks_after_stop=0`，recovery sealed 且 reconciliation
exact。修复前同类测试曾观测到 `callbacks_after_stop=6`；该失败事实不被通过结果
覆盖。

### append-visible Raw tail 与 rotation

此前 `RawLiveTail::Next()` 在“当前 control 仍指向已 seal 的旧段、下一段 control
尚未发布”的短窗口把整个 stream 永久标记为 terminal，Phase 3 因而停在旧段尾。
Raw control 没有 whole-stream closed bit，所以该状态不能证明全流结束。当前行为是
返回 `WouldBlock` 且不锁存 terminal；下一 control 发布后允许
`SegmentTransition`。真正终点只由 SDK shutdown、Raw drain 和 controller 的 exact
`StopAt` 证明。

定向测试覆盖“sealed current -> WouldBlock -> publish next -> transition -> next
record”。真实 1-second segment-age rotation 重测目录为：

```text
/tmp/l2flow-phase3-live-20260722-sh-snapshot-rotation-fixed-1100
```

该 run 产生 7 个 sealed Raw segment，Raw scan 与 Phase 3 都为 `2591` records，
append/durable/decoder WAL 均为 `3402464`，restart 扫描全部 7 段并找到 checkpoint
边界，最终 `passed=true`。修复前证据
`/tmp/l2flow-phase3-live-20260722-sh-snapshot-rotation-1052` 停在第 1 段的
358 records，最终 `worker_failure=11 (kStopCatchUpTimedOut)`；7 个 Raw 段本身均
已正确封存。

### production Phase 2 -> Phase 3 live runner 与第二代 recovery

[`tools/mdl_phase3_live_probe.cpp`](../../tools/mdl_phase3_live_probe.cpp) 构建为
`mdl-phase3-live-probe`。它不是内存 adapter，使用下列生产组件：

```text
Raw reserve coordinator / registered route
-> RawProductionRuntimeV1
-> immutable authoritative replay
-> append-visible POSIX Raw tail
-> ControlProductionControllerV1 + durable derived sink
-> SDK Connect / READY / exact StopAt
-> sealed Raw + terminal checkpoint
-> register RECOVERING + RESUME_CONNECT
-> analyze/execute sealed Raw recovery
-> create exact next open segment
-> publish RESUMED_OPEN maintenance report
-> receipt-gated ACTIVE promotion
-> checkpoint restore + suffix replay
-> pre-Connect Phase 3 worker
-> second real SDK Connect / current-generation READY
-> second exact stop and checkpoint
```

第二代不另起伪造 Raw namespace：`stream_day_id`、journal 和全局 WAL 连续，writer
instance 与 recovery-attempt identity 更新；closed manifest/certificate、terminal
seal marker、journal cursor、下一 segment base WAL/first ingress、open manifest 和
control page 必须交叉一致，才允许 `RECOVERING -> ACTIVE`。

2026-07-22 的两条完整两代真实 data-plane 通过证据为：

| 流 | 目录 | generation 1 Raw | 最终 Raw = Phase 3 | 最终 WAL | READY / checkpoints |
| --- | --- | ---: | ---: | ---: | --- |
| `sh-snapshot` | `/tmp/l2flow-phase3-live-20260722-sh-snapshot-two-generation-2` | 1713 | 3494 | 4539552 | 两代 READY；2 derived；2 checkpoints |
| `sh-tick` | `/tmp/l2flow-phase3-live-20260722-sh-tick-two-generation-3sec` | 24503 | 44466 | 9612848 | 两代 READY；2 derived；2 checkpoints |

两次均满足：

- generation 1 checkpoint publication 和只读 Raw boundary restore；
- clean sealed recovery plan，journal logical size 不被修复或缩短；
- segment 1 到 segment 2 的 base-WAL/ingress 连续性；
- generation 2 controller 在第二次 SDK `Connect()` 前完成 checkpoint restore；
- 新 generation 的 `LogonSuccess` 使累计计数从 1 增为 2，旧 checkpoint 登录证据
  不能直接满足 current-generation READY；
- generation 2 有真实新 Raw/derived suffix；
- 最终 Raw record count、Phase 3 processed count 与 decoder terminal cursor 精确
  相等，append WAL、durable WAL 和 decoder record-end WAL 精确相等；
- `worker_failure=0`、`worker_process_error=0`、`worker_live_tail_error=0`。

### 保留的非通过证据

以下目录保留且不解释为通过：

- `/tmp/l2flow-phase3-live-20260722-sz-tick-1033`：修复前长 backlog/rotation
  run；sealed Raw 离线扫描共 163926 records，Phase 3 当时停在 64320，checkpoint
  正确未发布；
- `/tmp/l2flow-phase3-live-20260722-sz-tick-two-generation-1`：generation 1 通过，
  generation 2 完成真实 recovery、checkpoint restore、Connect、live suffix 和
  exact stop，但 1-second 窗口只见 required market mask `2`，READY reason 20
  (`kMarketEvidenceIncomplete`)，所以 `passed=false`；最终 Raw=Phase 3=21911，
  worker/process/live-tail errors 仍为 0；
- `/tmp/l2flow-phase3-live-20260722-sz-tick-two-generation-3sec`：该次行情窗口中
  generation 1 未收齐 required market evidence，随后 controller 在 normal StopAt
  前已不再是 Running，因而 fail closed 且没有 checkpoint。输出时尚未加入精确
  worker failure 枚举，不能在没有证据时进一步归因。

### 当前回归口径

当前代码完成：

```text
strict Debug full CTest:    106/107 passed
strict Release full CTest:  105/106 passed
ASan+UBSan selected:         11/11 passed (ASAN_OPTIONS=detect_leaks=0)
TSan selected:                2/2 passed
```

Debug/Release 唯一失败均为既有 `test_phase0_baseline`，原因是仓库 SDK archive/LFS
artifact 与 frozen baseline 不匹配，不是 Phase 2/3 断言失败。ASan/UBSan 不声明
LeakSanitizer；TSan 定向项为 `test_phase3_control_production_controller` 和
`test_phase2_raw_production_runtime`。

上述结果证明 production-composition runner 的 Phase 2/3 两代实盘链路，但不证明
四个 production service cutover、跨进程 coordinator IPC、完整交易日、10,000
crash seeds、目标 NVMe、cold-cache 5x、正常 OS reboot、deterministic power-loss
或目标环境 power-cut，因此三层总退出状态仍保持未完成。
