# Phase 2 local acceptance record（进行中）

Date: 2026-07-19  
Host scope: local Linux x86-64 development container

## 结论

本记录严格采用
[`docs/design.md`](../design.md#分层完成口径与退出条件)
中的三层口径。当前源码包含较完整的 Phase 2 Raw 库、POSIX 持久化组件和定向
测试；三种本机构建/CTest 已通过，但尚未把 Raw runtime 接入四个生产 ingress，
也没有覆盖 Local verification 合同规定的全部 crash/reconciliation 条件和外部
验收。因此：

| 口径 | 当前状态 | 判定依据 |
| --- | --- | --- |
| **Implementation complete** | **未完成** | `L2Flow::production`、ingress service 和四个 ingress 仍走 Phase 0–1 shadow 路径；Raw coordinator IPC/controller、Raw service monitor 和 RunManifest service publication 尚未接入 |
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

这些能力目前只是 Phase 2 library/runtime builder；它们尚未被生产 ingress
service 构造和 monitor 调用。

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
feeder 收盘后停止推送 market record 而按预期失败。因此当前证据只证明真实
control plane 和 Raw plumbing 接入，**不声明真实行情 data plane 已通过**。
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

以下构建都启用 CMake 中的 strict warnings（含 `-Werror`），并在当前最终代码
状态完成 full CTest：

| Build / suite | 最终结果 |
| --- | --- |
| Debug strict build + full CTest | **通过：98/98，0 failed；91.28 s** |
| Release strict build + full CTest | **通过：98/98，0 failed；28.48 s** |
| ASan+UBSan Debug build + full CTest | **通过：97/97，0 failed；133.98 s** |

ASan+UBSan 运行使用
`ASAN_OPTIONS=detect_leaks=0:halt_on_error=1:abort_on_error=1` 和
`UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1`。因此该结果只声明
AddressSanitizer/UndefinedBehaviorSanitizer；**不声明 LeakSanitizer**。sanitizer
build 的测试总数少一个，是因为 `mdl_vendor_minimal_link` 只在
`L2FLOW_SANITIZER_MODE=none` 时生成。三个 build 的
`L2FLOW_BUILD_RAW_V1_LIBFUZZER` 和 `L2FLOW_ENABLE_TSAN` 均为 `OFF`，所以也不
声明 libFuzzer campaign 或 ThreadSanitizer 结果。

上述绿色全量结果仍需满足下一节的完整 crash/reconciliation 条件，才能把
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
