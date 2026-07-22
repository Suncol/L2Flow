# Phase 8 local acceptance record（本地代码范围）

Date: 2026-07-22

> 2026-07-22 superseded note：`L2Flow::production` 已由独立授权变更切换到
> `l2flow_production`；本 Python/Parquet package 仍未接入，并且不同于 C++
> `InstrumentHistoryRuntimeV1`。本记录的 formal exit 判定不变。

## 结论

本记录只验收仓库内 `l2flow_history` 的 Parquet、manifest/sidecar、bounded
hot/cold query、lineage、retention dry-run 与 recovery/rebuild plan 构造切片。
它不宣称正式 Phase 8 Exit；本阶段工作在记录当时也不改变 production alias。

| 口径 | 本记录时状态 | 判定依据 |
| --- | --- | --- |
| Real Parquet local slice | 通过（本地范围） | PyArrow 25.0.0 writer/footer/readback、ZSTD、完整行重读；无私有伪 Parquet fallback；DuckDB 1.5.4 可直接读取 |
| Manifest/sidecar local slice | 通过（本地范围） | canonical JSON envelope、immutable generation、CURRENT CAS、persistent watermark namespace；crash/tamper/path/fd 生命周期用例通过 |
| Query/lineage/retention local slice | 通过（本地范围） | fixed snapshots、stable duplicate semantics、exact cursor scope、lineage 三类预算、six-gate dry-run；索引与 brute force 随机对照一致 |
| Recovery/rebuild local slice | 通过（本地范围） | fail-closed plans/certificates 与定向用例通过；不执行 Raw repair 或 production route switch |
| Formal Phase 8 exit | 未完成 | 无完整日生产 lineage、24h 隔离压力、目标机 ingress-SLO 或删除执行竞态证据 |

## 验收边界

Raw 仍是唯一不可替代事实。Canonical/factor Parquet、manifest、watermark
sidecar、checkpoint 和查询索引全部是 derived artifact。只有 immutable manifest
generation 引用的 final 文件可进入 cold query；单独存在的 `.parquet` 文件不可见。
Canonical 永久可见性还要求 sealed/healthy、route-complete common-cut 证据。仓库内
wrapper 只绑定 caller 已验证的 certificate identity，并不解析该 certificate、验证
签名或自行观察 Phase 5 generation health；open/fatal generation 不能因本阶段写出
文件而被“补成”可消费代。

Manifest store 只接受 normalized absolute non-root path，逐级拒绝 symlink 并在对象
生命周期内 pin directory fd/device/inode；构造后的 pathname 替换不会重定向它。
Immutable no-replace publication 要求 Linux `renameat2(RENAME_NOREPLACE)`；能力缺失
时失败关闭，不采用存在 crash 双 hardlink 窗口的 link/unlink fallback。

真实 Parquet 写入依赖可选 PyArrow。逻辑层可在未安装 PyArrow 时导入，但任何实际
Parquet I/O 必须失败关闭，不生成同名替代格式。完整文件 SHA-256 由外部 manifest
保存；它不写回自身 footer。Factor V1 历史值明确冻结为 float64 bits + validity；
`PASSTHROUGH_PLACEHOLDER` 不允许 `value_valid=true`。五个首批名称只是在当前
Phase 7 producer/catalog 中均为占位；history storage 不硬编码一个普适名单。
Factor publication 要求 caller 提供 exact group/catalog 身份绑定并把 membership
hash 写入 manifest，但该 evidence identity 不是签名或独立 registry 认证；生产
publisher 仍必须从已认证 factor registry 构造它，不能接受任意自我声明。

PyArrow 会写标准 row-group column statistics；当前 V1 自定义 footer/manifest
没有实现设计 14.4 的完整 typed pruning statistics、set hash 和 quality aggregate。
本地完整性判断依赖完整行重读、排序端点和 logical-row hash，不把这些库级统计
表述成已验收的生产 pruning metadata。

Persistent watermark reference 是
`(run_id, watermark_table_generation, watermark_set_id)`，同时绑定 stable
`input_identity_sha256` 和完整 map hash。Observed Raw durable position 与 display-only
clock label 保存在 full map，但不进入 stable input identity。Lineage 和 retention 的
cursor/WAL 比较只在 exact cursor domain/scope 内执行：Raw WAL scope 是
`(origin_capture_date, source_stream_id, origin_stream_day_id)`；Canonical cursor
scope 另含 source writer/generation、Canonical generation、family 与 shard。
Publication 对 receipts 建一次索引；Factor projected route 必须先映射到唯一完整
`SourceNamespace`，再在其非重叠 `(begin,end]` ranges 内二分，重复 row/reference 复用
一次 sidecar full-map 与 coverage 验证。双零 cursor/WAL 只把该唯一 namespace 下的
receipts 当作 route identity（VISIBLE 时另绑定 caller 已验证的 common-cut identity），
query 返回 `zero_consumption=true` 且 `receipt=None`，不声称任何正向 range 已被消费；
单零仍非法。Canonical receipt 的
二维矩形候选仍可能线性，因此 publication 的 structural cap 只是拒绝边界，不是
最大输入下的延迟承诺。
固定 query catalog 预先验证并索引 artifact、source、sidecar/reference 与 watermark
map；Factor lineage 先做 entry/work 预检，再在 nested evidence 分配前一次性预扣其
确定性 accounting bytes。Factor route 先解析到唯一完整 namespace，随后对该 namespace
的非重叠 cursor range 二分。Canonical 的 ingress/WAL 矩形跨 namespace 可任意重叠，
因此最坏情况下仍会扫描候选；每次候选检查都消耗显式 lineage-work budget 并检查
deadline。本记录只声明这一硬截止，不把 Canonical 查找误述成纯 `O(log R)`。

Retention 只生成 deterministic hashed dry-run plan，不调用 `unlink`。六项设计条件由
七个 evidence check 表达（sidecar reachability 单列）；current manifest ownership
仍属于 active reference。Publication evidence 必须绑定 exact scope 与半开 cursor
range，approval/evidence 是 caller-authenticated input 而非本地签名验证。即使所有 gate
在一个 inventory snapshot 内通过，未来 destructive executor 仍需 deletion
lease/fence、立即重验和独立授权；本地 pass 不构成“可安全删除”的实时证明。
Planner 对 consumer/active/sidecar 完整 snapshot 各 canonicalize/hash 一次并建立
exact-scope index；共享证据处理为 `O(A+C+R+S)`，另有 inventory `O(A log A)` 排序、
每个 artifact 自身有界证据和固有七项 gate 输出。声明的 hard cap 是拒绝边界，不是
建议把百万对象作为单次生产 batch 的性能承诺。

Manifest successor 只追加并保留此前所有 artifact/sidecar reference；本地没有
manifest-root rotation/compaction 或删除 executor。因此部署必须把单个 store 作为
有界 publication epoch，并由外部协议完成新 root 发布、reader/writer fence、route
原子切换和旧 root 退役。在旧 root 仍被 current manifest/query/replay/audit 引用时，
本地 retention 会正确拒绝它，而不是假装能够回收。

Recovery API 只生成并验证步骤、barrier 和 isolated generation cutover certificate。
Ingress 不等待这些下游步骤；Canonical V1 保留 Raw route 原始 source/writer/clock
identity，但必须用 fresh Canonical generation、零进度 frontier、normalizer 与空 sink
从交易日日初重放，不能从旧 processed cursor 续接。Latest checkpoint 路径要求
Phase 6 exact config、durable/quiesced barrier，并补齐 Snapshot tail；若 checkpoint
状态包含独立 Tick-quality lineage，则还必须补齐该 family 的 tail。V1 不支持把旧
checkpoint rebase 到新 config。真正改 production route 仍需
单独审查授权。

## 定向测试与结果

验证环境：CPython 3.10.12、NumPy 2.2.6、PyArrow 25.0.0、DuckDB 1.5.4、
setuptools 83.0.0。editable package 在清除 `PYTHONPATH`、工作目录切换到 `/tmp`
后可导入 `l2flow_history` 及其 Parquet/manifest/query/retention/recovery 子模块。

仓库内 Phase 8 全量：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=python \
  /tmp/l2flow-phase8-venv/bin/python -m unittest discover \
  -s tests/python_phase8 -p 'test_*.py' -v
```

结果：`95/95` 通过。其中 publication `15/15`；测试包含真实 PyArrow ZSTD
round-trip、直接 `pyarrow.parquet` API（绕过仓库 reader adapter）读取、fault/tamper、
manifest/sidecar、hot/cold merge、lineage budgets、retention 与 recovery/cutover。

相邻 Phase 7 Python 回归：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=python \
  /tmp/l2flow-phase8-venv/bin/python -m unittest discover \
  -s tests/python -p 'test_*.py' -v
```

结果：`22/22` 通过。

全新 CMake 目录配置并构建 Phase 6/7 目标后：

```bash
cmake -S . -B /tmp/l2flow-phase8-final-build \
  -DPython3_EXECUTABLE=/tmp/l2flow-phase8-venv/bin/python \
  -DL2MOCK_BUILD_TESTS=OFF \
  -DL2FLOW_BUILD_TESTS=ON \
  -DL2FLOW_BUILD_PHASE8_PYTHON_TESTS=ON
cmake --build /tmp/l2flow-phase8-final-build -j2 --target \
  test_phase6_latest_state test_phase6_state_checkpoint test_phase6_c_abi \
  test_phase7_factor_watermark test_phase7_latest_factor \
  test_phase7_consumer_batch test_phase7_consumer_c_api test_phase7_c_abi
ctest --test-dir /tmp/l2flow-phase8-final-build \
  -L phase8 --output-on-failure
ctest --test-dir /tmp/l2flow-phase8-final-build \
  -L 'phase6|phase7' --output-on-failure
```

结果：Phase 8 CTest `1/1`，Phase 6/7 CTest `9/9`，均通过；Phase 8 的一个
CTest 项内部执行上述 `95` 个 Python tests。`py_compile` 与 `git diff --check`
也通过。

另以 DuckDB 1.5.4（不同于 PyArrow 的查询引擎）直接读取本地 writer 生成的两个
final Parquet 文件，结果为：

```json
{"canonical":[1,600000,100,192],"factor":[1,"book_imbalance",true,"PASSTHROUGH_PLACEHOLDER",0]}
```

字段依次验证 Canonical 的 row count、instrument、Raw WAL end、exact record bytes
长度，以及 Factor 的 row count、factor ID、全部 invalid、显式占位状态和零 value bits。
这项互操作检查不替代完整 typed pruning metadata 或生产读服务验收。

## 未完成的正式 Exit gate

仍需至少：

1. 在生产数据上随机抽取历史 factor 行，逐条解析 sidecar 并回溯全部 Canonical/Raw
   输入的完整日证据；
2. 目标硬件上持续 24 小时的 query/compaction 压力、独立 I/O/cgroup 与 ingress SLO
   对照；
3. 基于生产 consumer/replay/audit/backup registry 的 deletion dry-run 审批清单；
4. destructive retention executor 的 lease/fence、TOCTOU 重验、备份与恢复演练；
5. full cold start、schema upgrade 和 isolated rebuild 在同一 Raw 上的完整输出 hash
   对账；
6. Parquet/query service 与生产 generation route 的正式 wiring、shadow 和 cutover
   授权；
7. 设计 14.4 的 typed pruning statistics、set hash、quality aggregate 的持久化与
   reader 校验；
8. 有界 manifest publication epoch 的 root 切换、双 root drain/fence 与旧 root
   退役演练。

这些证据仍是 Phase 8 external exit 的未完成项；本记录当时因此保持
`L2Flow::production` 指向 `l2flow_phase01`。后来的独立 alias 变更不完成这些证据。
