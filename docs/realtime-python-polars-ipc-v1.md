# Realtime Python/Polars IPC V1

## 1. 已实现范围

本实现把 `mdl-production-router` 扩展为一个可选的 Linux 本机只读行情
服务，并保留原有 C++ Pipeline 的依赖方向：

```text
Vendor SDK
  -> RealtimePipelineV1
  -> mandatory Store / KLine / latest read model
  -> RealtimeAppliedRecordSinkV1
  -> V1 wire projection
  -> sealed memfd
       ^
       |  read-only fd via AF_UNIX SOCK_SEQPACKET + SCM_RIGHTS
       |
  native C reader
  -> Python client
  -> optional PyArrow / Polars micro-batches

immutable Store generation
  -> per-instrument oldest-first cursor
  -> sealed read-only history-page memfd
  -> Python history cursor
  -> optional PyArrow / Polars page batches
```

IPC 层只投影已经成功应用到 Store 的记录，不创建第二条接入、解码或
归一化路径。Pipeline 只依赖通用的
[`RealtimeAppliedRecordSinkV1`](../include/l2flow/market/realtime_history_v1.h)；
UDS、memfd、Python、Arrow 和 Polars 均位于应用组合边界之外。

V1 数据面已经提供：

- 上交所/深交所最新 snapshot 的单标的和批量查询；
- 每个标的的最新 mixed `latest_tick` 的单标的和批量查询；
- 每个 `instrument_id × window_id` 的最新 K 线查询；
- 按 `(market, SecurityIDSource, SecurityID)` 不透明字节组合键精确反查
  `instrument_id`；
- 打开时固定一个最新已发布 Store generation，并按 instrument 分页读取
  该 generation 的全部记录；
- 一个带全局单调序列的有界 mixed tick ring，以及独立 consumer cursor；
- session、registry identity、server state、coverage flag、K 线 generation 和
  heartbeat；
- Python 只读客户端，以及按需加载的 PyArrow/Polars 转换。

这里的 `latest_tick` 沿用现有语义：它混合上交所 tick、深交所逐笔委托和
深交所逐笔成交，内容可以是委托、成交、撤单或状态。它不是“最新成交”，
也不会为了 IPC 再拆成多套最新值。

这里的 snapshot wire 是面向实时因子的稳定核心字段投影，包括公共时序/
来源主字段、OHLC、成交量/额、委托总量与加权价、涨跌停价、IOPV、
open interest、十档盘口和一档委托队列；它不是两个 normalized event
结构的逐字段镜像。公共时间值的 raw/valid/null/unix-valid、vendor local
time 的完整投影和 `md_stream_id`，以及 instrument/trading-phase 状态
字符串没有进入 V1。市场字段中也未包含价差/市盈率、pre-close IOPV、
涨跌停语义、上交所 ETF/权证/收益率/撤单统计/委托总数/最大持续时间、
alternate weighted price，以及深交所 vendor premium 等字段。这些内容
仍保留在 C++ Store 的完整事件中。若 Python 因子需要它们，应通过后续
显式 ABI 版本扩展，而不能从保留字节推断。

V1 不提供 K 线全历史、远程网络访问、Arrow Flight、共享内存写权限、因子
结果回写或因子状态持久化。Python 因子结果仍由 Python 应用管理。

## 2. 构建

IPC 服务、native reader 和 Python client 目前仅支持 Linux。根 CMake
工程会生成：

```text
mdl-production-router
libl2flow_shm_reader.so
```

运行环境必须支持 `memfd_create` 和 `F_SEAL_FUTURE_WRITE`（Linux
5.1+），并允许服务进程通过 `/proc/self/fd` 为同一 memfd 打开只读
descriptor。缺少这些能力时服务会在启动阶段明确失败，不会降级为可写
共享内存。

示例：

```bash
cmake -S . -B build -DL2FLOW_BUILD_TESTS=ON
cmake --build build -j --target mdl_production_router l2flow_shm_reader
```

`libl2flow_shm_reader.so` 不链接 Python、Arrow 或 Polars。Python 包本身
使用标准库连接控制面，并通过 `ctypes` 调用 native reader。PyArrow 和
Polars 仅在显式请求对应转换时导入。

Python 3.10+ 包可以 editable 安装；核心安装没有第三方运行时依赖：

```bash
python3 -m pip install -e ./python
python3 -m pip install -e './python[arrow,polars]'  # 可选转换依赖
```

## 3. 启动服务

### 3.1 安全目录

先为控制 socket 创建专用目录。推荐模式为 `0700`：

```bash
IPC_DIR="${XDG_RUNTIME_DIR}/l2flow"
install -d -m 0700 "${IPC_DIR}"
```

`--ipc-socket` 必须是绝对路径。服务端会强制校验 socket 的直接父目录：

- 是真实目录，打开时不跟随该目录自身的符号链接；
- owner 是进程的 effective UID；
- group/other 权限位全部为零。

服务不会删除一个预先存在的 socket 路径；存在同名路径时会启动失败。
成功 bind 后 socket 模式被设为 `0600`。退出清理时仅删除与服务启动时
记录的 device/inode 相同的 socket，避免误删后来替换的路径。

### 3.2 启动参数

在现有 `mdl-production-router` 命令上增加：

```bash
./build/mdl-production-router \
  ...现有必需参数... \
  --kline-windows-ms 1000,60000 \
  --ipc-socket "${IPC_DIR}/market-v1.sock" \
  --ipc-tick-ring-records 1048576 \
  --ipc-max-mapping-mib 4096
```

| 参数 | 语义 |
|---|---|
| `--ipc-socket PATH` | 启用 IPC；必须是可放入 `sockaddr_un.sun_path` 的绝对路径 |
| `--ipc-tick-ring-records N` | 全市场 mixed tick ring 的记录数，默认 `262144` |
| `--ipc-max-mapping-mib N` | 整个 memfd layout 的硬上限，默认 `2048` MiB |
| `--kline-windows-ms LIST` | K 线窗口；duration-ms 同时是稳定的 `window_id` |
| `--generation-interval-ms N` | K 线 generation/cut 周期，默认 `1000` ms |

后两个 `--ipc-*` 容量参数只有在同时指定 `--ipc-socket` 时才合法。
router 还会校验 ring capacity 不小于配置下 decoder/store 队列和活跃
worker 可同时持有的 mixed-tick 数量；不足时直接拒绝启动。该下限用于
避免正常排队造成的槽位别名，异常调度或覆盖风险仍由 sequence 校验和
fail-closed 处理。

数据面大小主要由下式决定，最终按页对齐：

```text
4096-byte header
+ registry rows and key bytes
+ 16 bytes × KLine window count
+ 4096 bytes × instrument count          # latest snapshots
+ 512 bytes × instrument count           # latest ticks
+ 256 bytes × instrument × window count × 2  # double-buffered latest KLines
+ 512 bytes × tick ring capacity
```

因此默认 ring 本身约占 128 MiB。容量应按峰值 tick 速率和允许的 consumer
最长暂停时间设置，并保留 worker 重排余量；mapping hard cap 则应覆盖
完整 layout，而不只是 ring。

## 4. 控制面与只读数据面

控制面是 `AF_UNIX/SOCK_SEQPACKET`。V1 支持一次性 `GET_SESSION`，以及在
独立连接上使用的 `OPEN_HISTORY`/`READ_HISTORY`。
服务用 `SO_PEERCRED` 校验客户端 effective UID；不同 UID 不会收到
descriptor。响应使用 `SCM_RIGHTS` 发送 memfd 的只读 open-file
description。

服务内部先创建并映射可写 memfd，再单独取得 `O_RDONLY` descriptor，
最后加入：

```text
F_SEAL_GROW
F_SEAL_SHRINK
F_SEAL_FUTURE_WRITE
F_SEAL_SEAL
```

Python 进程收到的只是只读 descriptor。native reader 再次验证
`O_RDONLY` 和全部必需 seal，并使用 `PROT_READ | MAP_SHARED` 映射。
调用方可在 reader 创建成功后关闭收到的 fd；mapping 拥有独立生命周期。

history 不放进持续变化的 latest mapping。服务端在 `OPEN_HISTORY` 时用
`shared_ptr` 固定一个 immutable Store generation；每次 `READ_HISTORY`
构造一个完成后的页，移除可写 mapping，加入
`F_SEAL_WRITE|F_SEAL_GROW|F_SEAL_SHRINK|F_SEAL_SEAL`，再把重新打开的
`O_RDONLY` fd 传给 Python。Python 校验 fd 类型、大小、只读模式和全部
seal 后才映射该页。显式的零行 EOF 不携带 fd。open 响应给出随机初始
read token，每个数据页响应都会轮换 token；下一次 READ 必须回显刚收到
的 token。因此，在没有猜中新的 64-bit token 的前提下，客户端不能在
收到当前页响应之前预先排队一条有效的后续 READ；这是一项概率性协议约束，
不是数学上不可猜测或永不重复的证明。

这些约束防止普通 consumer 改写或调整数据面，但它们不是跨用户认证或
远程安全协议。同一 UID 下的进程属于同一信任边界。

## 5. Wire ABI

latest/ring 固定 ABI 的权威定义是
[`realtime_wire_v1.h`](../include/l2flow/ipc/realtime_wire_v1.h)，history
控制面和页 ABI 的权威定义是
[`realtime_history_wire_v1.h`](../include/l2flow/ipc/realtime_history_wire_v1.h)。
投影逻辑在
[`realtime_wire_projection_v1.cpp`](../src/ipc/realtime_wire_projection_v1.cpp)。
V1 使用固定宽度、小端字段；共享内存中不放置 C++ enum、`bool`、pointer、
`string`、`variant`、`span`、`size_t` 或 `std::atomic` 对象。
当前协议/ABI 版本是 1.1：minor 1 把 tick payload offset 132 原先的保留
word 定义为 `projection_flags`。1.0 reader 必须在握手时拒绝 1.1，
不能把非零 flag 误判成普通空字符串。

关键固定尺寸为：

| 对象 | 大小 |
|---|---:|
| header | 4096 bytes |
| instrument row | 64 bytes |
| KLine window row | 16 bytes |
| latest snapshot slot | 4096 bytes |
| latest tick / tick-ring slot | 512 bytes |
| latest KLine slot | 256 bytes |
| control request / response | 40 / 64 bytes |
| history open request / response | 64 / 296 bytes |
| history read request / response | 48 / 64 bytes |
| history page header | 4096 bytes |
| history record descriptor | 40 bytes |

header 的 9 个 region descriptor 描述 registry rows、registry key blob、
KLine windows、latest snapshots、latest ticks、latest KLines、tick ring
和两个保留区。header 同时绑定：

```text
magic + ABI major/minor + endian marker
run_id + session_epoch + trade_date
registry_version + registry_sha256
instrument/window counts
server state + flags + heartbeat
tick highest/contiguous sequence + KLine generation
```

每个动态 slot 用一个 64-bit `publish_tag` 发布。writer 在奇数 tag 期间
写入，并以偶数 tag release-publish；native reader 最多进行有限次
acquire/verify/copy，从而把一个一致的 payload 复制到 client-owned
buffer。Python 不应绕过
[`realtime_shm_reader_c_v1.h`](../include/l2flow/ipc/realtime_shm_reader_c_v1.h)
直接解释正在变化的 mmap，因为普通 Python 内存读取不能替代这里的原子
顺序和一致性检查。

latest KLine region 包含两个物理 table。writer 将完整的下一代
instrument×window 表写入非活动 table（无 bar 的 pair 也写入显式空
row），完成后才 release-publish `kline_generation`。reader acquire
该 generation 并按奇偶选择 table，因此正常刷新期间仍可读取上一代，
不会返回部分新 generation 或混代批次。generation 必须从 1 开始严格
递增，且同一 service 只允许一个 KLine publisher。若 generation 恰在
一次批量读取期间切换，native reader 会返回有界的一致性错误，调用方可
立即重试，不会接收混代结果。

公共 record 字段包括 source/ingress/tick-stream/vendor sequence、event
和 receive time、source slot、event kind、market、registry ordinal、
quality flags 和 market notices。价格同时保留 raw、scale、p6
归一化值、valid 和 null 状态；数量保留 raw、scale、valid 和 null
状态。snapshot 还包含最多 10 档买卖盘和最多 50 个一档队列数量；tick
payload 是适用于所有 mixed tick 类型的扁平 superset；K 线 payload
包含 OHLC p6、volume、trade count、revision 和首末事件元数据。

latest tick、tick ring 和 history 共用同一套 tick projection flags。
上交所 `raw_type`/`raw_tick_flag` 超过 32-byte inline 容量时，不截断、
不丢记录：inline 长度和内容置零，并设置对应 omission bit。真实空字符串
是 `length=0, flag=0`，因此不会与 omitted 混淆。Python 对象通过
`Tick.projection_flags`、`raw_type_omitted` 和
`raw_tick_flag_omitted` 暴露该状态；NumPy/Arrow/Polars tick 列也包含
`projection_flags`。

history 页复用固定大小的 snapshot/tick payload，并用 descriptor 保存
Store 的稀疏 `ingress_sequence`、source sequence、mixed-tick sequence、
event kind 和 payload index。`core_v1` 保证所选 instrument 在固定
generation 中的记录覆盖完整，但不保证 C++ Store event 的字段无损：
generation 的 `record_coverage_complete=true`，
`field_complete=false`。跨 source 的原始混合顺序只能按
`ingress_sequence` 恢复。history descriptor 的 omission flags 必须与
对应 tick payload 完全相同。

ABI major 或 layout 不匹配、fd 不是只读、seal 不完整、region
越界/重叠或 registry/window 排序非法时，native reader 拒绝建立 reader，
而不是尝试猜测布局。

## 6. Native C 读取 API

原有进程内
[`RealtimePipelineV1::GetLatestSnapshot(s)` / `GetLatestTick(s)`](../include/l2flow/runtime/realtime_pipeline_v1.h)
继续返回 Store-owned view；它们没有被当作跨进程 ABI。K 线 generation
通过
[`RealtimeKLineGenerationV1::GetLatestBar`](../include/l2flow/market/realtime_kline_v1.h)
提供进程内 latest-bar 查询。

稳定 C ABI 入口在
[`realtime_shm_reader_c_v1.h`](../include/l2flow/ipc/realtime_shm_reader_c_v1.h)：

```c
l2flow_shm_reader_open_fd_v1(...)
l2flow_shm_reader_session_v1(...)
l2flow_shm_reader_instrument_v1(...)
l2flow_shm_reader_resolve_instruments_v1(...)
l2flow_shm_reader_latest_snapshots_v1(...)
l2flow_shm_reader_latest_ticks_v1(...)
l2flow_shm_reader_latest_klines_v1(...)
l2flow_shm_reader_ticks_v1(...)
l2flow_shm_reader_close_v1(...)
```

`resolve_instruments` 对
`(market, security_id_source bytes, security_id bytes)` 做精确比较，
不 trim、不做大小写折叠、不转码，也不根据代码前缀猜市场或 source。
请求顺序和重复 key 保持不变；每项返回 `FOUND`、`UNKNOWN`、
`INVALID_MARKET` 或 `EMPTY_SECURITY_ID`，非 `FOUND` 项的 ID 恒为零。
market 校验先于 empty-ID 校验。C 调用方必须使用互不重叠的输入/输出
数组，并在所有 reader 调用结束后才能调用 `close`；Python wrapper 已用
同一把锁串行化调用和关闭。

latest 批量接口保持请求顺序和重复 ID，并为每一项返回独立状态：

```text
AVAILABLE
NOT_YET_OBSERVED
UNKNOWN_INSTRUMENT
INVALID_INSTRUMENT_ID
UNKNOWN_WINDOW
INVALID_WINDOW_ID
```

其中两个 window 状态只用于 KLine 查询。

每个 `AVAILABLE` row 是一致的 client-owned copy。但一次批量调用不是
跨 instrument 的原子 market cut；不同 row 可以来自不同发布时刻。
snapshot 与 tick 的两次调用也不构成联合 cut。需要固定全市场前缀的
C++ 计算仍应使用不可变 Store generation。

## 7. Python 查询

Python 模块位于 `python/l2flow_realtime`。运行时需要让解释器能找到该
目录，并让 client 找到 native reader：

```bash
export PYTHONPATH="${PWD}/python"
export L2FLOW_SHM_READER_LIBRARY="${PWD}/build/libl2flow_shm_reader.so"
```

也可以通过 `L2FlowClient.connect(..., native_library=...)` 显式传入
shared library 路径。

连接和 latest 查询示例：

```python
from l2flow_realtime import L2FlowClient, LatestStatus

client = L2FlowClient.connect(
    "/run/user/1000/l2flow/market-v1.sock",
    native_library="./build/libl2flow_shm_reader.so",
)
try:
    session = client.session_info()
    print(session.identity, session.server_state)

    one = client.get_latest_snapshot(600000)
    if one.status is LatestStatus.AVAILABLE:
        print(one.value.common.instrument_id, one.value.last_price.p6)

    snapshots = client.get_latest_snapshots([600000, 1])
    ticks = client.get_latest_ticks([600000, 1])
    bars = client.get_latest_klines(
        [600000, 1],
        [1000, 60000],
    )
finally:
    client.close()
```

示例中的数字是 registry 的稳定 `instrument_id`，不是证券代码字符串。
consumer 应在连接后核对 `registry_version/registry_sha256`。
`get_instrument(instrument_id)` 可返回该 ID 的 market、
SecurityIDSource 和 SecurityID；反向查询必须把 registry 中的三个 key
分量原样传回：

```python
from l2flow_realtime import InstrumentKey, Market

entry = client.get_instrument(600000)
key = InstrumentKey(
    Market(entry.market),
    entry.security_id_source,
    entry.security_id,
)
resolved = client.resolve_instruments([key, key])
assert resolved[0].found
assert resolved[0].instrument_id == entry.instrument_id
assert resolved[1] == resolved[0]  # duplicate input is preserved

one = client.resolve_instrument(
    key.market, key.security_id_source, key.security_id
)
```

这里的 bytes 可能包含尾空格或其他不透明字节；调用方不能自行
`strip()`。当前接口支持按 key 反查，不提供一次性枚举全 registry 的新
方法；已知 `instrument_id` 仍可用现有 `get_instrument` 读取。当前
Python API 不提供按 registry ordinal 读取 instrument 的方法。

可用的 point/batch 方法为：

```text
get_latest_snapshot(instrument_id)
get_latest_snapshots(instrument_ids)
get_latest_tick(instrument_id)
get_latest_ticks(instrument_ids)
get_latest_kline(instrument_id, window_id)
get_latest_klines(instrument_ids, window_ids)
resolve_instrument(market, security_id_source, security_id)
resolve_instruments(instrument_keys)
open_instrument_history(instrument_id, requested_page_records=...)
```

point 方法复用同一套 batch/native 实现，不建立另一套读取语义。latest
KLine batch 的两个数组按位置组成 `(instrument_id, window_id)` pair，并非
Cartesian product；传入一个标量 `window_id` 时会对全部 instrument
广播。latest batch 支持 `to_dict()`、`to_columns()`、`to_arrow()` 和
`to_polars()`；后两者缺少相应可选依赖时会明确报错，不影响核心 client
使用。

### 7.1 完整记录覆盖的 Instrument Store history

history cursor 固定打开瞬间“最近一次已发布”的 Store generation；后续
generation 发布不会混入当前扫描。要声明客户端已完整读取，必须一直读到
显式 EOF，让客户端同时核对 generation 总记录数和四路 source 记录数：

```python
with client.open_instrument_history(
    one.instrument_id,
    requested_page_records=4096,
) as history:
    generation = history.generation
    assert generation.record_coverage_complete
    assert not generation.field_complete

    for page in history.pages():
        if page.eof:
            continue
        frames = page.to_polars_by_kind()
        snapshot_frame = frames["snapshots"]
        tick_frame = frames["ticks"]
        # 两张同质表可分别计算；需要恢复 mixed Store 顺序时按
        # ingress_sequence 合并。
```

`pages()` 会返回最后一个零行 `eof=True` 页；成功读取该页后 cursor
自动关闭本地 socket。`records()` 是不把全部历史物化进内存的逐记录
迭代器；完整耗尽该迭代器同样会读取并校验 EOF。若提前 `break`，只能
说明已读取一个前缀，不能声称完成。

每个数据页可用 `to_columns_by_kind()`、`to_arrow_by_kind()` 或
`to_polars_by_kind()` 拆成 snapshot/tick 两个同质因子列子集。列中明确携带
`store_generation`、registry/input identity、`payload_projection`、
逐记录 `projection_flags`、`record_coverage_complete` 和
`field_complete`。`HistoryRecord.value` 的 Python 对象保留收到的完整
CoreV1 payload；列适配器为了因子计算只展开常用标量，并不展开盘口队列
header/50 个队列数量、所有 decimal 的 raw/scale/valid/null 或全部 common
字段。需要这些 CoreV1 字段时应直接读取对象模型或扩展版本化列 schema。

这条接口完成的是“全部记录可验证地读取”，不是 C++ Store event 的字段
无损序列化；因子若需要 core_v1 未暴露的 Store 字段，必须扩展版本化 wire
schema。`input_identity_sha256` 绑定 run、generation、trade date、ingress
cut、registry 和四路 source watermark；它不是页内容或 Store payload 的
摘要。完整性判断来自固定 generation、严格序列、计数对账和显式 EOF，
不能拿该 SHA 当逐记录内容校验值。

已注册但该 generation 中没有记录的 instrument 会直接得到合法 EOF；
未注册 ID 返回 `HistoryNotFoundError`。每个 cursor 占用一个有界服务端
reader slot，并受 idle timeout 限制，应尽量用 context manager 或显式
`close()`。同 UID 是既有信任边界：除非猜中 64-bit token，轮换 token
会拒绝未接收当前响应就提前发送的下一条 READ；但一个已收到 fd 的同 UID
进程仍可故意长期保留页面。页大小/readers 配置不声称限制这种受信任
客户端自身保留的内核内存。

### 7.2 Instrument Store history 延迟 benchmark

仓库提供一个 opt-in、operator-run 的单 instrument 合成 benchmark。它不接
vendor feed，也不修改生产热路径的默认配置；只有显式设置
`L2FLOW_BUILD_BENCHMARKS=ON` 的 benchmark 进程才安装 process-local stage
observer。建议使用独立的新输出目录，并用 Release 构建：

```bash
cmake -S . -B build-history-benchmark \
  -DCMAKE_BUILD_TYPE=Release \
  -DL2FLOW_BUILD_TESTS=OFF \
  -DL2FLOW_BUILD_BENCHMARKS=ON
cmake --build build-history-benchmark \
  --target benchmark_single_instrument_history_stages -j

./build-history-benchmark/benchmark-single-instrument-history-stages \
  --output-dir /tmp/l2flow-history-benchmark-tick \
  --records 100000 \
  --page-records 4096 \
  --warmups 5 \
  --rounds 50 \
  --snapshot-every 0

python3 benchmarks/analyze_single_instrument_history_stages.py \
  /tmp/l2flow-history-benchmark-tick
```

`snapshot-every=0` 是纯 tick，`1` 是纯 snapshot，`N>1` 表示从第 0 条开始
每隔 N 条放一个 snapshot。合成 fixture 只有一个 instrument，全局
`ingress_sequence` 精确为 `1..records`；analyzer 会用首末值、等差数列
求和、`xor(1..N)`、四路 source count 和逐页 cumulative count 验证每次
扫描，而不是只相信 CSV 自报的总数。

一次成功运行产生：

- `benchmark_config.csv`：本次固定 workload、records、分页和
  warmup/measure 数量；
- `client_pages.csv`：每个 READ 一行，包括不携带 memfd 的显式 EOF；
- `client_scans.csv`：从 open 开始、读到 EOF 为止的每次完整扫描；
- `server_pages.csv`：每个成功发送的非 EOF 数据页的 C++ stage；
- analyzer 生成的 `summary_scans.csv`、`summary_stages.csv` 和
  `analysis.json`。

数据页通过
`(benchmark_run_id, open_request_id, generation, page_index)` 精确 join，
并再次核对 `read_request_id` 和行数/类型/字节数；EOF 没有对应 server
stage。warmup 的 scan/page/server 也必须完整 join 和通过语义校验，但不会
进入分布统计。`object_decode_ns` 是 page header/mmap 之后的 Python
descriptor/payload 校验、payload bytes copy、对象解析与
`HistoryRecord` 创建；`column_build_ns` 只覆盖
`_history_columns_by_kind` 的 Python list 构造，不含 Arrow/Polars
materialization。`memfd_ns` 是服务端 prepare 与 finalize 之和，不包含
projection。

`analysis.json` 的 p50/p90/p95/p99 使用 Hyndman-Fan type 7（R-7）线性
插值，并明确记录样本数。少量 round 的 p99 只是对这些完整扫描样本的描述性
插值，不能当作生产尾延迟置信度；正式比较应增加 `--rounds`，固定 CPU/
NUMA/频率和系统负载，并保留 config、原始 CSV、输入哈希、源码哈希与环境
信息。`fully_accounted_residual_ns` 只是 duration-sum 诊断：`sendmsg`
和 `recvmsg` 的尾部可能重叠，因此它不是严格的 wall-time 分解，也可能略
小于零。相反，`requested_residual_data_ns` 使用不重叠的 memfd、
object-decode 和 column-build 子区间，负数会使 analyzer 拒绝该次运行。

### 7.3 latest snapshot 占位因子

若当前只需要验证 Python 因子访问链路，可以直接把 snapshot 的最新价
复制为占位因子，不必使用逐 tick cursor。仓库提供了可运行示例：

```bash
python3 -m pip install -e './python[polars]'

python3 python/examples/latest_snapshot_factor.py \
  --control-socket "${L2FLOW_IPC_SOCKET}" \
  --native-reader "${PWD}/build/libl2flow_shm_reader.so" \
  --instrument-ids 1001,2002 \
  --iterations 1
```

`1001,2002` 只是命令格式示例，必须替换为当前 registry 中的稳定
`instrument_id`。示例只执行一次批量 native read，然后在 Polars 中生成：

```text
placeholder_factor_p6 = last_price_p6
```

因子列保持 nullable `Int64` 和 p6 整数精度；非 `AVAILABLE` 行以及
snapshot 本身没有有效最新价的行不会被填成零。输出还保留 status、
ingress sequence、事件时间、接收时间和 `snapshot_age_ns`，方便核对读到
的是哪一版 latest 值。

持续轮询可使用：

```bash
python3 python/examples/latest_snapshot_factor.py \
  --control-socket "${L2FLOW_IPC_SOCKET}" \
  --native-reader "${PWD}/build/libl2flow_shm_reader.so" \
  --instrument-ids 1001,2002 \
  --iterations 0 \
  --interval-ms 10
```

这仍然是 latest-value polling：两次读取之间的中间 snapshot 可以被覆盖，
不能把它描述为逐 snapshot 不漏消费。多标的应保持一次批量调用；低间隔
轮询时，终端打印和 Polars DataFrame 构造也会进入观测延迟。服务进入
`STOPPED_CLEAN` 后，示例会输出最后一批并退出；heartbeat stale、
coverage lost 或 `FAILED` 会由 client fail-closed，而不是继续计算旧
mapping。

## 8. 逐 tick cursor

`tick_stream_sequence` 是 callback admission 分配的、从 1 开始的全局
dense 序列，覆盖：

```text
Shanghai tick
Shenzhen order
Shenzhen transaction
```

snapshot 的该字段始终为 0。worker 可以乱序完成，因此 header 同时提供：

- `tick_highest_published_sequence`：已经看到的最大序列，可能暂时超前；
- `tick_contiguous_published_sequence`：从 1 开始、确认已发布的连续前缀。

reader 只读取 contiguous prefix，不把 worker 重排误报成 gap。一个成功
的零行读取表示前缀尚未推进，不表示 session 已结束。

Python cursor 示例：

```python
cursor = client.open_tick_cursor(start="earliest")

while True:
    batch = cursor.read(4096)
    if not batch:
        continue
    process(batch)
    save_checkpoint(
        run_id=batch.session_identity.run_id,
        session_epoch=batch.session_identity.session_epoch,
        next_sequence=batch.next_sequence,
    )
```

`start="earliest"` 指当前 ring 中仍保留的最早序列，不保证仍是 session
序列 1；`start="latest"` 从当前 contiguous prefix 之后开始，只消费未来
记录。也可以传入一个正整数恢复显式 cursor。

ring 是有界的，“不漏”成立的前提是 consumer 在覆盖前推进并保存自己的
cursor。若 `expected_sequence` 已被覆盖，reader 返回 overrun 和观测到的
最早/slot 序列，保持请求 cursor 不变；Python 抛出
`TickOverrunError`，不会静默跳到新位置。应用必须按业务策略停止、告警、
从其他可审计数据源补数，或明确选择从新的位置重新开始。任何非成功读取
的部分输出都不应提交 checkpoint。

每个 consumer 独立持有 cursor；服务端不保存 consumer 状态。checkpoint
至少必须包含：

```text
run_id
session_epoch
next_sequence
factor/schema version
```

重连后先比较 `(run_id, session_epoch)`。当前 router 每次进程启动生成新
`run_id`；不能把旧进程的 sequence 直接套用到新 mapping。

## 9. Session、heartbeat 与失败处理

server state 的 wire 值为：

| 状态 | 值 | 含义 |
|---|---:|---|
| `INITIALIZING` | 1 | layout 尚未开放读取 |
| `ACTIVE` | 2 | 正常发布 |
| `DRAINING` | 3 | 已启动关闭 admission 和排空流程 |
| `STOPPED_CLEAN` | 4 | 最终 admitted tick sequence 与 ring 的 highest/contiguous 前缀一致 |
| `FAILED` | 5 | coverage 或发布完整性已经丢失 |

服务启动时写入 heartbeat；之后控制线程在最长约一秒的空闲 poll 周期，
以及处理控制连接前后，用当前 `CLOCK_MONOTONIC` 刷新它。Python client
的 `stale_after_ns` 是本机进程活性策略，不是交易所数据新鲜度定义；
默认是 3 秒，也可以显式传 `None` 关闭。stale 检查只作用于 `ACTIVE` 和
`DRAINING`；`STOPPED_CLEAN` 的静止最终 mapping 不会因 heartbeat 停止而
被误报。市场静默不应仅凭“没有新 tick”判断进程死亡。

任何 applied-record 投影失败、ring 发布失败、K 线投影失败或控制线程
致命失败都会设置 sticky `coverage_lost` 并转为 `FAILED`。native latest
和 tick API 对 `INITIALIZING`/`FAILED` 返回 unavailable，而不是继续
提供可能不完整的数据。`DRAINING` 和 `STOPPED_CLEAN` 中已发布数据仍可
读取。

进程重启后必须重新连接 UDS 并取得新的 memfd。持有旧 mapping 并不能
发现新进程；它只代表旧 session。

## 10. Polars 微批因子

逐条把 Python 对象送入 Polars 通常会让解释器和 DataFrame 构造开销主导
延迟。推荐以“最大行数或最大等待时间先到”为边界形成微批：

```text
cursor.read(max_rows)
  -> consistent client-owned TickBatch
  -> flat columns
  -> Polars DataFrame/LazyFrame
  -> vectorized factor expressions
  -> publish result
  -> commit next_sequence checkpoint
```

示例：

```python
import polars as pl
from l2flow_realtime import Side, TickFactorRunner

cursor = client.open_tick_cursor(start="latest")

def compute_factor(frame):
    return frame.with_columns(
        pl.when(pl.col("side") == int(Side.BUY))
        .then(pl.col("quantity_raw"))
        .when(pl.col("side") == int(Side.SELL))
        .then(-pl.col("quantity_raw"))
        .otherwise(None)
        .alias("signed_quantity_raw")
    )

runner = TickFactorRunner(
    cursor,
    compute_factor,
    max_rows=8192,
    max_latency=0.005,
    as_polars=True,
)

result = runner.run_once()
if result is not None:
    publish_factors(result.value)
    commit_cursor(result.input_batch.next_sequence)
```

`TickFactorRunner.run_once()` 返回 `None` 或包含原始 `input_batch` 和
transform `value` 的 `FactorResult`；它不替应用发布或持久化结果。生产
loop 应同时设置 `max_rows` 和 `max_latency`：高流量时由行数限制单批内存
与尾延迟，低流量时由时间限制因子可见延迟。

扁平 batch 中 `price_p6` 和 `*_quantity_raw` 是 nullable 的安全投影：
底层字段无效或显式 null 时为 `None`，对应 scale 也为 `None`。Python
对象模型仍保留原始 `raw/scale/valid/is_null` 四元组。涉及成交额或跨
品种数量比较时，不能直接把 `price_p6 * quantity_raw` 当成通用名义金额；
必须同时处理 `quantity_scale`、`quantity_unit` 和品种合约语义。

`TickCursor.read()` 在一次 native read 成功后推进进程内 cursor；
持久化 checkpoint 仍应只在 factor 计算和下游发布都成功后提交。若后续
处理失败，应丢弃该 cursor，再从最后一个已提交的 `next_sequence` 创建
cursor 重放，而不是错误地持久化已经推进的进程内位置。

runner 不管理 factor 内部状态。若 transform 持有跨批滚动窗口、上一笔值
或其他可变状态，必须把该状态（或可确定重建它的版本/快照）与
`run_id/session_epoch/next_sequence` 及下游结果原子协调。失败重放前先
回滚或重建 factor 状态，不能在已经改变的 closure 状态上重复应用同一批。

`to_arrow()`/`to_polars()` 的输入已经是 native reader 做过一致性检查的
client-owned copy。正在被 C++ writer 修改的 shared-memory slot 不能
直接作为 Arrow immutable buffer 零拷贝暴露；否则 Arrow/Polars 在计算
期间可能看到被改写的数据。Arrow 到 Polars 的后续转换可能复用 Arrow
buffer，但这不代表从 mutable shared memory 到 Arrow 是零拷贝。

## 11. 正确性与低延迟边界

- latest snapshot/tick 是 O(log instrument count) registry 查找加一次
  固定大小一致性 copy；批量查询摊薄 Python/native 边界成本。
- mixed-tick contiguous prefix 在 History worker 上只推进固定小预算；
  大段乱序 backlog 由 eventfd 唤醒控制线程继续推进，避免缺口闭合把
  单条 tick 热路径变成长扫描。终止校验前会在非热路径同步完成剩余推进。
- latest KLine 只表示最近一次已发布 generation 中该窗口的最新 bar，
  不是 K 线全历史，也不会在每个 tick 后立即刷新。
- history cursor 是周期性 Store generation 的完整记录扫描，不是逐 tick
  热流；需要新到一条即可见仍应使用 tick ring，需要当前值则使用 latest。
- history 页内和跨页都要求 `ingress_sequence` 严格递增，同时分别要求
  四路 `source_sequence` 严格递增；到达 EOF 时还会核对总数和分 source
  计数。`record_coverage_complete` 不等价于 `coverage_from_open`，也不
  证明上游 vendor feed 自身没有缺失。
- latest batch 只保证逐 row 一致，不保证跨 instrument 原子；要做同一
  市场截面的严格比较，应在因子中携带 sequence/time 并定义容忍窗口，
  或回到 C++ immutable generation。
- latest cache 可能跳过中间 tick；需要逐条消费时必须使用 tick cursor，
  不能轮询 `get_latest_tick()`。
- ring 的固定容量限制了最长可暂停时间；增加容量会线性增加 mapping
  内存，但不会改变每次 reader 调用的复制量。
- Python/Polars 路径是只读旁路，不向 C++ Store、latest slots 或 KLine
  写回数据，也不影响行情 admission。

核心实现文件：

- 服务与生命周期：
  [`realtime_shared_service_v1.h`](../include/l2flow/ipc/realtime_shared_service_v1.h)
- wire schema：
  [`realtime_wire_v1.h`](../include/l2flow/ipc/realtime_wire_v1.h)
- history wire schema：
  [`realtime_history_wire_v1.h`](../include/l2flow/ipc/realtime_history_wire_v1.h)
- native C reader：
  [`realtime_shm_reader_c_v1.h`](../include/l2flow/ipc/realtime_shm_reader_c_v1.h)
- production 组合和 `--ipc-*`：
  [`mdl_production_main.cpp`](../apps/mdl_production_main.cpp)
- Python client：[`python/l2flow_realtime`](../python/l2flow_realtime)
