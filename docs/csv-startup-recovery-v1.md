# 盘中启动 CSV 补数与实时接管 V1

## 1. 目标与边界

当 `mdl-production-router` 在交易日盘中才启动时，通联客户端已经生成了从
开盘到当前时刻的 CSV。V1 只提供 online recovery：唯一 SDK owner 先把受支持
callback 深拷贝到有界、异步写盘的 live journal，再立即推进独立
`LIVE_PARTIAL` preview pipeline；CSV 只进入 SDK-less shadow pipeline，
shadow 从 journal 追到固定边界后才开放 recovered 服务。

恢复在同一会话、无损 callback 的显式运维契约下，用精确重叠及
tuple fence/cutoff 验证 CSV/live 闭合接管。实现不会让较旧 CSV 与较新
live callback 直接并发写同一个 Store；preview 与 shadow 始终是两套状态。
生产 Pipeline 不再包含同步 CSV replay、startup live buffer 或恢复完成后的
direct-callback cutoff guard；接缝状态只属于 online coordinator/shadow 路径。

本功能解决的是**同一进程本次启动时的盘中补数**。它不读取任意应用
checkpoint，也不加载上一次进程序列化的内部顺序号、KLine/因子状态或 IPC
cursor；需要的完整 Store、KLine 与 Factor 状态由本次 CSV+live journal 经
shadow 正常路径重新构建。`online` 的 live journal 只服务当前 bootstrap
session：新进程要求 journal 目录为空，不恢复旧 segment，因此它不是进程
崩溃后的自动续跑机制。恢复成功后，各 pipeline 仍生成自己从 1 开始的
`global_ingress_sequence`、`source_sequence` 和 `tick_stream_sequence`。

生产入口要求以下三个 startup mode 恰好选择一个：

```text
--intraday-store-from-open
--intraday-recovery-csv-dir /absolute/path/to/vendor-csv-directory
--intraday-live-partial
```

前两个是能够建立 `coverage_from_open=true` 的 coverage source；第三个明确
表示盘中从本进程启动点开始、且不进行 recovery。它开放 `LIVE_PARTIAL`
latest，并在首次周期 generation 后开放从本进程启动点开始的单标的完整
History 与 tick generation delta；`coverage_from_open` 及全部强完整性 flags
仍为 false。它不属于本文描述的 CSV 恢复流程，也不能随后在同一 run 内升级
为完整 ACTIVE。online recovery 使用的另一条 preview socket 仍只开放 latest，
始终拒绝 History/delta。

选择 CSV 时可选地显式指定：

```text
--intraday-recovery-mode online
```

不显式指定时 CSV 仍进入 online recovery；`blocking` 会被明确拒绝。盘中启动、
不 recovery 时应显式使用
`--intraday-live-partial`；此时用
`--native-maximum-backward-displacement` 声明 per-channel callback disorder
上界并启用 process-start canonical Event，也可通过 `--kline-windows-ms`
显式启用 process-start partial KLine。它保留按交易所
自然时间对齐的窗口，只发布 latest KLine；在 SDK Connect 可能交付 callback
前采样保守的 live coverage boundary。只有已 materialize 的 bar 严格满足
`window_start < boundary < window_end` 时才标记为 left-truncated；无成交窗口
不会合成所谓“首根 bar”。`full_day_kline_valid` 始终为 false。partial Event
明确携带 process-start coverage，且不作全日完整性声明；
`--disable-native-gap-recovery` 则选择 FAST-only partial。盘中启动却使用
`--intraday-store-from-open` 仍是错误的事实声明；该参数只适用于本进程确实
从首条相关市场消息前开始接收并持续健康的会话。

选择 `--intraday-recovery-csv-dir` 是运维方的事实断言：

1. 目录属于 `--trade-date` 指定的同一交易日；
2. 客户端从开盘起持续保存了本进程订阅的全部五类消息；
3. 文件没有被换日文件、另一客户端会话或另一套订阅混入；
4. 在本次启动前，目录没有发生未被发现的数据丢失；
5. 到各 tuple 的 fence/capture 时，writer 已追平 fence 前属于同一订阅的
   已发布消息；唯一允许正在提交的文件边界是一个尚未以 LF/CRLF 完成的
   末行。header/snapshot 边界必须在一次有限 extension 中提交；深圳双文件
   边界必须在配置的 alignment hard deadline 内形成联合连续前缀，否则恢复
   失败。

程序会严格验证格式、关联、原生逐笔连续性和 CSV/live 接缝，但不能从文件
内容单独证明上述外部事实。特别是，CSV `SeqNo` 从 1 开始并不等价于“从
开盘完整”，程序也不能仅凭文件大小证明外部 writer 没有把一条更早收到的
完整消息延迟到 capture 之后才落盘。

production router 没有 arrival-order Event 兼容入口。默认开启的 CERTIFIED
worker 会在隐藏阶段按 canonical 顺序构建 append-only Event journal，并在
promotion barrier 后与 recovered FAST 一起开放 History-to-live-tail API。

## 2. 支持的消息与八个 CSV 文件

生产订阅仍是五个 SDK message tuple；快照的最优价位委托队列由单独的 CSV
子表保存，所以完整输入共八个逻辑文件：

| SDK tuple | 内容 | 主文件名 | 兼容别名/子文件 |
| --- | --- | --- | --- |
| `4.101.4` | 上海 L2 快照 | `mdl_4_4_0.csv` | 主文件别名 `MarketData.csv`；委托队列为 `mdl_4_4_1.csv` 或 `OrderQueue.csv` |
| `4.101.24` | 上海竞价合并逐笔 | `mdl_4_24_0.csv` | 无子文件 |
| `6.101.28` | 深圳 L2 快照 | `mdl_6_28_0.csv` | 卖一队列 `mdl_6_28_1.csv`；买一队列 `mdl_6_28_2.csv` |
| `6.101.33` | 深圳逐笔委托 | `mdl_6_33_0.csv` | 无子文件 |
| `6.101.36` | 深圳逐笔成交/撤单 | `mdl_6_36_0.csv` | 无子文件 |

`MarketData.csv`、`OrderQueue.csv` 是《沪深 L2 行情数据结构展示 V4.0》
使用的上海文件名；`mdl_4_4_0.csv`、`mdl_4_4_1.csv` 是按同一 tuple
映射支持的兼容名称。一个逻辑文件只能解析到一个实际文件；若同一目录同时
存在主名和别名，程序不会猜测谁较新，而是以 alias conflict 失败。

快照主表与队列表共同还原**一条** SDK 快照消息。队列表不是独立的 Store
record。所有由 CSV 重建并成功应用的 record 都携带
`kRecoveredFromCsv` market notice，供查询端与实时 SDK record 区分来源。

## 3. 文件快照与 CSV 解析契约

客户端可以在回放期间继续追加 CSV。每个逻辑消息 tuple 在打开其根文件前，
先把 tuple-local serial fence 与 callback capture 线性化：online 在 live
journal 分配 global/tuple callback serial 的同一 mutex 下取 fence。随后每个
实际文件只打开一次，并
在这个固定文件描述符上 `fstat` 一次取得初始 byte prefix；后续固定前缀
解析使用同一个描述符和显式 offset，不会按路径重新打开而意外读到换名后的
另一 inode：

- 只有 LF 或 CRLF 结束的完整 record 才可发布；
- 初始 cut 尾端尚未提交行终止符的半行绝不直接发布；header、snapshot
  关系仍使用一次有限 extension，深圳 `6.33/6.36` 数据行则进入下述
  `TAIL_ALIGNING` 有界等待；
- 除下述显式 extension round 外，取得 prefix 之后追加的字节不进入本次
  CSV 回放；
- 打开的对象必须是 regular file；文件缺失、打开/读取失败、读取时可见的
  截短、extension 再次 `fstat` 发现缩短、超过配置上限或单行/重建消息
  超过上限都会失败。

运维契约要求文件在打开后只追加，不得原位改写、truncate 后重写或复用同一
inode。固定描述符能防止路径换名/rotation 使本次读取跳到另一文件，也能
发现读不到已捕获长度；它不能证明已经读取的 inode 字节从未被外部原位
覆盖。

八个文件不能在同一个系统调用中原子取快照。快照 tuple 在 fence 后先捕获
子表、最后捕获 root，让 root prefix 成为该 tuple 的主 cut；即便如此，
主表/子表或深圳 `6.33`/`6.36` 的初始 prefix 仍可能恰好切在同一条逻辑
关系两侧。header、独立上海逐笔和 snapshot root/child 保留原有的一次有限
extension 语义：一旦选中该次 `fstat` 长度，其中完整 record 都必须消费并
校验。

- 独立上海逐笔与 snapshot root 的 extension 完整行继续重建并发布；
- snapshot child 的 extension 行先用于补齐 final root cut 内的关联；严格
  大于最终 root `SeqNo` 的 child 行仍须完整解析、检查顺序与重复后才裁掉；
- 深圳 `6.33/6.36` 不再把两次独立 `fstat` 的当前大小当作必须读完的最终
  cut。它区分 `initial_cut`、`available_end` 与 `sealed_cut`，并在固定 hard
  deadline 内重复查看 retained descriptor 的可用末端；文件增长不会重置
  deadline；
- `TAIL_ALIGNING` 每次只提交完整行，并在所有 initial joint
  `ApplSeqNum` gap 与 initial-cut 半行义务首次同时闭合时立即 seal。另一文件
  已解析但未选中的 read-ahead 行仍推进仅用于校验物理文件单调性的内部 scan
  frontier，但不发布、不推进 published tuple cutoff/channel `next_expected`、
  不生成 overlap fingerprint，也不推进 sealed cut；
- 半行起点在 initial cut 之前时必须等待 LF；闭合点之后的新半行属于 live
  suffix，不阻止 seal。schema 错误、重复/倒退、文件缩短、资源上限或 sink
  不健康仍立即 fail-close；目标缺口在 deadline 前未出现则报告
  `boundary_alignment_timeout`，而不是把当前可用 EOF 描述成永久丢包。

深圳 alignment 的默认库配置只做一次立即 refresh；production online recovery
默认等待 10 秒，可用 `--intraday-recovery-boundary-alignment-ms` 在 1..60000
毫秒内调整，且仍受总 warmup deadline 限制。poll 从 2ms 指数退避到 20ms。
等待、`fstat` 和 sleep 只发生在 recovery thread；SDK callback、preview 与
journal capture 不等待 alignment，也不执行磁盘同步。alignment 的绝对期限还会
传入 parser checkpoint、FAST-aware pressure governor 和 shadow admission；持续
背压不能让一次 10 秒 alignment 偷用剩余的总 warmup 预算。

被选中的 extension record 仍须通过 schema、`SeqNo` 与原生顺序校验，并在
属于 sealed prefix 时实际进入重建消息/Store；extension 不是未经校验的
“存在性提示”。sealed cut 之后的全部字节（seal 时已经存在或随后追加）属于
live journal 侧，并继续通过
第 6 节的永久 cutoff guard 验证接缝。

recovery snapshot 与 production final log 暴露
`INITIAL_REPLAY|TAIL_ALIGNING|SEALED` phase、等待时间/poll、实际扫描
record 与 parser-consumed byte（包含 provisional read-ahead 与半行重解析）、
纳入 sealed prefix 的
extension record/byte、pending gap、mandatory partial 以及两文件
initial/sealed/available cut。扫描成本和 sealed extension 分开计量；前者受独立
record/byte hard cap 约束，避免碎片化 append 反复从行首解析而无限消耗冷路径
CPU。底层 `pread` 也受单次剩余 parser budget 限制，但为了顺序解析会按最多
64 KiB 缓冲预取；因此 byte 指标不是精确的 page-cache/I/O 计数，seal 时每个
retained descriptor 最多可能留有一个未消费的预取块。不同 channel 的
`ApplSeqNum` 不可比较；`representative_missing_*` 固定表示当前有 pending 的
最小 `ChannelNo` 及其缺失 frontier，只是稳定的诊断代表，不表示跨 channel 的
等待时间顺序。

`maximum_file_bytes` 仍是 retained physical file 的防御性上限，不只是最终
sealed cut 上限；alignment 期间文件物理大小越过该值会 fail-close，即使多出的
fast suffix 最终可能不被选择。production 当前保持默认 unlimited，实际有界的
冷路径成本由 alignment scan/selected record/byte cap 控制。

当前实现是无 feeder 协议时的安全 reader fallback，不是 committed barrier
证明。若需要严格、可证明的固定边界，feeder 必须让 marker 穿过其完整有序
dispatcher、全部 CSV writer queue 和同一 subscriber callback stream，在
各 writer 完成 marker 前完整行后 ACK 精确 inode/byte cut，并在本地 journal
对 callback marker 取 immutable fence 后原子发布 manifest。仓库中没有这些
feeder writer/dispatcher，不能以普通 `fstat` 或 side-channel ACK 冒充该
证明；任何等待超时仍只能拒绝 promotion，不能安全跳过缺项。
完整的 producer/marker/manifest/consumer 契约见
[`feeder-online-recovery-barrier-v1.md`](feeder-online-recovery-barrier-v1.md)。

CSV 按 UTF-8 与 RFC 4180 字段规则解析。header 必须存在、字段唯一，并与
对应消息的受支持 schema 精确匹配；错误列数、非法引号、非法 UTF-8 或未知
schema 都不会被宽松跳过。`SecurityID` 的前导零和
`SecurityIDSource`、`TradingPhaseCode` 等字符串中的有效尾随字节不会被
trim 或自行规范化。唯一显式的表示映射是 V4 深圳 CSV 的严格文本
`SecurityIDSource=102`：重建 SDK body 时写入本项目 catalog/decoder 使用
的精确四字节 `102 `；CSV 输入本身仍不接受其他拼写或额外空格。

时间按 `HH:MM:SS.mmm` 严格解析，并与 `--trade-date` 组合成当前 session
使用的时间。十进制字段直接从文本解析成目标定点整数，按该 CSV 列声明的
小数位缩放；实现不经过 `double` 往返，因此不会引入二进制浮点舍入。非法
数字、不能按目标刻度精确表示的小数和溢出均为恢复错误，不能用零替代。

这里的“严格”是为了防止一条格式错误记录被静默漏掉。只有各文件最终有限
cut 之后的增长会有意留给实时接管；已经选中的有限 extension 内不存在可
忽略后缀。

## 4. 快照子表关联与字段质量

### 4.1 上海 `4.101.4`

`mdl_4_4_0.csv`/`MarketData.csv` 是 104 列快照主表；
`mdl_4_4_1.csv`/`OrderQueue.csv` 是 62 列队列表。队列行使用关联快照的
`SeqNo` join 回主行，并校验证券代码、行情/本地时间、方向、买卖一档价格与
数量、委托笔数和揭示长度等关联字段。买卖两侧的最多 50 笔委托量被放回
SDK 快照的嵌套 list body。

V4 队列表保存 `OrderQty`，但没有 SDK 嵌套订单项中的 `OrderQueOper` 和
`OrderQueID`。重建时这两个不可获得的字段置为 `0`，并附加
`kCsvSourceFieldUnavailable` market notice；程序不会生成虚构的队列操作
或订单 ID。最优档的 `PriLevOpera` 来自 CSV `PrcLvlOperator`，其余档位按
CSV 能表达的内容构建。客户端可见的上海队列行只接受 V4 对应的
`ImageStatus=1` 或 `3`；未授权状态 `2` 及其他值不会被当作普通队列数据
恢复。

重复关联、孤儿队列、缺少主表要求的队列侧或字段不一致都会 fail-close，
不会发布一个看似完整但实际缺队列的快照。

### 4.2 深圳 `6.101.28`

`mdl_6_28_0.csv` 是 89 列主表，`mdl_6_28_1.csv` 和
`mdl_6_28_2.csv` 分别保存卖一和买一的 62 列队列。两侧同样按 `SeqNo`
关联，并验证证券、时间、方向以及主表一档汇总字段，再重建快照的两个嵌套
队列。V4 对这两个深圳子表约定 `ImageStatus=1`、`NoPriceLevel=1`，且
`PrcLvlOperator` 为保留字段；恢复器要求保留字段为 `0`，不会把任意值送入
SDK body。

V4 PDF 的 `mdl_6_28_0.csv` schema **没有 `ChannelNo` 列**，而当前 SDK
`Snapshot300111_v2` 消息含该字段。实现只在实际 header 明确提供可选
`ChannelNo` 时读取它；标准 89 列文件无法提供该源字段时，重建值严格置
为 `0`，同时在该 record 上携带
`kCsvSourceFieldUnavailable` market notice。程序不会从逐笔 channel、
证券代码或接收顺序推测它。

因此，CSV 可以恢复完整的**消息记录前缀**，但不能把 PDF 未保存的源字段
变成已知值。

## 5. 原生顺序与回放顺序

逐笔连续性使用交易所消息自己定义的原生序号验证：

- 上海 `4.101.24`：在每个
  `(trade_date, SH, Channel)` 内，`BizIndex` 从 1 开始连续；
- 深圳 `6.101.33` 与 `6.101.36`：两类文件必须联合归并，在每个
  `(trade_date, SZ, ChannelNo)` 内共享从 1 开始连续的 `ApplSeqNum`。

深圳不能先回放全部委托文件再回放全部成交文件；同一 channel 的两类记录
必须按 `ApplSeqNum` 合并。重复、倒退或缺口都使恢复失败。上海产品状态
消息也在 `BizIndex` 序列中，必须原样经过 decoder，不能因其不是委托/成交
而丢弃。深圳 `ChannelNo=0` 仍是一个可验证的合法 domain，不会仅因值为零
被拒绝；上海 `Channel` 则按 V4 的正整数约束处理。

文档与消息都没有提供跨上海/深圳、跨 channel 的交易所全局顺序。因此实现
只承诺：

- 同一上海 channel 的 `BizIndex` 顺序；
- 同一深圳 channel 内 6.33/6.36 的联合 `ApplSeqNum` 顺序；
- 每个快照文件自身的 record 顺序；
- journal global callback serial 所表达的 SDK callback 顺序。

实现不声明可以还原开盘以来原始的跨市场或跨 channel callback 全序。
回放时产生的进程内 global/tick 顺序是本次恢复进程的确定性应用顺序，不是
交易所提供的历史全局序号。

CSV 尾部的 `SeqNo` 对应客户端接收/消息头序列。它不用于证明上述交易所
原生连续性，也不能跨五类消息排序。它只在同一个
`(service_id, service_version, message_id)` tuple 内参与 CSV/live 闭合
接管。

V4 表格只把这里的 `SeqNo` 描述为消息/接收序列号；它没有赋予该字段
`BizIndex` 或 `ApplSeqNum` 那样的交易所连续性语义。为了让有界 handoff 在
指纹淘汰后仍能 fail-close，V1 额外把“每个产生 SDK publication 的
root/逐笔物理 CSV，其 `SeqNo` 非零、唯一且按文件行严格递增；live 接管
继续同一 tuple identity domain”定义为**恢复输入协议**并实际验证。快照
child 的 `SeqNo` 是 join key，按各自一侧/方向的关联规则验证，不是独立
publication。深圳按 `ApplSeqNum` 修复跨文件、跨 channel 缺口时，某一
tuple 的 `Publish` 调用顺序可以不同于其物理文件 `SeqNo` 顺序；因此 sink
以精确 `(tuple, SeqNo)` 身份去重，并以该 tuple 所见最大 `SeqNo` 作为
cutoff，不能把最后一次 `Publish` 当成 cutoff。程序也不对
多个 `> cutoff` 的 live `SequenceID` 再发明单调性规则；它保留厂商
callback 顺序。这个接受条件不是对交易所序列的推断，也不能用于填补
`SeqNo` 数值上的空洞。

## 6. 启动状态机与闭合接管

online recovery 使用以下状态机：

```text
建立 live journal 与 INITIALIZING preview IPC
  -> 创建唯一 SDK-owner preview Pipeline 并 SDK Connect
  -> 每个受支持 callback：先 copy/reserve journal，再推进 preview
  -> 建立 recovered IPC、CERTIFIED worker 与 SDK-less shadow Pipeline
  -> 预留 overlap hash/min-heap，预创建 parked recovery thread
  -> preview backlog < 64 且所有依赖健康后开放 LIVE_PARTIAL
  -> release recovery thread
  -> 逐 tuple 在线性化点记录 journal tuple fence，并固定 CSV byte prefix
  -> 深圳初始接缝若未闭合则在 recovery thread 执行 TAIL_ALIGNING
  -> CSV 只经 shadow admission/decoder/History/KLine/Factor/CERTIFIED 回放
  -> 用 journal 中的 SeqNo + 语义 fingerprint/cutoff 验证并去重接缝
  -> CSV 完成后固定初始候选 journal frontier B0
  -> journal reader 只从 durable committed prefix 追到 B0
  -> 等待 shadow applied frontier，并执行无副作用 CERTIFIED/Event FIFO probe
  -> 若 GAP_OPEN/CATCHING_UP，逐个 global serial 消费 journal B0+1... 并重做 probe
  -> 在第一个完整候选处冻结最终 promotion frontier P
  -> 只执行一次 shadow generation 与一次最终 CERTIFIED prefix commit
  -> 两个 recovered control 在共享关闭 gate 后启动
  -> 最终复核、记录 promotion realtime ns，并用一次 release store 开放 gate
  -> 永久执行 cutoff guard，并按 global callback serial 继续消费 P+ journal
```

SDK 必须先连接并开始 lossless capture，不能先读完 CSV 再连接，否则两步
之间存在不可证明的行情空洞。另一方面，CSV 不能在 live 已经推进之后直接
append 到那个 live Store；CSV 只写 shadow Store。CSV 与 journal live record
都必须经过正常 admission、
A 股过滤、catalog 查找、decoder、History、KLine、Factor 与 applied sink，
否则会绕过顺序号、状态机和派生状态。

online preview 的外部承诺仅是 latest snapshot/tick。实现内部仍使用从进程
启动点开始的 partial Store，以保证 IPC latest record 的生命周期；它不是
支持乱序历史插入的 Store，也不是物理上的专用 latest-only map。CSV 永远
不会写进这个 partial Store。

preview SDK 在 control exposure 前连接的隐藏窗口不是丢弃窗口：shared
mapping 在 `INITIALIZING` 状态已经接受 applied record 和 processing progress，
`StartLivePartial` 只改变外部可查询状态，不会清空此前记录。启动前会持续
复核 preview/shadow/journal/CERTIFIED/recovered IPC 健康，并在有界 deadline
内等待 preview lag 低于 64；lag 是压力信号，不是终态失败。启动后的第二次
复核只判断终态健康，不会因为一个合法的瞬时 callback burst 刚跨过 64 就把
已经开放的 session 错误标记为失败。recovery thread 在 exposure 前已经创建，
因此 thread stack/TCB 分配失败也发生在客户端可取得 session 之前。

在 tuple fence 之前或同时已复制的 callback 必须在保留的 CSV 尾部找到
相同 `SeqNo`；找不到就失败。在 fence 之后复制的 callback 有两种合法
情况：若 retained tail 中存在同一身份，则语义完全一致时去重、不一致时
报 overlap conflict；若没有 retained fingerprint，则只有其非零
`SequenceID` 严格大于该 tuple 的 CSV cutoff 才可进入纯 live suffix。
小于等于 cutoff 的身份即使已经从 tail 淘汰也一律失败，不能被重复写入
Store。进入 live suffix 后又回到 CSV prefix 同样失败。

这个 guard 不在 journal reader 追到某个瞬时 frontier 时销毁。CSV writer
可能已经提交某条 record，而对应 callback 尚在厂商
网络/SDK 内部、还没有进入本进程，因此
`journal_accepted == journal_consumed` 不能证明所有重复项已到达。恢复
session 的整个生命周期都保留每 tuple 的 immutable cutoff 与有界尾部指纹：

- 后续 `SequenceID > cutoff` 的 callback 按厂商 callback 顺序正常进入
  实时路径；
- `SequenceID <= cutoff` 只有在尚未进入 live suffix、且仍命中保留指纹并
  语义完全相同时才作为迟到副本抑制；
- 指纹已淘汰、payload 冲突，或进入 live suffix 后返回 CSV prefix，均使
  session fail closed。

因此，某 tuple 的第一条 live callback 确实发生在其 CSV cutoff 之后时，
不必为了制造重叠而等待；暂存期间完全没有该 tuple 消息也合法。但这依赖
运维保证 CSV 保存流与当前 SDK 订阅属于同一无重置的 `SeqNo` identity
domain、消息集合和逐 tuple 顺序。程序可验证实际出现的身份、payload 与
cutoff 关系，不能从 V4 文件本身证明两个外部会话等价。

重叠指纹只保留每个 tuple 中数值最大的有界 K 个 CSV `SequenceID`，而
不是最后 K 次 `Publish`；这保证深圳乱序释放不会淘汰真实物理文件尾部。
handoff 在 preview exposure 前为每个 tuple 的 fingerprint hash 与 `K+1`
min-heap 预留容量；min-heap 仍逐次淘汰最小 identity，但避免原 `std::set`
逐节点分配及 bulk replay 中的 hash rehash。
另保存一个常数大小的 tuple 最大 cutoff，不建立全天 `SeqNo` 集合。二者在
恢复 session 内保持只读。比较使用字段语义而不是动态 string/list 的相对
offset、空字段的非规范 offset 诊断位或整个 body 的原始字节。深圳
快照 `ChannelNo`、上海队列 `OrderQueOper`/`OrderQueID` 等 PDF 明确没有
保存的字段按上述规则规范化；除此之外的可恢复字段必须一致。这样既不把
“CSV 没有该字段”误判成随机冲突，也不会用忽略整个 payload 的方式掩盖
真实冲突。

CSV bulk replay 遇到 decoder 短暂背压时可在有界超时内等待。online callback
不等待 CSV、shadow 或磁盘 `fdatasync`，但必须同步完成一次独立 head/body 深拷贝、
逻辑 WAL 容量 reservation 和有界 writer-queue 入队；任何一步失败都在
preview 推进前 fail-close。磁盘提交、容量和回放背压契约见第 11 节。

## 7. 首个 generation 与 FAST 查询可见性

recovered FAST 映射先以 `INITIALIZING` 建立，可以接收内部
applied record 和 generation publication，但控制线程尚未启动，外部客户端
不能取得一个半恢复 session。

### 7.1 preview 与 promotion

online 在上述隐藏初始化和 backlog/健康门禁通过后开放另一个 socket/run：

```text
server_state=LIVE_PARTIAL
coverage_from_open=false
startup_prefix_recovered=false
full_day_kline_valid=false
full_day_factor_valid=false
certified_prefix_valid=false
```

preview 的 `GET_SESSION` 和 latest snapshot/tick 可用；History 和 tick delta
由服务端拒绝。`LIVE_PARTIAL` 即使随后进入 `DRAINING` 或 `STOPPED_CLEAN`，
也不会因状态名字变化而获得 from-open coverage。正常 heartbeat stale、
coverage-lost 与布局验证仍然适用。门禁前已写入 `INITIALIZING` mapping 的
latest/progress 会原样成为第一个可见 prefix，而不是在 `StartLivePartial`
时从零重新开始。

CSV 完成并确认五个生产 tuple fence 都已捕获后，coordinator 在 journal capture
mutex 的一致快照中读取 `accepted_serial`，固定为有限的初始候选 `B0`。
reader 等待 `committed_serial >= B0`，验证并消费恰好到 `B0`。若此候选的
CERTIFIED/Event FIFO probe 为 `GAP_OPEN` 或 `CATCHING_UP`，同一 reader、tuple
fence、fingerprint/cutoff 和 warmup absolute deadline 会继续保留；coordinator
每次只消费下一个 durable global serial，再等待对应 shadow applied frontier 并
重新 probe。第一个完整候选被冻结为最终 promotion frontier `P`。因此若
`B0+1` 修复旧 gap 而 `B0+2` 打开新 gap，系统会在 `P=B0+1` promotion，不会因
批量追到 `B0+2` 而错过已经完整的前缀。后来的 callback 可以继续获得
`P+1...`，不要求 promotion 时 `accepted_serial == P`。

找到 `P` 前不执行完整 generation cut。只有候选 probe 完整后，shadow pipeline
才执行一次 parked generation fence，等待冻结的 shadow accepted frontier 全部
applied，并发布首个 Store/Factor/KLine generation；发布 watermark 的 accepted
和 applied frontier 都必须等于该冻结 shadow frontier。

默认开启 native-gap recovery 时，online CERTIFIED worker 是 promotion 的
必需组成部分。可重复的 `ProbePrefixFence` 只观察精确 worker FIFO prefix，
不写 `startup_prefix_recovered`、不授权 control；`GAP_OPEN/CATCHING_UP` 是可等待
新 journal record 的候选状态。conflict、resource exhaustion/freeze、dropped
handoff、worker failure 和 seam/journal 错误仍立即终止。找到完整候选并冻结
`P` 后，`WaitForPrefixBarrier` 才执行一次不可重试的最终 commit；只有这个 commit
可以发布 recovered coverage 并授权 recovery-mode control。
显式 `--disable-native-gap-recovery` 时不创建该 sidecar，最终
`certified_prefix_valid=false`。

最终 promotion 在一个进程内 mutex 中与 shutdown 的未完成取消决策互斥。
recovered FAST 与 CERTIFIED control 共用同一个初值为 false 的单调 exposure
gate；accept thread 可以在 gate 后先启动，但 gate 打开前只会关闭新连接，不会
分发请求或传递 memfd。临界区在启动 control 前后都复核 journal 仍处于
healthy WRITING、其 committed frontier 仍覆盖 P、唯一 preview/SDK pipeline
仍在 accepting 且未 fatal/跨交易日、preview IPC 未失败。顺序为：

```text
最早完整候选 P 已冻结
  -> 首 generation 已发布且 watermark 匹配冻结 shadow frontier
  -> CERTIFIED 最终 prefix commit 已完成（若启用）
  -> FAST 在 INITIALIZING 中准备最终 certified_prefix_valid（若启用）
  -> CERTIFIED control 在关闭的共享 gate 后启动（若启用）
  -> recovered FAST control 在同一关闭 gate 后启动并进入 ACTIVE
  -> 再次确认 journal/preview/FAST/CERTIFIED control 健康
  -> 记录非零 system-clock realtime ns
  -> handoff 标记 promoted（tail API 由此变为可用）
  -> 对共享 gate 执行唯一一次 release store(true)
  -> 发布主线程 periodic-cut eligibility
```

因此 completion time 是上述整组操作全部成功后的时间，不是 CSV EOF、B0 捕获
或首 generation cut 的时间。gate 打开是 recovered 对外可见性的线性化点：
此前没有 FAST 或 Event descriptor 能成功转移；此后首个成功 FAST descriptor
已经包含 ACTIVE 与最终强 flags。与 gate 竞争的旧连接允许被拒绝并重试，但
不能取得半 promotion mapping。gate 打开后不再执行可能失败的 promotion 步骤。

preview、recovered FAST 和 CERTIFIED 使用独立 UDS；启用 CERTIFIED 时，两个
recovered accept loop 读取同一个 gate。这里也没有把 preview Store 原地替换为 shadow Store：
preview/recovered 拥有不同
`run_id`，客户端必须显式切换 socket 并丢弃旧 run 的 cursor/checkpoint。
promotion 后 preview 仍保持 partial 查询，shadow 则按 journal global serial
消费 `P+`，并由周期 generation cut 继续发布严格连续的 recovered prefix。
preview header 当前不携带 recovered `run_id` 或自动 redirect；部署层应根据
promotion 日志/健康探针重试 recovered `GET_SESSION`，不能等待 preview 自己
变成 ACTIVE。

## 8. 失败语义

恢复是 all-or-fail，不做“能读多少算多少”。以下情况包括但不限于：

- 必需文件缺失、别名冲突、I/O 或固定 prefix 失败；
- UTF-8、CSV、header、列数、时间、定点数字或消息布局错误；
- 快照主/子表重复、孤儿、缺侧或关联字段不一致；
- 上海 `BizIndex` 或深圳联合 `ApplSeqNum` 重复，或深圳接缝在固定 deadline
  内仍未闭合（`boundary_alignment_timeout`）；
- 恢复协议要求的 tuple-local `SeqNo` 为零、重复、倒退，或 CSV/live
  identity domain 不兼容；
- CSV/live 缺少应有 overlap，或同一 overlap 身份的 payload 冲突；
- ACTIVE 后迟到 callback 命中已淘汰的 CSV 身份、与 CSV 语义冲突，或在
  已进入 live suffix 后返回 CSV prefix；
- online journal writer queue/逻辑 byte budget、CSV
  pending merge、消息池、Store 或其他有界资源耗尽；
- journal 目录非空/不合法、segment create/open/write/`fdatasync` 失败、
  committed record 的 header/session/serial/offset/CRC32C/SHA-256 校验失败，
  或 shutdown 后 committed frontier 未追平 accepted frontier；
- online preview callback 已无法完成独立 capture，或 preview/shadow pipeline
  任一方 fatal；
- online CERTIFIED worker 停止、resource freeze/exhaustion、dropped handoff、
  conflicting duplicate、prefix barrier 或 control activation 失败；
- 回放背压、接管或 applied-prefix 等待超时；
- 首 generation、KLine/Factor publication、FAST/CERTIFIED control activation
  或 promotion completion timestamp 失败；
- 正常生产路径中的 catalog miss、跨交易日或下游应用失败。

任何 recovery、preview、journal、shadow 或 promotion 终态错误都会使本次
进程 fail-close 并退出；promotion 前不会开放 recovered/CERTIFIED，也不会把
部分 shadow Store 标记为“已从开盘恢复”或跳过缺项继续。若以后要求恢复失败
后让 `LIVE_PARTIAL` 长期独立运行，必须先实现 callback capture gate、journal
安全脱离以及 shadow/CERTIFIED 资源回收，不能只忽略 recovery error。

诊断包含错误类别，
CSV/alignment 错误可定位时还包含文件、行号、固定 waited/poll 计数、代表缺失
channel/`ApplSeqNum`、两侧 initial/sealed/available byte cut 和 extension bytes。

## 9. 三种完整性不能混为一谈

| 概念 | 成功恢复后表示什么 | 不表示什么 |
| --- | --- | --- |
| `coverage_from_open` | 运维声明该交易日从开盘到启动点由 CSV 覆盖，之后由闭合 live handoff 连续接管；只有整个恢复成功才允许成立 | 程序仅凭 `SeqNo` 自动证明了外部文件从开盘完整；上游交易所没有延迟发布或漏发 |
| `record_coverage_complete` | 某个已发布 immutable generation/cursor 包含本进程在其 cut 范围内应有的全部 Store record | 载荷包含 SDK 的所有源字段；跨市场原始 callback 全序已恢复 |
| `field_complete` | wire projection 是否无损保留 Store event 的所有字段 | record 数量或时间覆盖完整 |

Wire V2.5 把服务状态与这些事实分开编码。健康 online preview 必须是
`LIVE_PARTIAL`，且 `coverage_from_open`、`startup_prefix_recovered`、
`full_day_kline_valid`、`full_day_factor_valid`、
`certified_prefix_valid` 全为 false。`KLINE_ENABLED` 只表示布局里有 KLine
表，不能替代 `full_day_kline_valid`；CSV online recovery 的 preview 连 KLine
window 也不配置。standalone partial 可显式配置 process-start KLine，但仍不
获得全日有效性。reader 会拒绝以下组合：

- `ACTIVE` 但没有 `coverage_from_open`；
- `LIVE_PARTIAL` 携带任一 from-open/recovered/full-day/CERTIFIED 强标志；
- 任一强标志存在但 `coverage_from_open=false`；
- `full_day_kline_valid=true` 但 KLine 布局未启用；
- 任意未知 header flag bit。

promotion 成功后的 recovered FAST 才会同时声明
`server_state=ACTIVE`、`coverage_from_open=true`、
`startup_prefix_recovered=true` 和适用的 full-day flags；只有启用了
CERTIFIED 且 prefix barrier/control 成功时，
`certified_prefix_valid=true`。`coverage_lost` 是独立的 fail-closed 覆盖项，
一旦置位，原有 from-open 来源声明也不能使 session 继续被视为健康。

现有 CoreV2 history/delta wire 的
`record_coverage_complete=true` 与 `field_complete=false` 约定不因 CSV
恢复而改变。深圳快照缺少 `ChannelNo`、上海队列缺少操作/订单 ID 的 notice
进一步说明：记录可以完整存在，同时某些源字段不可从 CSV 获得。

“从开盘完整”还只表示客户端保存并恢复了**截至当时上游已经发布**的全部
相关消息。例如 V4 文档明确说明上海集合竞价与停牌阶段存在延后统一发布的
逐笔信息；程序不能恢复交易所尚未发布的未来消息。

## 10. 运维检查清单

启动前至少确认：

1. `--trade-date` 是当前 UTC+08:00 交易日，CSV 目录也来自该日；
2. 八个逻辑文件均来自同一客户端接收会话，且每个逻辑文件没有同时出现
   两个别名；
3. 客户端自开盘前已开始保存与本程序相同的五类生产订阅，期间没有重启、
   换目录、清空或 `SeqNo` identity reset；盘中 SDK 订阅与这些文件保持
   同一 identity domain、消息集合和无损 callback 顺序；writer 在各
   capture 点已追平 fence 前消息；若深圳双文件存在短暂跨 writer 可见偏差，
   它必须能在 boundary alignment hard deadline 内闭合。程序不会把
   unresolved initial partial 交给 live，也不会把闭合点之后的 read-ahead
   行误纳入 CSV sealed prefix；
4. 恢复期间这些已打开文件保持 append-only，不原位改写、truncate/rewrite
   或复用 inode；
5. Store record/内存上限足以容纳完整前缀，并同时考虑 preview partial
   Store、shadow full Store、journal queue/cache 和 WAL 磁盘增长；
6. 若需要 CPU 隔离，显式选择 `--event-cpu-set`，并确认它是启动 affinity
   的真子集；
7. 预期 `coverage_from_open` 是运维事实声明，并接受深圳快照
   `ChannelNo=0` 及上海队列操作/订单 ID 为零、同时携带 source-field
   unavailable notice 的字段边界；
8. online 的 preview、recovered FAST、CERTIFIED（若启用）socket 两两不同，
   都不存在旧路径；journal 目录为空、与 CSV 目录不同，底层文件系统有足够
   空间且 I/O 延迟能支撑实测 callback 峰值；
9. 客户端知道 preview 与 recovered 是两个 `run_id`，会在 promotion 后重新
   GET_SESSION/建 cursor，不会跨 run 复用 instrument cursor/checkpoint。

online 示例：

```bash
build/mdl-production-router \
  --sdk-library /absolute/path/to/vendor.so \
  --session-epoch 1 \
  --trade-date 20260730 \
  --daily-catalog /absolute/path/to/daily.catalog \
  --catalog-version 20260730 \
  --server-address HOST:PORT \
  --user-name USER \
  --ipc-socket /absolute/path/to/recovered.sock \
  --live-preview-ipc-socket /absolute/path/to/live-preview.sock \
  --intraday-store-max-records 100000000 \
  --intraday-store-memory-gib 64 \
  --intraday-recovery-csv-dir /absolute/path/to/20260730 \
  --intraday-recovery-journal-dir /absolute/path/to/empty-journal \
  --intraday-recovery-boundary-alignment-ms 10000 \
  --intraday-recovery-progress-interval-seconds 10
```

总 warmup 和 shadow/replay 单条 admission 等待分别使用
`--intraday-recovery-warmup-seconds`（默认 1,800）与
`--intraday-recovery-backpressure-seconds`（默认 30）。
深圳双文件接缝有界等待使用
`--intraday-recovery-boundary-alignment-ms`（默认 10,000）；它不会因文件
继续增长而延长，也不能超过总 warmup deadline。
production 在 promotion 前按
`--intraday-recovery-progress-interval-seconds`（默认 10，范围 1..3,600）输出
一行 `online recovery progress` 结构化日志。日志包含 coordinator phase、五个
物理 tuple 的累计与区间 publication、CSV parser checkpoint、preview/shadow
accepted/applied/lag、journal accepted/committed/consumed/backlog、candidate
frontier、深圳 alignment pending/cut，以及启用时的 CERTIFIED queue/channel
状态。采样只读取现有冷路径快照，不在 SDK callback 或 CSV 每行路径打印日志。

该日志刻意不输出“CSV 完成百分比”。reader 使用 retained descriptor 上的
`pread()`，内核 fd position 不表示 parser cursor；活跃文件还会继续增长，且
严格 `sealed_cut` 只有在联合连续前缀闭合后才成立。运行中可以据 phase、各
tuple publication 增量与处理速率判断实际推进位置，但不能把文件当前大小、
RSS 或经验吞吐换算成完整性百分比。`alignment_sealed=true` 也只证明 reader
fallback 的本次接缝已闭合，不是 feeder ordered-marker/ACK/manifest 证明。
CSV、candidate journal wait、applied wait、probe、cut 和 final commit 的所有
可取消等待共享同一个 steady-clock absolute warmup deadline，candidate retry
不会重置预算。这个 deadline 不是对任意用户 calculator 或 lifecycle syscall 的
强制抢占：`CutAndPublishGeneration` 的 setup/calculator work 仍遵守其既有契约；
final commit 若已通过 `Pending -> Completing` 取得不可逆提交权，调用方会等待
短 commit tail 得出唯一结果，而不会在 metadata 发布后错误返回 timeout。

重叠指纹按 tuple 分开，各保留数值最大的 262,144 个 `SequenceID`；当前生产
入口没有单独的 retention CLI。深圳双文件 merge 另有独立默认上限：
2,000,000 条 pending message 和 512 MiB pending body。

## 11. online journal、追赶与背压契约

### 11.1 callback capture 与 WAL 格式

正常 `--intraday-store-from-open` 路径不创建 online journal。pipeline
配置里的 capture sink 为 null 时，普通 callback 只多一次
nullable pointer 条件判断，仍直接进入原 `Ingest`；不会因此新增第二次
inspect、clock read、message copy、allocation、virtual call 或 queue 操作。

online 配置 capture sink 后，唯一 SDK callback owner 对每个受支持生产 tuple
执行：

```text
capture 阶段执行一次结构 inspect
  -> 读取 realtime/monotonic callback clocks
  -> 深拷贝完整 vendor MDLMessageHead + body
  -> 提取 tuple/source slot/SequenceID/native sequence descriptor
  -> 在一个短 mutex 中分配 global serial + tuple serial
  -> 预留本 record 及可能的新 segment header 的逻辑 WAL bytes
  -> 放入有界 writer queue
  -> 原消息进入 preview Ingest（普通 admission 会再次 inspect）
```

五个受支持 tuple 中的非 A 股 callback 也先进入 journal，随后才由 preview/
shadow 的既有 admission filter 排除。这样它们仍参与 tuple fence、SeqNo、
digest/native descriptor 的闭合证明；过滤发生在 Store admission，而不是在
lossless capture 之前。SDK 仍被配置为一个 callback thread，global serial
因此表示本进程观察到的 callback 顺序，不声称是交易所跨市场全序。

目录中的 segment 名为：

```text
segment-0000000001.wal
segment-0000000002.wal
...
```

segment header 绑定 journal major/minor、little-endian marker、preview
`run_id`、trade date、segment index、first global serial、创建 realtime/
monotonic ns，并带 CRC32C。每条 record 保存：

- record/global/tuple serial 与 segment offset；
- callback receive realtime/monotonic ns；
- tuple、source slot、vendor `SequenceID`；
- 完整 vendor header 与 body；
- 可提取时的 native market/channel/sequence descriptor；
- body SHA-256、body CRC32C 和覆盖 record header+body 的 CRC32C。

reader 还会核对长度、保留字节、tuple/head 一致性、trade date、segment/offset
连续性和严格 global serial。任一错误把整个 journal 标记为 failed；不会
跳过坏 record 寻找下一个看似可读的后缀。

### 11.2 accepted 不等于 committed

`Capture()` 返回 true 的精确定义是：完整消息已有独立所有权、global/tuple
serial 已分配、对应逻辑 byte budget 已 reservation，并已进入有界 writer
queue。它**不表示该 callback 返回前已执行 `fdatasync`**。这样 callback 不
受 CSV 速度或每条磁盘同步延迟阻塞，但 queue/byte reservation 失败会在
preview 推进前立即失败整个 session。

writer 按有界 batch 写 segment。只有当前 batch 的每个 record 都写完，所有
跨越的旧 segment 在 close 前同步、最后 segment 也通过 `fdatasync` 后，才在
mutex 下 release-publish：

```text
committed_serial = batch.back.global_serial
```

shadow reader 只等待和读取 `committed_serial` 覆盖的 prefix；从不把
`accepted_serial` 当作磁盘可读性证明。`StopAndFlush()` 停止新 capture、排空
queue、同步并 join writer，且只在 `committed_serial == accepted_serial` 时
成功。

`--intraday-recovery-journal-max-gib` 限制的是 segment/record header 与 body
的**逻辑序列化总字节数**。实现会在 callback 入队前精确 reservation，但不
做 fallocate，也不保证底层文件系统真的还有相同物理空间；真正的
create/write/sync 失败仍会异步使 preview/recovery fail-closed。segment 大小
必须不大于 total cap。当前 CLI 默认值为 512 GiB total、256 MiB segment、
65,536 个 queued records；目录必须为空，已有 segment 不会被续写或自动
清理。

### 11.3 有限候选 B0、最终 frontier P 与永久 seam guard

online CSV replay 完成后才快照 journal `accepted_serial` 为有限候选 `B0`。这个
快照和 callback serial 分配共用 journal mutex，所以并发 callback 要么属于
`<=B0`，要么严格属于 `B0+`，不存在半分配 record。reader 随后等待 durable
commit 并按 global serial 消费到 `B0`；若 native/Event probe 尚不完整，则
继续逐个 serial 扩展候选，直到冻结最早完整的 `P`：

- 命中 retained `(tuple, SequenceID, semantic digest)` 的 record 是 CSV
  duplicate，payload 相同才抑制；
- 未命中但 tuple serial 不超过该 tuple fence，失败；
- 未命中且 `SequenceID==0` 或 `SequenceID<=CSV maximum cutoff`，失败；
- 只有同时越过 tuple fence 与 immutable maximum cutoff 的 record 才作为
  live suffix 进入 shadow；
- 某 tuple 一旦进入 live suffix，后来再命中 CSV prefix 也失败。

CSV fingerprint 每 tuple 只保留数值最大的 262,144 个 `SequenceID`（当前
生产 online 入口没有单独的 retention CLI）；全天只另存常数大小 maximum
cutoff。若 fence 前身份已因容量淘汰，恢复选择 `overlap_missing` fail-close，
不会猜测它等同于 CSV。production 还把 alignment 的 aggregate selected/scanned
record cap 保守绑定到同一个 262,144，避免一次最小闭包自身就必然超过单 tuple
retention；但 fence 与两个 initial cut 之间的外部 writer 差以及 tuple 分布仍
不能由这个本地 cap 证明，极端大 overlap 仍可能安全地以 `overlap_missing`
失败。应按实测 writer 可见偏差评估 retention 内存，严格消除该歧义仍依赖
feeder manifest barrier。每个候选先等待 shadow applied frontier，再插入
CERTIFIED/Event FIFO probe；只有到达 P 后才执行 parked generation barrier。
P 本身是 journal callback frontier，而
`promotion_shadow_ingress_frontier` 只统计真正进入 A 股 shadow pipeline 的
CSV/live publication，两者数值不要求相等。

promotion 之后同一个 reader 和 seam 状态继续消费 `P+`，所以 cutoff guard
不是启动临时对象。新到 journal record 可能在 shadow 中短暂滞后于 preview；
recovered Store 始终保持从开盘连续的已应用前缀，而 `ACTIVE` 不等价于
“已经读到此刻最后一个 SDK callback”。当前 Wire header 没有单独暴露
journal accepted-minus-consumed backlog，运维应结合 promotion/final 日志和
journal/recovery snapshot 指标观察这一差值。

tail loop 每轮 journal read 最多等待 100 ms。`PumpNext` 在每次 read 前通过
同一个低成本 health gate 复核唯一 preview/SDK owner、shadow、journal 的
lock-free failure bit、preview/recovered FAST 控制面与 CERTIFIED；应用层不会
在其外再重复读取 preview progress cacheline。非 shutdown 场景下任一终态
条件失效会立即把 preview/recovered 标为 FAILED 并停止 CERTIFIED control；
journal write/sync/corruption failure 也不再可能被持续 pressure wait 遮蔽。

promotion 后的正常停止不是上述 owner failure。应用先调用
`BeginCleanShutdownTailDrain(deadline)` 进入单向 clean-drain 状态，再 quiesce
唯一 SDK owner 并发布 preview final generation，随后 `StopAndFlush()` journal。
tail 在 deadline 前不被正常 stop signal cancel，并尽力继续读到 End；pressure
wait、journal read 和随后发起的 shadow admission 都被该绝对 deadline 限制。
若 preview/shadow/CERTIFIED 压力永久不下降，则以 `BACKPRESSURE_TIMEOUT`
fail-close，而不是让 join 永久等待。成功 join 后显式验证
`last_journal_serial == committed_serial == accepted_serial`，最后才发布 shadow
final generation。放宽仅接受 preview 的预期 non-accepting/stopped 状态；fatal、
非法 applied/accepted progress 或 trade-date boundary 仍 fail-close。

### 11.4 FAST-aware work-conserving governor

online 的优先关系是 SDK callback capture/preview 高于 shadow CSV replay。
coordinator 在任何新的昂贵 publication 前检查三类压力：

```text
preview accepted-applied lag       high=64，触发后 drain 到 low=0
shadow accepted-applied lag        high=1024，触发后 drain 到 low=256
CERTIFIED handoff queue            low=50%，high=配置值（默认75%），pause=90%
```

每轮在进入任何 pressure sleep 前先采齐 preview、shadow、journal、两套 FAST
控制面与 CERTIFIED terminal health，避免前一个 backlog 分支遮蔽后一个 owner
failure。CERTIFIED 在 worker-only warmup 阶段不要求 socket control 已启动；一旦
`StartControl()` 成功，accept loop 的异常退出会通过独立原子状态立即变为 terminal。
journal 使用独立 atomic failure bit，控制面使用现有 atomic state，因此不获取
callback capture queue mutex。preview/shadow 达到
high 后以 50 us 间隔复查并等待到 low；draining 状态由单 consumer handoff
持有并跨 100-ms `PumpNext` deadline 保留，因而不会在下一轮 tail poll 提前恢复
灌入。CERTIFIED 的 health 与 utilization 从同一次 service snapshot 得出；
worker stop、resource freeze/exhaustion、dropped handoff、conflicting duplicate
都是 terminal，不能用 sleep 掩盖。达到 90% 时同样暂停并复查。

低于 pause 时调度以 64 个实际 bulk publication 为一个 quantum：无任何压力
时不 sleep、不 yield；CERTIFIED 位于 low/high 或 high/pause 区间时，每个
quantum 最多执行一次 50 us/500 us cooldown，不再逐 record sleep。若 quantum
内观察到新的 preview callback，但 backlog 未达到 high，则在 quantum 边界
cooperative yield 一次，使恢复保持 work-conserving，同时给实时 worker 调度
机会。promotion 后永久 journal tail 本来就是一进一出，不执行 bulk cooldown，
但仍保留 preview/shadow terminal health 和 CERTIFIED 90% pause gate。

publication 不是 CSV 唯一的重工作边界。深圳 `6.33`/`6.36` native-sequence
gap 可能让 parser 持续向 pending map 插入而尚未产生任何 `Publish`；超长 quoted
record 也可能在单行内增长到 MiB。因此 parser 另外在每 256 个完整 logical
record、以及每次新的 64 KiB `pread` 前调用同一 governor。该 checkpoint 不
伪造 bulk publication 计数，并保留准确 source file/record-start line；持续
压力、cancel 或 warmup deadline 会在下一有界 checkpoint fail-close。进入
`TAIL_ALIGNING` 后，checkpoint、CSV `Publish` 的 governor wait 和 shadow
admission 使用更早的 alignment absolute deadline；deadline 返回给 CSV reader
并统一归类为 `boundary_alignment_timeout`，不会先污染 handoff 为另一种终态错误。
外部自定义 sink 若在自己的非协作计算中永久阻塞仍无法被 C++ 虚调用强制抢占，
必须自行实现 deadline-aware variant 的契约。

每条 journal record 不再无条件执行第二次 semantic decode。只有 identity
确实命中 retained CSV overlap 时才计算 semantic digest；已经可证明进入 live
suffix 的 record 直接交给 shadow 的正常 decoder。snapshot 中的
`journal_overlap_digests`、`journal_live_suffix_digests_skipped`、
`csv_parser_checkpoint_events`、preview/shadow pause 与 cooperative-yield
计数用于区分正确性工作和调度动作。

这里仍是应用层 cooperative governor，不是严格 CPU/IO scheduler。一个正在
执行的 `pread`、page fault、allocator、外部 shadow ingress serialization 或首
generation seal 不能在任意指令处抢占；deadline-aware sink 会在这些调用返回后
再次检查绝对期限并拒绝 seal/promotion，但不承诺在任意机器指令处强制中断。
当前也没有 replay CPU quota/affinity CLI。若生产验收要求在整机过载、
共享磁盘拥塞下仍给出硬 p999 上界，必须再用独立 CPU/NUMA、I/O cgroup 或
进程级资源隔离，并在目标机器按下节 benchmark 重复验收。

### 11.5 latency A/B 验证

`benchmark_online_recovery_fast_v1` 是 opt-in target，不注册到 CTest。它用
真实 CSV source、journal、shadow、`LIVE_PARTIAL` Wire V2 服务和 C reader
提供三个模式：

- `ordinary`：null-capture/no-recovery callback 数据面；synthetic control 仍是
  `LIVE_PARTIAL`，不单独证明生产 from-open `ACTIVE` 生命周期；
- `parked`：online journal/handoff 已建立，但 bulk recovery 暂停；
- `active`：50,000 条 CSV 与所有 measured callback 同期恢复。

固定 CPU 后分别运行：

```bash
taskset -c 0-31 ./build/benchmark_online_recovery_fast_v1 --mode ordinary --parallel-decoder-workers 4
taskset -c 0-31 ./build/benchmark_online_recovery_fast_v1 --mode parked --parallel-decoder-workers 4
taskset -c 0-31 ./build/benchmark_online_recovery_fast_v1 --mode active --parallel-decoder-workers 4
```

验收必须交错重复运行，而不是对单次 p999 设 CTest 阈值；同时检查
`history_records == history_expected`、active 的
`promotion_overlap_samples > 0`（并记录实际 overlap 比例）、零丢序/错误状态、
recovery duration/throughput 和全部 governor
telemetry。普通 FAST/CERTIFIED 则继续用
`benchmark_realtime_certified_v1 --all`，从而把“online bulk 的增量影响”和
“无需 online recovery 的常规路径”分开判断。

2026-08-01 的固定 CPU 五轮 paired 结果与完整性/sanitizer 记录见
[`online-recovery-fast-latency-validation-20260801.md`](online-recovery-fast-latency-validation-20260801.md)。
