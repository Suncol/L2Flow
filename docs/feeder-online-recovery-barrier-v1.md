# Feeder online-recovery barrier 设计草案

## 1. 状态与边界

本文记录严格 feeder barrier 的跨进程设计要求，尚不是已发布、可互操作的
wire contract。L2Flow 当前仓库拥有 SDK
subscriber、live journal、CSV reader、shadow replay 与 promotion coordinator，
但不包含 feeder ingress/dispatcher 或 `6.33/6.36` 等 CSV writer。因此本文是
外部 feeder 与本仓库后续集成的设计基线，不能用本地 `fstat()`、路径时间戳或
side-channel ACK 伪装成已经实现的 barrier。

当前无 feeder 协议时使用的深圳 `TAIL_ALIGNING` 是安全 fallback：闭合则继续，
超时则拒绝 promotion。它解决独立 current-EOF 把接缝持续向前推进的问题，但
不证明超时缺项是永久丢失。

## 2. 因果顺序

feeder 必须在一个能够证明所有边界前任务已经完成路由的有序点选择边界。
若源没有全局线性序，边界必须是 per-input-lane frontier vector，不能伪造单个
标量 `P`。

```text
all source work <= P traverses every producer lane/join
        ↓
writer queue:   data <= P | Barrier(id,P) | data > P
callback stream:data <= P | Barrier(id,P) | data > P
        ↓
each writer ACKs exact completed-row byte cut
callback marker snapshots the local journal fence
        ↓
coordinator waits for journal durable frontier
        ↓
atomically publish immutable manifest
```

在 ingress 前端直接向 writer queue 插 marker 是不充分的：尚未结束的并行
decode/format task 可能把 `<=P` 数据晚写到 marker 之后。callback marker 必须
经过同一 subscriber outbound stream；更早到达的控制 socket ACK 不能证明
此前 market callback 已经交付。

## 3. 热路径隔离

- SDK callback 只复制 market message、入 journal queue，并在收到精确 control
  key 时对已分配 serial 做一次小型 fence 快照；不等待 writer、manifest、
  `fdatasync` 或 recovery thread。
- 每个 CSV writer 在自己的有序 queue 中处理 marker。marker 必须使用预留的
  control capacity；queue 满可以令本次 barrier 失败，不能令 source dispatch
  等待空位。
- barrier coordinator 独立等待 ACK、执行可选文件同步和 manifest 发布。
- barrier 只在一次 online recovery 启动时执行。目标语义是超时只放弃
  recovery、健康的 `LIVE_PARTIAL` 保持可用；当前仓库在具备 callback capture
  gate、journal 安全脱离与 shadow 资源回收前，仍对 recovery terminal error
  执行进程级 fail-close，不能用忽略错误来冒充隔离。

普通非 recovery 配置不安装 marker hook，保留现有 callback 路径。安装 hook
时只增加一个 exact message-key 分支；malformed/duplicate marker 记录为
recovery failure，不能返回 live-ingress capture failure。

## 4. Writer ACK

本次订阅使用的全部八个逻辑文件都必须参与，而不只深圳两份逐笔文件。每个
writer 到 marker 时必须完成此前队列元素的 CSV 编码、写出完整 LF 行、flush
用户态缓冲，并从 writer 自己维护的 descriptor/offset 返回：

```text
barrier_id
file_role and exact basename
writer generation
device/inode
cut_bytes
complete_record_count
last tuple-local SeqNo
durability = WRITE_COMPLETE | FDATASYNC_COMPLETE
```

深圳逐笔还需给出每个已出现 channel 的联合 `ApplSeqNum` frontier/hash。控制
线程对文件路径临时 `stat()` 不能替代 writer ACK。发生部分 marker 插入时整个
barrier ID 作废，迟到 ACK 只作审计。

## 5. Callback/journal fence

callback marker handler 必须在 live journal 分配 global/tuple serial 的同一
mutex 下，一次性冻结：

```text
barrier_id
source_frontier_digest
subscriber session identity
accepted_global_serial
accepted_tuple_serials[5]
```

marker 后由 coordinator 再调用普通 `Snapshot()` 是错误的：调度间隙中 P 后
callback 可能已进入 journal，使 fence 越过固定 CSV cuts。marker handler 不
执行 sync；本地 journal writer 继续异步 `fdatasync`，coordinator 只等待
`committed_serial >= accepted_global_serial`。

## 6. Immutable manifest

manifest 至少包含：wire magic/version/length/mandatory flags、CRC32C、SHA-256、
request nonce、barrier ID、trade date、feeder instance/epoch、subscriber session、
subscription identity digest、source frontier vector/digest、callback marker digest、
journal fence digest、八个 file cut、深圳 native frontiers、durability 与创建时间。

发布顺序：

```text
open unique .tmp with O_CREAT|O_EXCL|O_NOFOLLOW
write complete versioned body
fdatasync(tmp)
renameat2(RENAME_NOREPLACE) to barrier-<nonce>.manifest
fsync(manifest directory)
return the exact immutable basename
```

不得覆盖 `latest.manifest`、依赖 symlink 或让 consumer 读取 `.tmp`。若 CSV 未
`fdatasync`，manifest 必须声明 `WRITE_COMPLETE`；这只适用于同一运行的
page-cache 边界，不是掉电持久性证明。

## 7. Consumer 与 promotion gate

strict consumer 必须验证 manifest 的长度/数量上限、未知 mandatory flag、
trade date/nonce/session/subscription digest、角色唯一且完整、basename 不含
slash/`..`/NUL、durability policy、retained descriptor device/inode、
`current_size >= cut_bytes`，并用 `pread(cut_bytes-1)` 验证 LF。文件在 cut 后
继续增长是合法的；prefix 必须固定为 manifest cut，禁止 extension。

barrier 模式下 online handoff 必须安装 marker callback 中冻结的 immutable
tuple fences，不能在 CSV replay 时重新读取 journal 当前 fence。固定 cut 内的
半行、join orphan 或联合 sequence gap 都是 certificate mismatch，必须
fail-close。

CSV certificate 成功后仍须依次通过 journal durable catch-up、SeqNo/semantic
fingerprint 接缝、shadow applied frontier、CERTIFIED/Event native-gap probe、
generation cut 与最终 prefix commit，才允许 promotion gate 打开。

## 8. 超时与资源

barrier deadline 是固定 hard deadline，不因部分 ACK、文件增长或阶段进展而
重置，并且不能超过总 warmup deadline。writer failure、session/frontier/inode
不一致、非 LF cut、journal commit timeout、manifest corruption、资源上限或
取消都只令本次 recovery fail-close。LIVE_PARTIAL 的 callback owner 不等待
这些冷路径操作。
