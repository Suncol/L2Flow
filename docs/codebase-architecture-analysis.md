# L2Flow 代码库架构与端到端流程分析

> 分析范围：当前源码、CMake 调用关系、仓库内验收记录与目标设计文档。
> 分析原则：严格区分“正式四源聚合入口”“legacy 四个 Shadow ingress”“已经接入
> live 聚合的 Phase 2–5 能力”“尚未接入的 Phase 6–8 派生服务”和“外部验收目标”。

## 1. 核心结论

这个仓库已经接通 fresh live 的“Raw → control/market decode → Canonical →
进程内 instrument history → ProductionRoute”，但还不是“Latest State → 数学因子
→ Parquet”全部上线的平台。当前有四个不同层次：

1. 正式单进程 `mdl-production-router`：四路 SDK/Raw source-order pipeline、固定
   instrument worker、进程内 history 与 owner-liveness-bound route；
2. 保留的四个 `mdl-ingress-*`：仍执行 Phase 0–1 Shadow Capture，只是 legacy
   诊断/过渡入口；
3. Phase 6–8 Latest State、factor/Python、Parquet/query library slices：源码和
   定向测试存在，但未接正式聚合入口；
4. 各阶段完整日、真实语料、目标机性能、crash/power-loss/recovery 与数学因子
   external exit 目标。

正式 live 主链是：

~~~text
four MDL SDK callbacks
→ four ByteRing / Raw WAL writers
→ four source-order ControlDecoder + MarketDecoder + Canonical pipelines
→ fixed InstrumentHistoryRuntimeV1 workers
→ one ProductionRoute
~~~

这个判断由实际构建和入口交叉约束：

- <code>L2Flow::production</code> 指向 <code>l2flow_production</code>；
- <code>mdl-production-router</code> 链接该目标并通过严格 deployment manifest 装配；
- <code>ProductionServiceV1</code> 在第一个 SDK Connect 前闭合 capture、pipeline、
  SourceFrontier、registry 与 route identity；
- Active 只在四路 readiness/history barrier 后发布，并由 owner lease 提供跨进程
  存活证明。

阅读本仓库时应使用以下视角：

| 层次 | 当前状态 | 代表组件 |
|---|---|---|
| 正式 live 执行路径 | fresh-only，已接入 | <code>ProductionServiceV1</code>、<code>ProductionAggregateRuntimeV1</code>、<code>ProductionSourcePipelineV1</code>、<code>InstrumentHistoryRuntimeV1</code> |
| Legacy Shadow 路径 | 四个独立程序仍保留 | <code>IngressApp</code>、<code>ShadowCaptureWriter</code> |
| Phase 2–5 基础设施 | Raw live 与 control/market/Canonical 已被正式入口组合；recovery/replay 仍未接 V1 | Raw WAL、live-tail、decoder、Canonical、SourceFrontier |
| Phase 6–8 派生切片 | 未接正式聚合 | Latest State、factor/Python、Parquet/query |
| 外部资格证据 | 未由本次切换宣告完成 | 完整日、真实 corpus、性能、crash/power-loss、recovery、因子数学 |

## 2. 代码库的总体定位

从物理代码看，这个仓库主要解决以下问题：

1. 接入 DataYes MDL 风格的沪深 Level-2 SDK；
2. 把供应商 callback 中的完整 23-byte head 和 body 通过尽可能短的路径捕获下来；
3. 将 callback、磁盘 I/O、readiness 观察和服务运维解耦；
4. 为后续可恢复、可重放的 Raw WAL 提供 Phase 2 基础设施；
5. 提供 SDK-compatible 的合成行情 mock，用于测试和压测。

当前源码树中尚未接入正式聚合主链的模块是：

- Latest State 共享内存；
- Python 因子运行时；
- Parquet/manifest/query/retention/recovery-plan；
- 模型训练、损失函数或推理流程。

Phase 3 control、Phase 4 market decoder 和 Phase 5 Canonical 已进入正式 fresh
live 组合；Phase 6–8 仍是独立 slice。所有阶段尚缺的 external exit 证据不会因
alias 切换自动变成通过。相关路线分别见：

- [Phase 3：Control Decoder](design.md#L6308)
- [Phase 5：Sequence Guard 与 Canonical](design.md#L6384)
- [Phase 6：Latest State](design.md#L6427)
- [Phase 7：Python Factor Runtime](design.md#L6456)
- [Phase 8：Parquet 与查询](design.md#L6512)

## 3. 仓库物理分层

| 目录 | 主要职责 |
|---|---|
| <code>apps/</code> | 四个 ingress 可执行程序的统一 <code>main</code> |
| <code>src/apps/</code>、<code>include/l2flow/apps/</code> | 服务启动、参数解析、systemd notify/watchdog、监控、停止协调 |
| <code>src/sdk/</code>、<code>include/l2flow/sdk/</code> | SDK 抽象、订阅清单、配置、endpoint contract、动态库装载 |
| <code>src/ingress/</code>、<code>include/l2flow/ingress/</code> | callback、ByteRing、Shadow、Raw WAL、Recovery、Replay、Reserve 等数据面 |
| <code>src/ops/</code>、<code>include/l2flow/ops/</code> | credential、metrics、systemd、稳定输出前缀等运维组件 |
| <code>src/common/</code>、<code>include/l2flow/common/</code> | SHA-256、CRC32C、Identity128、sealed snapshot 等基础组件 |
| <code>src/control/</code>、<code>include/l2flow/control/</code> | Phase 3 authoritative control decoder/state |
| <code>src/market/</code>、<code>include/l2flow/market/</code> | Phase 4 safe market decoder、registry 与 full-session history |
| <code>src/canonical/</code>、<code>include/l2flow/canonical/</code> | Phase 5 schema、sequence guard、mmap segment、frontier/mux 与 bundle runtime |
| <code>src/state/</code>、<code>include/l2flow/state/</code> | Phase 6 opaque Latest State slot、local query 与 checkpoint codec |
| <code>src/consumer/</code>、<code>include/l2flow/consumer/</code> | Phase 7 committed batch reader、safe mux/as-of C ABI |
| <code>src/factor/</code>、<code>include/l2flow/factor/</code> | Phase 7 watermark、Latest Factor slot 与 process-local watermark table |
| <code>src/runtime/</code>、<code>include/l2flow/runtime/</code> | 四路 source-order pipeline 与 production aggregate |
| <code>src/route/</code>、<code>include/l2flow/route/</code> | ProductionRoute codec/store/controller、owner lease 与 live guard |
| <code>python/l2flow_factor/</code> | Phase 7 FactorSpec、leased NumPy view、transaction runtime、windows/checkpoint 与显式占位因子 |
| <code>python/l2flow_history/</code> | Phase 8 real-Parquet、manifest/sidecar、bounded query/lineage、retention dry-run 与 recovery/rebuild plan |
| <code>src/baseline/</code>、<code>configs/</code> | 供应商 SDK/library 基线与准入检查 |
| <code>schemas/</code> | Raw、Manifest、Reserve、Recovery 等 Phase 2 wire schema |
| <code>tools/</code> | Raw replay、vendor/ABI probe、mock demo 等工具 |
| <code>include/l2mock/</code>、<code>src/l2_mock.cpp</code> | 独立的 SDK-compatible 合成行情子系统 |
| <code>tests/</code> | Phase 0–8 的单元、集成、恢复和故障路径测试 |
| <code>docs/design.md</code> | 全平台目标设计，不等于当前全部实现 |
| <code>docs/acceptance/</code> | 各阶段本地验收事实和未完成项 |

构建依赖关系可以概括为：

~~~text
l2flow_phase01 → phase2 → phase3 → phase4 → phase5 → phase6 → phase7
                                                               │
                                                               └── l2flow_production
                                                                     ├── L2Flow::production
                                                                     ├── mdl-production-router
                                                                     └── l2flow_ingress_service
                                                                           └── four legacy mdl-ingress-* binaries

l2flow_phase2 → l2flow-raw-replay
~~~

这里容易误读的一点是：四个 legacy `mdl-ingress-*` 的业务代码仍构造
`IngressApp`/Shadow writer；它们链接 production aggregate 的依赖并不等于执行
正式 Raw/Canonical 路径。正式入口只有 `mdl-production-router`。

## 4. Legacy 四进程 Shadow 拓扑

四个 ingress 共用同一个 <code>apps/mdl_ingress_main.cpp</code>。CMake 通过 <code>L2FLOW_INGRESS_KIND=0..3</code> 编译出四个目标；<code>main()</code> 只把编译期 kind 交给 <code>RunIngressService()</code>：[mdl_ingress_main.cpp](../apps/mdl_ingress_main.cpp#L15)、[CMake 目标生成函数](../CMakeLists.txt#L348)。

~~~text
mdl-ingress-sh-snapshot ─┐
mdl-ingress-sh-tick     ─┤
mdl-ingress-sz-snapshot ─┼─> 同一个 RunIngressService(kind, argc, argv)
mdl-ingress-sz-tick     ─┘
~~~

消息三元组以下统一表示为：

~~~text
ServiceID / ServiceVersion / MessageID
~~~

| 进程 | source_stream_id | Required | Optional | Forbidden | 默认 work/io threads |
|---|---:|---|---|---|---:|
| <code>mdl-ingress-sh-snapshot</code> | 1001 | 4/101/4 | 4/101/6 | 无 | 2 / 1 |
| <code>mdl-ingress-sh-tick</code> | 1002 | 4/101/24 | 无 | 无 | 4 / 1 |
| <code>mdl-ingress-sz-snapshot</code> | 2001 | 6/101/28 | 6/101/29 | 6/101/53 | 2 / 1 |
| <code>mdl-ingress-sz-tick</code> | 2002 | 6/101/33、6/101/36 | 无 | 6/101/53 | 4 / 1 |

对应定义在 [subscription_manifest.cpp](../src/sdk/subscription_manifest.cpp#L33)。

几个重要结论：

- SH、SZ 分进程；
- snapshot、tick 也分进程；
- 每个进程有独立 <code>source_stream_id</code>；
- <code>6.53 CombinedTick</code> 被明确禁止，当前不能把它算作核心输入；
- SZ tick 要求同时订阅 6.33 和 6.36；
- Optional 消息只有开启 <code>--include-optional-index=true</code> 时才纳入；
- 当前只建立每个 source stream 内的 ingress 顺序，没有建立四条流之间的全局权威顺序。

## 5. Legacy Shadow 运行链路总图

~~~text
systemd / shell
      │
      ▼
RunIngressService(kind)
      │
      ├── 参数、路径、credential、endpoint contract、vendor baseline 校验
      ├── 动态装载并封存 vendor SDK library
      ├── MetricsWorker
      └── IngressApp
             │
             ├── SDK Manager / Subscriber
             │        │
             │        ▼
             │   UnifiedCallbackRouter
             │        │
             │        ▼
             │   CallbackHandler
             │        │
             │        ▼
             │   SPSC ByteRing
             │        │
             │        ▼
             └── ShadowCaptureWriter thread
                       │
                       ├── shadow capture file
                       └── observational readiness facts
                                  │
                                  ▼
                         service monitor / sd_notify
~~~

必须明确：

- callback 不直接写磁盘；
- callback 不做完整业务字段解码；
- ByteRing 满不会覆盖旧消息，而是触发 fatal；
- 该 legacy sink 是 <code>ShadowCaptureWriter</code>；
- 该 legacy readiness 来自 Shadow writer 对已捕获消息的观察；
- 这四个程序没有调用 <code>RawIngressApp</code> 或
  <code>RawProductionRuntimeV1</code>；正式聚合入口会调用。

## 6. Legacy Shadow 服务启动流程

四个 legacy 程序的入口是 [RunIngressService](../src/apps/ingress_service.cpp#L1042)。

### 6.1 参数解析

服务定义了 14 个已知参数，其中 10 个必选：[ingress_service.cpp](../src/apps/ingress_service.cpp#L62)。

必选参数：

~~~text
--baseline
--archive
--library
--endpoint-contract
--endpoint-contract-sha256
--credential-name
--capture-date
--shadow-path
--sdk-log-prefix
--metrics-path
~~~

可选参数：

~~~text
--credential-path
--ring-bytes
--max-message-bytes
--include-optional-index
~~~

这里没有明文 token 命令行参数。secret 不应出现在 <code>argv</code>、进程列表、日志或 canonical config hash 中。

重要输出路径必须是绝对路径，并进一步经过服务 path policy 检查：[参数验证](../src/apps/ingress_service.cpp#L847)。

Phase 1 配置结构位于 [ingress_config.h](../include/l2flow/sdk/ingress_config.h#L30)。默认最大消息大小是 16 MiB，production ring 的最小容量为 512 MiB：[默认限制](../include/l2flow/sdk/ingress_config.h#L14)。

### 6.2 进程控制边界

服务会：

- 解析 systemd watchdog 环境；
- block <code>SIGINT</code>、<code>SIGTERM</code>，让主监控循环统一处理退出；
- 准备 <code>sd_notify</code> 状态更新；
- 在初始化完成前保持非 READY。

信号不会直接让 SDK callback 异步析构对象，而是由服务主线程进入有顺序的停止流程。

### 6.3 Endpoint contract 与 credential

服务读取 endpoint contract，并要求实际内容 SHA-256 与命令行提供的 hash 匹配。

token 来源是：

- systemd credential；
- 或显式 <code>--credential-path</code> 指向、符合权限与 owner 要求的 credential 文件。

credential 不进入稳定配置 hash。<code>CanonicalIngressConfig</code> 有意只规范化非秘密字段：[ingress_config.cpp](../src/sdk/ingress_config.cpp#L249)。

### 6.4 SDK 日志输出和安全动态装载

<code>OpenStableOutputPrefix</code> 会固定 SDK 日志目录身份，降低初始化期间输出路径被替换的风险。

SDK 动态库不是简单对原始 pathname 直接 <code>dlopen</code>。当前代码的安全边界是：[sdk_runtime.cpp](../src/sdk/sdk_runtime.cpp#L436)

1. 对指定 SDK <code>.so</code> 创建 sealed file snapshot；
2. 对 snapshot fd 执行 approved runtime preflight；
3. 最终 <code>dlopen</code> 指向同一个 sealed snapshot 的 <code>/proc/self/fd/...</code>；
4. <code>dlsym("DllCreateIOManager")</code>；
5. 动态库句柄和 SDK 工厂/对象生命周期绑定。

这样最终加载的文件和此前校验的文件是同一个不可变 snapshot，避免 pathname 的 TOCTOU 替换。

SDK 访问通过 [sdk_runtime.h](../include/l2flow/sdk/sdk_runtime.h) 中的抽象封装：

- <code>SdkFactory</code>
- <code>SdkManager</code>
- <code>SdkSubscriber</code>

因此测试可以注入 mock factory，生产则使用动态 SDK adapter。

### 6.5 构造服务对象

服务随后创建：

- <code>MetricsWorker</code>；
- <code>IngressApp</code>。

legacy 服务实际持有的是 <code>std::unique_ptr&lt;IngressApp&gt;</code>，而不是 <code>RawIngressApp</code>：[服务对象构造位置](../src/apps/ingress_service.cpp#L1275)。

随后依次进入：

~~~text
STATUS=initializing
IngressApp::Initialize()
MonitorIngress()
StopAndReconcile()
~~~

## 7. IngressApp 对象关系与生命周期

<code>IngressApp</code> 的核心成员集中在 [ingress_app.h](../include/l2flow/ingress/ingress_app.h#L126)：

~~~text
IngressApp
├── IngressConfig / IngressSpec
├── SdkFactory / CaptureClock
├── FatalLatch
├── CaptureMetrics
├── ByteRing
├── CallbackHandler
├── UnifiedCallbackRouter
├── ShadowCaptureWriter
├── shadow writer thread
├── SDK Manager
├── SDK Subscriber
└── lifecycle mutex/state
~~~

构造阶段的重要所有权关系是：

- <code>ByteRing</code>、<code>CallbackHandler</code> 先于 SDK manager/subscriber 构造；
- SDK callback 引用的 handler 生命周期长于 SDK 对象；
- 停止时先阻止 callback，再释放 handler 和 ring；
- Shadow writer 是 ring 的唯一 consumer。

相关构造逻辑见 [ingress_app.cpp](../src/ingress/ingress_app.cpp#L92)。

这个顺序用于防止 opaque vendor SDK 在 shutdown 边界发生 callback-after-free。

## 8. IngressApp 初始化和连接顺序

<code>IngressApp::Initialize()</code> 的主线在 [ingress_app.cpp](../src/ingress/ingress_app.cpp#L136)：

1. <code>SdkFactory::Create</code>
2. <code>EnableLog</code>
3. <code>CreateSubscriber(..., false)</code>
4. <code>SetServerAddress</code>
5. <code>SetUserName</code>
6. 配置 heartbeat interval/timeout
7. 配置消息 encoding
8. 配置 merge-message 行为
9. 配置 MAC auth
10. 配置 server select
11. 提交订阅项
12. 启动 <code>ShadowCaptureWriter</code> 线程
13. 调用 <code>Connect</code>
14. 状态进入 Running

<code>CreateSubscriber(..., false)</code> 对应当前 ingress 所依赖的单 callback 执行合同，因为后面的 <code>ByteRing</code> 是 SPSC，而不是 MPSC。

必须先启动 Shadow writer，再调用 <code>Connect</code>。vendor <code>Connect()</code> 可能同步触发 API/SYS/登录等 callback，相关防护逻辑见 [ingress_app.cpp](../src/ingress/ingress_app.cpp#L447)。

## 9. Callback 热路径

所有 callback 最终统一进入 <code>CallbackHandler::CaptureMessage()</code>。接口位置见 [callback_handler.h](../include/l2flow/ingress/callback_handler.h#L24)，实现主线见 [callback_handler.cpp](../src/ingress/callback_handler.cpp#L210)。

### 9.1 callback 重入门禁

进入函数后的第一个关键操作是 <code>atomic_flag callback_gate</code>。

如果发现 callback 重入：

- 增加 reentry 计数；
- trip <code>CALLBACK_REENTRY</code> fatal；
- 不分配 ingress sequence；
- 不读取或写入 ring；
- 尽快退出。

这是因为 ByteRing 的生产侧依赖单 producer。代码不会在重入时加锁后继续，而是把违反 SDK callback 合同视为运行时致命错误。

### 9.2 inflight 和停止检查

正常 callback 会进入 inflight 状态，然后检查：

- 是否已经 <code>BeginStopping</code>；
- 是否已有 fatal；
- 是否仍允许生产新 record。

inflight 计数用于停止阶段的 <code>Quiesce</code>，确保不会在 callback 尚未返回时销毁 handler。

### 9.3 消息 framing 校验

进入 ring 前会检查：

- message 对象非空；
- head pointer 非空；
- <code>HeadSize == 23</code>；
- <code>MessageSize &gt;= HeadSize</code>；
- message 不超过 <code>max_message_bytes</code>；
- ServiceID 只能是 API、SYS 或本 ingress 对应的市场 service；
- body size 非零时 body pointer 必须非空；
- ingress sequence 不能溢出到 <code>UINT64_MAX</code>。

这里做的是 callback framing 和基本边界校验，不是完整业务解码。callback 中不会深入解析证券代码、价格、档位、动态 list 等业务字段。

### 9.4 CaptureMetaV1

每条消息都会生成固定捕获元信息：[capture_meta.h](../include/l2flow/ingress/capture_meta.h#L17)。

<code>CaptureMetaV1</code> 为 40 bytes，主要字段是：

~~~text
source_stream_id
connection_epoch_hint
ingress_sequence
recv_realtime_ns
recv_monotonic_ns
capture_date
flags
~~~

其中：

- <code>ingress_sequence</code> 是本 ingress 捕获链自己的顺序；
- <code>recv_realtime_ns</code> 用于现实时间定位；
- <code>recv_monotonic_ns</code> 用于进程内时间间隔和 replay pacing；
- <code>connection_epoch_hint</code> 只是 hint，不是权威 connection epoch。

权威 epoch 按目标设计必须从 Raw 中按 <code>ingress_sequence</code> 重建 API/SYS/control 消息。

### 9.5 复制完整 callback bytes

写入 ring 的内容是：

~~~text
CaptureMetaV1
+ 完整 23-byte vendor head
+ 完整 body
+ commit_length
~~~

它保存的是 SDK callback 交付的 head/body，不是网络层原始报文、TCP frame 或交易所线路 packet。设计文档也提示不要把 Callback WAL 称为网络原始包：[design.md](design.md#L7149)。

只有 ring publish 成功后，代码才递增 ingress sequence，并更新 published record/byte metrics。

## 10. ByteRing 并发模型

<code>ByteRing</code> 定义为有界、变长、SPSC byte ring：[byte_ring.h](../include/l2flow/ingress/byte_ring.h#L52)。

每个物理 entry 为：

~~~text
CaptureMetaV1
23-byte vendor head
body
uint32 commit_length
~~~

entry 允许跨物理 buffer 末尾，ring 只在构造时分配容量，callback 热路径中不扩容。

### 10.1 Producer 发布

生产侧实现见 [byte_ring.cpp](../src/ingress/byte_ring.cpp#L87)：

1. 检查容量；
2. 复制 meta、head、body；
3. 写入 commit field；
4. 最后对 <code>published_position</code> 做 release store。

consumer 不会先看到可读位置，再看到尚未复制完整的 payload。

### 10.2 Consumer 读取

消费侧见 [byte_ring.cpp](../src/ingress/byte_ring.cpp#L166)：

1. acquire load <code>published_position</code>；
2. 检查物理 entry framing；
3. 校验 head/message size；
4. 校验 commit length；
5. 复制出完整 record；
6. 最后 release store <code>consumed_position</code>。

### 10.3 满 ring 语义

ring 满时不会覆盖旧数据，也不会静默 drop 新消息。production ingress 把 ring full 视为 fatal，后续由服务主循环停机。

这与 mock 子系统中可配置的 <code>DropNewest</code> 不同，不能混为一谈。

## 11. Phase 1 ShadowCaptureWriter

Shadow 文件格式定义在 [shadow_capture.h](../include/l2flow/ingress/shadow_capture.h#L21)：

~~~text
Shadow file header：64 bytes

重复：
    Shadow record header：80 bytes
    vendor head：23 bytes
    body：MessageSize - 23
    zero padding：对齐到 8 bytes
~~~

Shadow 格式的定位是：

- 当前 Phase 1 捕获证据；
- local-host native-endian；
- 用于核对 callback、消息 framing 和 readiness；
- 不是 portable Raw V1；
- 没有 Phase 2 durable journal 和 durable frontier 协议。

### 11.1 消费循环

Shadow writer 是 ByteRing 的唯一 consumer，循环位于 [shadow_capture.cpp](../src/ingress/shadow_capture.cpp#L1000)。

停止后它不会看到一次 EMPTY 就立即退出，而是执行第二次 EMPTY 检查，确保：

1. callback 已经 quiesce；
2. producer 不会再发布；
3. 已发布 entry 被全部 drain。

正常结束时会执行 <code>fdatasync + close</code>：[shadow_capture.cpp](../src/ingress/shadow_capture.cpp#L1712)。

### 11.2 单条 record 处理顺序

单条写入主线见 [shadow_capture.cpp](../src/ingress/shadow_capture.cpp#L1247)：

1. 写 record header；
2. 写 vendor head；
3. 写 body；
4. 写零 padding；
5. 解析与 readiness 有关的 control/market 事实；
6. 最后发布 sink record count。

readiness 观察建立在已交给 Shadow sink 的消息上。

## 12. Phase 1 readiness

Phase 1 readiness 不是简单的“SDK Connect 返回成功”。它由 Shadow writer 对 control 消息和 required market 消息进行观察，主要逻辑见 [shadow_capture.cpp](../src/ingress/shadow_capture.cpp#L1349)。

### 12.1 LogonResponse generation

每次新的 LogonResponse：

- 创建新的 readiness generation；
- 先撤销旧 generation 的 READY；
- 检查登录返回码；
- 不允许把旧连接的订阅成功和新连接的市场消息拼成一个 READY。

### 12.2 Required subscription

只有本 ingress 的 required subscription 全部成功，才满足订阅维度条件。Optional 项不影响默认 READY，Forbidden 项不应被订阅。

### 12.3 Required market first-seen

每个 required market message 至少需要看到一条结构可接受的消息。

判断不仅看 MessageID，还检查：

- fixed body 最小长度；
- 动态 string/list 的 offset 和 range；
- 不能因为收到一条截断或越界 body 就标记 first-seen。

结构不满足 readiness 条件的消息仍可作为捕获事实写进 Shadow，但不能用于证明 required market 已正常出现。

### 12.4 READY 完整条件

最终条件集中在 [shadow_capture.cpp](../src/ingress/shadow_capture.cpp#L1638)：

~~~text
current generation != 0
AND latest LogonResponse 成功
AND 没有失败登录事实
AND required subscription 没有历史 failure
AND required subscription 全部 OK
AND required market 全部 first-seen
~~~

READY 只表示当前 generation 已观察到成功登录、成功订阅和 required market 数据。它不表示：

- Raw WAL 已 durable；
- connection epoch 已权威解码；
- 业务 sequence 连续；
- 四条流已形成一致 snapshot；
- Canonical 或 Latest State 已就绪。

## 13. 服务监控与 watchdog

服务主循环在 [ingress_service.cpp](../src/apps/ingress_service.cpp#L388)。

主要行为是：

1. 每秒采集 capture、ring、sink、readiness 等 metrics；
2. 提交给 MetricsWorker；
3. 对 readiness generation 做稳定双读；
4. 满足条件后发送 <code>READY=1</code>；
5. 只有 READY 且 generation 仍一致时才发送 watchdog；
6. 检查 signal、fatal、readiness lost。

稳定双读避免把旧 generation 的 control facts 和新 generation 的 first-seen 错误拼接。

一旦已经 READY，以下情况会被视为终止性 readiness lost：

- readiness generation 改变；
- READY 条件消失；
- 新的登录失败；
- required subscription failure；
- fatal latch 被触发。

服务不会在失去健康证据后继续向 systemd 喂 watchdog。

## 14. 当前正常停止和对账

<code>IngressApp::Stop()</code> 主线见 [ingress_app.cpp](../src/ingress/ingress_app.cpp#L498)。

正常顺序为：

~~~text
1. CallbackHandler::BeginStopping
2. SDK Manager::Shutdown
3. callback Quiesce
4. ShadowCaptureWriter::StopAndDrain
5. join shadow writer thread
6. release subscriber
7. release manager
~~~

这个顺序维护三个关键不变量：

1. writer 停止前，callback 必须不再生产；
2. handler 被释放前，所有 callback 必须返回；
3. SDK manager/subscriber 的 release 必须在 handler/ring 生命周期内完成。

如果 opaque SDK 无法证明 callback 已收敛，代码采用保守 fail-stop，极端情况下调用 <code>std::terminate</code>，避免 callback-after-free。

服务级停止位于 [ingress_service.cpp](../src/apps/ingress_service.cpp#L625)，会：

1. 发送 <code>STOPPING=1</code>；
2. 调用 <code>app.Stop()</code>；
3. 精确比较 callback 与 sink 的 record count；
4. 精确比较 callback 与 sink 的 vendor bytes；
5. 提交 final metrics；
6. drain MetricsWorker；
7. 发布 <code>STATUS=stopped</code> 或 <code>stopped-with-error</code>。

clean stop 的核心关系是：

~~~text
callback 已发布 records == sink 已写 records
callback 已发布 vendor bytes == sink 已写 vendor bytes
~~~

## 15. Legacy Phase 1 线程模型

| 线程/执行上下文 | 职责 |
|---|---|
| 服务主线程 | 初始化、signal 处理、monitor、systemd notify、停止协调 |
| SDK 内部 work/io 线程 | 网络、协议、SDK 内部处理 |
| SDK callback 执行上下文 | 调用 UnifiedCallbackRouter/CallbackHandler |
| Shadow writer 线程 | ByteRing 唯一 consumer、写 shadow、观察 readiness |
| MetricsWorker 线程 | 异步提交 metrics textfile |

关键约束是：

~~~text
SDK callback：单 producer
Shadow writer：单 consumer
ByteRing：SPSC
~~~

callback 线程不承担：

- 文件 write/fdatasync；
- 完整业务解码；
- readiness 状态机的复杂推进；
- metrics 文件写入；
- Python 调用。

## 16. Phase 2 已实现能力总图

Phase 2 不是只有设计。仓库中已经存在 Raw 数据链、格式、恢复、live-tail、replay 和大量控制面组件。

正式 `mdl-production-router` 已组合其中的 fresh Raw writer、live-tail 和
readiness 能力；四个 legacy `mdl-ingress-*` 仍保持
<code>IngressApp + ShadowCaptureWriter</code>。recovery/takeover/replay 没有接入
当前 fresh-only 正式入口。

~~~text
SDK callback
     │
     ▼
CallbackHandler
     │
     ▼
SPSC ByteRing
     │
     ▼
RawCaptureWorker ──────────────┐
     │                         │
     ▼                         │
RawWalStreamWriter             │
     │                         │
     ├── segment-N.raw         │
     ├── durable.journal       │
     ├── control.page          │
     ├── manifest.json         │
     └── index / sealed artifacts
                               │
RawLiveTailPosixSource ◀───────┘
     │
     ▼
RawReadinessWorker
     │
     ▼
RawReadinessObserver

另一路：
segment + explicit durable limit
     ├── Raw reader
     ├── Recovery analyzer/executor
     └── Raw replay
~~~

## 17. RawIngressApp 与 RawProductionRuntimeV1

Phase 2 顶层对象是 [RawIngressApp](../include/l2flow/ingress/raw_ingress_app.h#L120)：

~~~text
RawIngressApp
├── ByteRing
├── CallbackHandler
├── RawWalSink
├── RawLiveTail
├── RawCaptureWorker
├── RawReadinessObserver
├── RawReadinessWorker
├── capture worker thread
├── readiness worker thread
├── SDK Manager / Subscriber
└── Raw clean-stop gate
~~~

<code>RawProductionRuntimeV1</code> 再向上组合 POSIX live-tail source 和 Raw ingress app：[raw_production_runtime.h](../include/l2flow/ingress/raw_production_runtime.h#L106)。

它提供三类构建路径：

- fresh activation；
- recovered closed activation；
- already-active adoption。

Raw 初始化顺序见 [raw_ingress_app.cpp](../src/ingress/raw_ingress_app.cpp#L523)：

1. 校验 prepared sink；
2. 校验 recovered append/durable cursor；
3. 启动 Raw capture worker；
4. 启动 Raw readiness worker；
5. 创建 SDK manager/subscriber；
6. 配置和 Connect。

与 Phase 1 一样，worker 必须先于 <code>Connect()</code>。Raw callback 仍复用相同的 <code>CallbackHandler + ByteRing</code>，没有把磁盘写入放进 callback。

## 18. Ring 到 Raw WAL

Raw consumer 是 <code>RawCaptureWorker</code>，主循环在 [raw_capture_worker.cpp](../src/ingress/raw_capture_worker.cpp#L52)。

每条 record 的处理顺序是：

1. 从 ByteRing 消费完整捕获 record；
2. 转换成 <code>RawWalRecordInputV1</code>；
3. 调用 sink <code>AppendRecord</code>；
4. 检查 append cursor；
5. 必要时处理 segment rotation；
6. 达到时间或 bytes 阈值时 <code>FlushDurable</code>；
7. clean stop 时 drain、强制 flush、seal。

单条转换见 [raw_capture_worker.cpp](../src/ingress/raw_capture_worker.cpp#L389)。

Phase 2 默认配置包括：[raw_ingress_config.h](../include/l2flow/ingress/raw_ingress_config.h#L14)

~~~text
durable sync interval：10 ms
durable sync bytes：4 MiB
segment target：4 GiB
segment max age：5 min
~~~

这些是 Phase 2 配置默认值，不代表当前 Shadow ingress 在使用这些阈值。

## 19. Raw V1 wire format

Raw V1 常量定义在 [raw_v1.h](../include/l2flow/ingress/raw_v1.h#L17)：

~~~text
segment header：4096 bytes
record header：96 bytes
record trailer：16 bytes
journal header：4096 bytes
durable marker：48 bytes
record alignment：8 bytes
~~~

Raw record header 保存：

- source stream；
- ingress sequence；
- capture realtime/monotonic time；
- capture date；
- connection epoch hint；
- vendor local time/sequence；
- service/version/message/encoding；
- message/body size；
- header/payload CRC32C。

record trailer 保存完整 record size 和 ingress sequence commit 信息。

C++ typed struct 不是直接原样写盘，而是走显式 codec，因此 wire layout 不直接依赖编译器 padding 或 host ABI。

Schema identity 在 [raw_schema.h](../include/l2flow/ingress/raw_schema.h#L12) 中冻结：

- <code>schemas/raw_v1.json</code> exact bytes；
- exact size；
- SHA-256。

但当前 codec 仍是手写源码，尚缺少 schema → generated codec 的构建一致性 gate。因此不能把 generated-codec gate 视为已经完成。

## 20. Raw WAL append 与 durable 协议

Phase 2 架构最重要的原则是：append 可见不等于 durable。

### 20.1 AppendRecord

单 segment writer 实现在 [raw_wal_writer.cpp](../src/ingress/raw_wal_writer.cpp#L397)。

写入顺序：

~~~text
record header
payload
zero padding
----------------
最后单独写 16-byte trailer
----------------
发布 append cursor
~~~

只有 trailer 完整写入后，append cursor 才会向前发布。但此时数据仍可能只在 page cache，不能据此宣称 power-loss durable。

### 20.2 FlushDurable

持久化顺序见 [raw_wal_writer.cpp](../src/ingress/raw_wal_writer.cpp#L512)：

~~~text
1. segment fdatasync
2. 向 durable.journal 追加 durable marker
3. durable.journal fdatasync
4. 发布 durable cursor
~~~

证据链为：

~~~text
segment bytes 已同步
        ↓
journal marker 记录 durable frontier
        ↓
journal 自身也已同步
        ↓
内存 durable cursor 才能向前发布
~~~

权威 durable frontier 来自 durable journal marker，而不来自：

- segment 物理文件大小；
- append cursor；
- <code>control.page</code> 中的快照；
- 内存 metrics；
- “最后一次 write 成功”。

### 20.3 Seal

seal 主线见 [raw_wal_writer.cpp](../src/ingress/raw_wal_writer.cpp#L535)：

1. truncate 到逻辑末尾；
2. sync segment；
3. 追加 <code>SEGMENT_SEALED</code> marker；
4. sync durable journal；
5. close。

## 21. Multi-segment stream 与 rotation

<code>RawWalStreamWriter</code> 在一个逻辑 <code>RawWalSink</code> 后组合多个 physical segments：[raw_wal_stream.h](../include/l2flow/ingress/raw_wal_stream.h#L79)。

~~~text
segment-00000001.raw
segment-00000002.raw
segment-00000003.raw
...
~~~

rotation 逻辑见 [raw_wal_stream.cpp](../src/ingress/raw_wal_stream.cpp#L448)：

1. finalize 当前 segment；
2. 生成 rotation plan；
3. backend 创建下一 segment；
4. 发布含 open entry 的 manifest；
5. 更新 control；
6. 切换 current writer。

finalize 会 seal/close，生成 index/hash/artifact plan，并把 segment 作为 closed entry 发布。

namespace 中的重要固定对象包括：

~~~text
durable.journal
segment-00000001.raw
control.page
manifest.json
writer lease marker
~~~

命名合同见 [raw_namespace.h](../include/l2flow/ingress/raw_namespace.h#L18)。

## 22. control.page 的作用和限制

<code>control.page</code> 是一个 4096-byte host-local mmap ABI：[raw_control_page.h](../include/l2flow/ingress/raw_control_page.h#L19)。

它包含：

- writer/stream identities；
- append cursor；
- durable cursor；
- fatal bit；
- writer heartbeat；
- generation。

发布使用 odd/even generation 的 seqlock 风格：

~~~text
odd generation：更新中
even generation：稳定快照
~~~

reader 需要双读 generation，避免 torn snapshot。

<code>control.page</code> 是低延迟可观测状态，不是 durable 证明。恢复只能相信 durable journal 和经过完整验证的 segment bytes。

## 23. Raw reader

Raw reader 接口在 [raw_reader.h](../include/l2flow/ingress/raw_reader.h#L22)，扫描实现在 [raw_reader.cpp](../src/ingress/raw_reader.cpp#L127)。

调用者必须显式提供：

~~~text
[0, durable_limit)
~~~

reader 不会因为物理文件尾部有更多 bytes，就自动把它们解释成 durable records。

扫描会验证：

- segment header；
- stream/day/segment namespace；
- segment sequence；
- record framing；
- header/payload CRC32C；
- zero padding；
- trailer；
- ingress/WAL 连续性；
- logical end。

<code>RawRecordView</code> 使用 shared owner 保证 span 引用的底层 bytes 在 view 生命周期内有效。

## 24. Recovery

Recovery 抽象位于 [raw_recovery.h](../include/l2flow/ingress/raw_recovery.h#L14)。

架构把分析和磁盘 mutation 分开：

~~~text
Recovery analyzer：只读判断事实
Recovery executor：根据 typed plan 执行 truncate/seal/rename 等 mutation
~~~

分析大体分两遍。

第一遍验证 journal：

- journal header；
- durable marker framing；
- marker CRC；
- marker chain；
- sealed 状态；
- partial marker；
- terminal CRC-invalid marker。

第二遍对每个 journal marker 承诺的 durable range 扫描 segment：

- 调用完整 Raw scan；
- 确认 record framing 和 CRC；
- 验证 namespace、sequence、cursor；
- 不允许仅凭 marker 数值越过真实有效 record。

最终区分：

- accepted durable prefix；
- 完整但未被 durable marker 覆盖的 recovered append-only suffix；
- partial tail；
- invalid tail；
- zero-preallocation tail；
- journal partial/invalid terminal；
- initial anchor/orphan 等情况。

recovered append-only 不等于 durable。它只表示 crash 后发现一段 framing 完整的追加数据，但缺少对应 durable journal 证明。是否允许 replay 必须显式选择并携带 provenance。

## 25. Phase 2 live-tail 与 readiness

Phase 2 readiness 不能成为 ByteRing 的第二 consumer，否则会破坏 SPSC 所有权。因此链路改为：

~~~text
ByteRing
   │
   └── RawCaptureWorker（唯一 consumer）
            │
            ▼
          Raw WAL
            │
            ▼
     validating RawLiveTail
            │
            ▼
     RawReadinessWorker
            │
            ▼
     RawReadinessObserver
~~~

observer 接收的是通过 Raw framing、CRC 和 continuity 校验的 record，而不是直接从 callback ring 读取。

接口见 [raw_readiness_observer.h](../include/l2flow/ingress/raw_readiness_observer.h#L16)，worker 主循环见 [raw_readiness_worker.cpp](../src/ingress/raw_readiness_worker.cpp#L91)。

Phase 2 readiness gate 会检查：

- writer identity；
- namespace identity；
- fatal bit；
- writer heartbeat；
- observer heartbeat；
- recovery boundary；
- append/durable/control 证据；
- observer 与 append frontier 的 lag；
- observer 是否追到新采样的 append frontier。

它仍是 Phase 2 compatibility observer，不是 Phase 3 的 authoritative control decoder。

## 26. Raw replay

独立工具是 <code>l2flow-raw-replay</code>，CLI 实现在 [l2flow_raw_replay.cpp](../tools/l2flow_raw_replay.cpp#L1398)。

它要求为 segment 提供显式 durable logical end：

~~~text
--segment <relative-path> <durable-logical-end>
~~~

而不是只给文件路径后默认 replay 到 EOF。

CLI 会：

1. secure-open segment；
2. 检查 canonical filename；
3. 按显式 durable limit 调用 <code>ScanRawSegmentV1</code>；
4. 校验 segment namespace、sequence、WAL 连续性；
5. 构造 <code>RawReplayEngine</code>。

Replay engine 接口在 [raw_replay.h](../include/l2flow/ingress/raw_replay.h#L17)。

支持的 pacing：

- as fast as possible；
- fixed multiplier；
- original monotonic timing。

支持的过滤维度：

- source stream；
- capture date；
- service/version/message；
- WAL range；
- ingress sequence range；
- realtime range。

默认只 replay durable records。recovered append-only records 必须显式允许，并保留 provenance。

当 clock epoch 变化或 monotonic time regression 时，engine 会先产生 boundary step，避免把跨时钟边界的间隔错误地用于正常 pacing。

当前 replay 的终点仍是 validated Raw view，没有接到 control decoder、normalizer、Canonical 或 factor pipeline。CLI stdout 也不能等同于 durable <code>RunManifest</code> artifact。

## 27. Manifest 与审计模型

### 27.1 RawManifest

定义在 [raw_manifest_v1.h](../include/l2flow/ingress/raw_manifest_v1.h#L86)，描述：

- Raw namespace；
- closed segment entries；
- optional open segment entry；
- closed prefix SHA-256；
- manifest generation；
- 前一个 manifest 到当前 manifest 的 append-only 演进。

编码采用 RFC 8785 JCS 风格规范化。

### 27.2 RunManifest

定义在 [run_manifest_v1.h](../include/l2flow/ingress/run_manifest_v1.h#L166)，描述一次 live 或 replay run 的输入、环境和派生身份，包括：

- run/host/boot/clock identities；
- vendor/build/config；
- Raw inputs；
- durable marker/segment hashes；
- optional factor provenance；
- fault injection 信息。

库中已存在 RunManifest codec 和 POSIX store，但当前四个默认 service 没有把它接入生产 publication path：[phase2-local.md](acceptance/phase2-local.md#L267)。

## 28. Phase 2 startup、reserve 与控制面

### 28.1 配置与恢复状态分离

<code>RawIngressConfig</code> 把稳定配置和恢复出来的 runtime state 分开：[raw_ingress_config.h](../include/l2flow/ingress/raw_ingress_config.h#L29)。

稳定配置不应混入：

- capture date；
- runtime segment sequence；
- writer identity；
- stream-day identity；
- recovered cursors；
- secret token。

### 28.2 Startup planner

Phase 2 startup planner 位于 [raw_phase2_startup_plan.h](../include/l2flow/ingress/raw_phase2_startup_plan.h#L11)。

它的设计特点是：

- state-first；
- mutation-free；
- 只分类下一步动作；
- <code>connect_allowed</code> 始终为 false。

planner 只能判断：

~~~text
需要 fresh activation
需要 recovery
可以 adopt active
必须拒绝
~~~

它不能直接授权 SDK Connect。controller 必须完成 recovery、lease、identity 和 sink 构造，再调用具体 runtime factory。

### 28.3 Reserve 状态

Reserve typed 状态定义在 [reserve_state_v1.h](../include/l2flow/ingress/reserve_state_v1.h#L205)。

包含：

~~~text
Coordinator phase：
PROVISIONED
RELEASING_INTENT
RELEASING_PREPARED
CONSUMED

Registry status：
UNUSED
SCAFFOLDING
INIT
RECOVERING
ACTIVE
~~~

并有 ACK、grant、finalization action、emergency transition 等结构。

### 28.4 当前控制面边界

`ProductionServiceV1` 已提供单进程四源 startup、identity gate、Active publication
与 route-first drain；deployment builder 会 attach 预先 provision 的 reserve
coordinator，并为 fresh route 注册 exact SCAFFOLDING。以下能力仍不在 V1：

- coordinator daemon 或跨进程 AF_UNIX client/server 协议；
- recovery/takeover 路由装配；
- 完整 emergency continuation、archive cleanup 和 reprovision controller；
- 把 legacy 四个独立 ingress 协调成同一个 production generation。

因此，正式入口是“单进程 fresh-only 控制器”，不能描述成完整 recovery 控制面。

## 29. l2mock：独立测试子系统

仓库还有一套独立 SDK-compatible mock。README 明确说明它生成 synthetic 数据，API/payload 兼容不代表真实交易业务语义完全等价：[README.md](../README.md#L1)。

其对外 API 位于 [l2_mock.h](../include/l2mock/l2_mock.h#L41)，提供：

- <code>Config</code>
- <code>Instrument</code>
- <code>SupportedMessages</code>
- <code>Statistics</code>
- <code>CreateIOManager</code>
- <code>SubscribeAll</code>
- <code>GetStatistics</code>

内部流程大致是：

~~~text
Generator
   │
   ├── 按 synthetic instrument 维护状态
   ├── 轮询 message descriptor
   ├── mutate synthetic state
   └── 构造 MDLMessage
          │
          ▼
        filter
          │
          ▼
         pace
          │
          ├── 同步 callback
          └── bounded queue
                 │
                 ▼
          callback worker threads
                 │
                 ▼
     vendor MessageHandlerBase::OnMessage
~~~

Generator 逻辑见 [l2_mock.cpp](../src/l2_mock.cpp#L715)，engine 主循环见 [l2_mock.cpp](../src/l2_mock.cpp#L2827)，bounded queue/backpressure 见 [l2_mock.cpp](../src/l2_mock.cpp#L2891)。

mock queue 支持 Block 和 DropNewest；callback exception 会被隔离并计数。

CMake/README 还区分：

- <code>L2Flow::l2mock_standalone</code>：包含 SDK helper fallback，可不链接真实 vendor <code>.so</code>；
- <code>L2Flow::l2mock</code>：用于与真实 vendor library 混合链接，不包含可能抢占 vendor symbol 的 fallback。

边界说明见 [README.md](../README.md#L302)。

正确关系是：

~~~text
production ingress：
真实 vendor SDK → IngressApp/RawIngressApp

test/mock：
l2mock → 提供 SDK-compatible synthetic callback

两者可以通过 SDK 抽象衔接，但不是同一个运行拓扑。
~~~

## 30. 架构不变量与失败语义

### 30.1 callback 不做磁盘 I/O

callback 只做 framing 校验、采集时间、分配 per-ingress sequence、复制 head/body 和发布到 ring。vendor callback 延迟不直接依赖磁盘抖动。

### 30.2 单 source stream 内保持捕获顺序

每个 ingress 使用：

~~~text
单 callback producer
→ 单 ByteRing
→ 单 sink consumer
~~~

以维护 per-stream <code>ingress_sequence</code>。它不自动建立四条流之间的权威全序。

### 30.3 不允许静默覆盖或丢消息

production ByteRing 满时触发 fatal，不覆盖旧 entry。

### 30.4 捕获事实先于业务解释

Raw/Shadow 保存 callback head/body，再由后续 observer/decoder 解释。即使 decoder 暂时不认识某条消息，捕获事实仍可用于审计和重放。

### 30.5 append 和 durable 分离

~~~text
append cursor：完整 record 已写入并发布
durable cursor：segment + journal 持久性协议已完成
~~~

二者不能混用。

### 30.6 control page 不是持久化证明

控制页适合低延迟观察，recovery authority 只能来自 journal 和经过完整 scan 的 bytes。

### 30.7 clean stop 精确对账

Phase 1 要求 callback records/bytes 与 Shadow sink 一致；Phase 2 进一步要求 callback、append、durable、seal/finalization 形成可解释的收敛关系。

### 30.8 SDK 生命周期包住 callback 生命周期

如果无法证明 callback quiescence，就不能释放 handler/ring。

### 30.9 secret 不参与稳定配置 hash

这样可以避免泄露 credential，也避免 secret rotation 无意义地改变数据 schema/config identity。

### 30.10 readiness 和 durability 是两个维度

READY 证明当前服务具备所需的观察事实；durable 证明哪些 Raw bytes 在 crash/power-loss 语义下可恢复。二者不能互相替代。

## 31. 当前实现与目标设计差距矩阵

| 能力 | 正式聚合是否使用 | 准确结论 |
|---|---:|---|
| pinned sealed SDK loader | 是 | baseline 文件与 SDK archive 先做不执行 library 代码的静态校验；随后 SDK library digest、ELF/ABI/runtime gate 与最终 `dlopen` 绑定同一 sealed snapshot |
| CallbackHandler + SPSC ByteRing | 是 | 四个 source 各一个 callback producer/Raw consumer |
| Shadow capture | 否 | 只由四个 legacy `mdl-ingress-*` 使用 |
| Raw WAL/journal/live-tail/readiness | 是 | fresh live 路径已接入；Raw 仍是持久化 authority |
| Raw recovery/takeover | 否 | primitives 存在，正式 V1 不装配 recovered/adopted runtime |
| Raw replay CLI | 不适用 | 独立工具只输出 validated Raw view，不是正式 pipeline recovery |
| reserve coordinator | 是，进程内 attach | 要求外部预先 provision；没有 daemon/IPC/reprovision controller |
| authoritative control decoder | 是 | 每 source 在 market decode 前按 Raw 顺序推进 |
| safe market decoder | 是 | 五类 core message；SH 4.24 在拆分前单线程推进状态 |
| Canonical bundle/SourceFrontier | 是 | fresh fixed-capacity generation；无日中 rotation/checkpoint attach |
| fixed instrument history workers | 是 | 16 logical shards、1–16 physical workers、进程内查询、无跨源全序 |
| ProductionRoute + owner liveness | 是 | Active 需 owner lease；跨进程 reader 必须持有 live guard |
| Latest State | 否 | Phase 6 本地切片，未接正式 writer/endpoint |
| Native/Python factor runtime | 否 | Phase 7 本地切片；五个命名实现仍是无数学值 placeholder |
| Parquet/query/retention | 否 | Phase 8 本地 package；retention 只 dry-run，未接正式路由 |
| external production qualification | 否 | 完整日、真实 corpus、性能、crash/power-loss、recovery 与因子数学证据仍待完成 |

目标架构方向是：

~~~text
MDL
  ↓
四路 ingress
  ↓
Raw Callback WAL
  ↓
control decoder / safe decoder
  ↓
sequence guard / normalizer
  ↓
Canonical mmap log
  ├── Latest State SHM
  ├── Python Factor Runtime
  └── Parquet / Query / Replay
~~~

当前真实状态应画成：

~~~text
正式 mdl-production-router：
MDL → callback → Raw WAL → source-order control/market decode
    → Canonical → fixed instrument history → live ProductionRoute

Legacy four mdl-ingress-*：
MDL → callback → ByteRing → Shadow

尚未接正式聚合：
Latest State writer → factor executor → Parquet/query service

仍待外部验证/实现：
完整日资格证据 + recovery/takeover/replay composition + 派生服务 cutover
~~~

## 32. 三层事实模型

### 32.1 Raw callback 事实

保存 SDK 实际交付的：

~~~text
capture meta
+ 23-byte vendor head
+ complete body
~~~

优点：

- 不依赖当前 decoder 是否正确；
- 可以恢复和重放；
- 可以定位第一次出现差异的 WAL position；
- malformed/unknown message 也能被审计。

### 32.2 Canonical 规范化事件

经过 control epoch、安全字段解码、vendor/exchange sequence guard、quality 标注和
normalizer，形成稳定、可比较的固定结构。Phase 3–5 已提供这一层的库内 schema、
normalizer、mmap segment、SourceFrontier 与同 source bundle runtime，并有定向测试；
它已经接入 fresh-only 的正式四源聚合，但没有完成正式 Phase 5 external exit，
也没有接入 Python factor consumer。

### 32.3 Latest/Factor 派生状态

Latest State、因子、Parquet 都是可从前两层重建的派生物，而不是原始事实。Phase 8
本地 package 保存 exact Canonical bytes、factor 输入身份与 scoped Raw receipts，但这不
改变 Raw 的 authority。

目标恢复链是：

~~~text
同一 Raw
→ 新 decoder/normalizer generation
→ 新 Canonical
→ 重建 Latest/Factor
→ 对账 hash
~~~

这是恢复目标和本地计划契约；当前正式 V1 只实现 fresh live 物理链路，尚未把
该恢复链装配进入口。

## 33. 推荐源码阅读顺序

### 33.1 正式入口

1. [mdl_production_main.cpp](../apps/mdl_production_main.cpp)
2. [production_deployment_v1.h](../include/l2flow/apps/production_deployment_v1.h)
3. [production_service_v1.h](../include/l2flow/apps/production_service_v1.h)
4. [production_aggregate_runtime_v1.h](../include/l2flow/runtime/production_aggregate_runtime_v1.h)
5. [production_source_pipeline_v1.h](../include/l2flow/runtime/production_source_pipeline_v1.h)
6. [instrument_history_v1.h](../include/l2flow/market/instrument_history_v1.h)
7. [production_route_controller_v1.h](../include/l2flow/route/production_route_controller_v1.h)
8. [production_route_owner_lease_v1.h](../include/l2flow/route/production_route_owner_lease_v1.h)

### 33.2 Legacy Phase 1 数据面

1. [mdl_ingress_main.cpp](../apps/mdl_ingress_main.cpp)
2. [IngressApp](../include/l2flow/ingress/ingress_app.h)
3. [CallbackHandler](../src/ingress/callback_handler.cpp)
4. [ByteRing](../src/ingress/byte_ring.cpp)
5. [Shadow consumer](../src/ingress/shadow_capture.cpp)
6. [RunIngressService](../src/apps/ingress_service.cpp)

### 33.3 Raw/恢复独立能力

1. [RawIngressApp](../include/l2flow/ingress/raw_ingress_app.h)
2. [RawProductionRuntimeV1](../include/l2flow/ingress/raw_production_runtime.h)
3. [RawCaptureWorker](../src/ingress/raw_capture_worker.cpp)
4. [Raw V1 format](../include/l2flow/ingress/raw_v1.h)
5. [RawWalWriter](../src/ingress/raw_wal_writer.cpp)
6. [Raw reader/recovery](../include/l2flow/ingress/raw_recovery.h)
7. [Raw replay engine](../src/ingress/raw_replay.cpp)

### 33.4 决策、验收和目标设计

1. [Production Instrument Runtime V1](decisions/production-instrument-runtime-v1.md)
2. [各阶段本地验收记录](acceptance/)
3. [总体路线图](design.md)

按照这一顺序，可以避免先读目标设计，再把尚未落地的组件误判为当前实现。

## 34. 最终归纳

这个仓库已经从单纯的 Shadow 接入演进出一条正式、fresh-only、可审计的 C++
live 路由：四路 Raw source 顺序在 instrument 拆分前完整保留，解码与 Canonical
commit 后由固定 worker 并行追加进程内 history，并通过带 owner 存活证明的 route
发布。

仍需严格区分三件事：

~~~text
已接通：Raw live → control/market → Canonical → instrument history → route
仍保留：four legacy ingress → ShadowCaptureWriter
尚未接：recovery/takeover/replay composition、Latest State、factor、Parquet/query
~~~

最准确的一句话是：

> `mdl-production-router` 已实现 fresh live 的四源 source-order/Canonical/
> instrument-history 正式组合，`L2Flow::production` 已切换到
> `l2flow_production`；Phase 6–8 派生服务、recovery 装配以及完整日、真实语料、
> 性能、crash/power-loss 和数学因子 external exit 仍未完成。五个命名 Python
> 因子仍是显式 passthrough placeholder。

## 35. 分析与验证边界

本文依据当前工作目录中的实际源码、CMake 和仓库内验收记录。

Phase 6–8 的本地命令与结果单独记录在对应 acceptance 文档；任何未执行的完整日、
目标机性能、真实进程 crash/power-loss、恢复装配或派生服务 wiring 均不写成通过。
