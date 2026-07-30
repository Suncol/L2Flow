# L2Flow 内容泄漏审计报告

| 项目 | 值 |
|---|---|
| 审计日期 | 2026-07-28（Asia/Taipei） |
| 审计基线 | `adb9ce0f6526ee397121eec8f33218fada4ecaa0` |
| 分支 | `feature/live-latest-tick-snapshot-v1` |
| 建议处理级别 | 内部受限；报告未复述疑似 token 的实际值或行情数据行 |

## 1. 结论

本次审计不能给出“当前代码不存在内容泄漏”的结论。

已确认的主要问题是：

1. 本地可达 Git 历史和本地 `origin/main` 快照中存在一个含
   `Address`、`Service`、`token` 字段的供应商配置文件。三个 token
   值确实存在，但本报告不复述其值；它们是否为真实凭证、是否仍有效尚未验证。
2. 当前树直接跟踪 28 个供应商 SDK 头文件，其中至少 4 个文件明确标注
   “通联数据机密 / DataYes CONFIDENTIAL”并声明未经书面许可不得传播或复制。
   仓库外是否另有授权尚未验证。
3. 生产程序要求把部署所用 runtime credential token 放入
   `--user-name` 命令行参数。无害 canary 实验确认，本机同 UID 进程可以从
   `/proc/<pid>/cmdline` 读到完整参数。
4. 当前树跟踪两份行情抓取/对比 CSV，共 2,290,532 bytes；它们包含精确行情字段，
   对比报告还包含本机绝对路径。当前工作区的 Unix DAC mode 允许其他本机用户读取。
5. `--replace-wal` 打开已有文件时不会收紧原 inode 权限。动态实验确认已有
   `0644` 文件替换后仍为 `0644`，而代码随后会写入完整 vendor head 和 body。
6. Python 客户端包含 CWD 动态库候选。满足“前序候选失败且攻击者可写 CWD”
   的前提时，无害 DSO 构造器在符号校验前被执行。
7. 行情链路不比较 payload 的 exchange time 与接收时间。已经到达进程的
   未来、回退或重放时间戳可以污染 latest、K 线和因子。这是时间戳投毒/语义前视，
   不是系统凭空读到了尚未到达的未来行情。

同时，本次没有发现以下问题：

- 没有发现 post-cut ingress 混入旧 generation 的证据；
- 没有发现跨标的串线；
- 没有发现正常生产路径把前一交易日状态装入新 session；
- 没有发现 IPC 跨 UID 获取 memfd 的路径；
- 没有发现 IPC 未初始化 padding 或内存池旧 tail 被正常接口导出的路径；
- 对本地 22 个可达提交进行常见高置信密钥格式扫描，没有命中 PEM 私钥、
  AWS、GitHub、GitLab、Slack、OpenAI/Anthropic 格式 token、JWT 或带
  userinfo 的 URL。这个阴性结果不覆盖短 token，也不能证明历史中没有其他秘密。

## 2. 范围、方法和事实边界

### 2.1 审计范围

本次检查覆盖：

- 当前 `HEAD` 的 C++、Python、工具、测试、文档和已跟踪 artifacts；
- 初始工作树中已有的未跟踪文件
  `.tmp_instrument_full_read_probe.cpp`（只读扫描，未构建、未修改）；
- `git rev-list --all` 可达的 22 个本地提交及本地 remote-tracking refs；
- WAL、SDK、stderr/log、诊断输出、Git、构建目录和 Linux memfd/UDS IPC；
- generation、event time、跨标的、跨日、重放等金融数据语义泄漏；
- 正常 Debug、ASAN/UBSAN 及 TSAN 可执行性。

仓库中未识别到模型训练、验证或测试集切分管线，也未命中常见 ML 训练框架调用；
因此传统的 train/validation/test 污染不适用于这个实时行情 runtime。本报告中的
“数据泄漏”重点是 arrival/event-time 前视、跨标的/跨日混入和内容保密性。

没有执行：

- 真实凭证登录或有效性探测；
- 远端 GitHub 公私属性、ACL 或 Git LFS 远端对象可用性检查；
- 供应商授权、行情许可或法律结论；
- 对真实服务进行攻击性动态库替换；
- 对生产 SDK 日志内容进行真实连接测试。

### 2.2 术语

- **已确认暴露**：数据或危险传播机制在当前内容、当前文件权限或无害动态实验中
  已经得到直接证实。
- **条件性风险**：代码路径已证实，但利用需要尚未证实的前置条件。
- **未发现**：在本次范围和方法内未找到证据，不等于形式化证明不存在。
- **处置优先级 P0/P1/P2**：表示建议的响应顺序，不是 CVSS 分数，也不代表法律结论。

### 2.3 工作区状态

审计开始前 `git status --short` 只有：

```text
?? .tmp_instrument_full_read_probe.cpp
```

本次没有修改该文件。除本报告外，没有修改生产代码或测试。

## 3. 发现汇总

| ID | 优先级 | 证据状态 | 结论 |
|---|---:|---|---|
| CL-01 | P0 | 敏感字段存在已确认；有效性未验证 | Git 中有 3 个短 `token` 和 endpoint 配置 |
| CL-02 | P0 | 内容及声明已确认；授权未验证 | 当前/历史 Git 包含供应商明确标机密的 SDK 内容 |
| CL-03 | P0 | 已确认并动态复现 | 生产 runtime token 经 argv 传入 |
| CL-04 | P0/P1 | 内容、Git 跟踪和本机 DAC 已确认；许可未验证 | 行情抓取 CSV 和内部绝对路径可被仓库读者及本机其他用户读取 |
| CL-05 | P0 | 已确认并动态复现 | 替换已有 WAL 时保留 `0644/0664` 权限 |
| CL-06 | P1 | 条件路径已确认；无害构造器已复现 | Python CWD/可写构建目录动态库劫持 |
| CL-07 | P1 | 条件路径已确认 | 生产 SDK DSO 未做 identity/integrity 固定 |
| DL-01 | P1 | 数据流已确认 | future/rollback exchange time 可形成语义前视 |
| CL-08 | P1/P2 | 传播链已确认；真实 SDK 内容未验证 | opaque SDK error 和 SDK log 可能泄漏上下文 |
| CL-09 | P1 | 输出内容和创建方式已确认 | 抓取/对比/验收工具依赖 umask，且输出绝对路径 |
| DL-02 | P1/P2 | 条件路径已确认；供应商语义未验证 | 重连重放若发生，会重复累计成交/K 线 |
| OB-01 | 部署决策 | 设计边界已确认 | IPC 以同 UID 为完整信任边界，没有客户端/证券级 ACL |
| OB-02 | P2 | 内存残留已确认；正常导出路径未发现 | ingress pool 回收时不擦除 raw body |

## 4. 详细发现

### CL-01：历史 `Config.json` 含 credential-like token

优先级：P0；状态：文件和字段已确认，凭证真实性、用途及有效性未验证。

证据：

- 历史路径：`mdl_sdk_2_13_234/bin/Config.json`
- blob：`b1cb17cf96a097ae0094b3a83ddb928a53c67ca7`
- blob 大小：1,006 bytes
- JSON 顶层有 3 条记录；每条均含 `Address`、`Service`、`token`
- 三个 token 互不相同，均为 5 位 ASCII 字母数字串
- 仅凭长度和字符集不足以判断它们是真实凭证还是测试值
- 文件由提交 `61bb5762ad28c7d25f579b5dd427681d7f1baf53` 引入，
  当前分支在 `a21adff23f33bf3410e09a676677da345772b0f1` 删除
- 引入提交是当前 `HEAD` 的祖先，所以 blob 仍可从完整 Git 历史读取
- 本地 `refs/remotes/origin/main` 快照
  `506834bb6ac7ad71ca90a0e3dccf09bf472c5c3d` 的树仍直接包含该文件。
  这是本地最后一次获取的状态，不代表 2026-07-28 已联网刷新

本报告故意不记录三个 token 的实际值，也没有尝试连接任何 Address。

本机权限进一步扩大了暴露面：

- `.git/objects/b1/cb17...` 为 `0444`
- `.git` 为 `0755`，`.git/objects` 及其该级子目录为 `0775`
- 从 `/home` 到仓库路径均允许 Unix mode 的 `other` 遍历

因此，在没有额外 ACL/MAC 限制的标准 Unix DAC 解释下，其他本机账户可以读取
该 Git 对象。是否存在额外 ACL、SELinux/AppArmor 或主机级隔离未验证。

影响：

- 若 token 曾连接真实服务，应视为凭证事件；
- 即使 token 是测试值，endpoint/服务拓扑也已进入不可由普通删除清除的 Git 历史；
- 常见长 token 正则不会发现这类 5 字符语义 token。

建议：

1. 由凭证所有者在受控渠道核验用途；若曾用于真实环境，先轮换/吊销，再清历史。
2. 清理所有发布 refs 中的配置 blob，并协调镜像、fork、CI cache 和开发者 clone。
3. 增加 JSON 语义规则：字段名为 `token`、`secret`、`password`、`credential`
   时，不因值较短而跳过。
4. 用只含合成 endpoint 和显式占位符的模板替代供应商原始配置。

### CL-02：当前及历史 Git 含供应商明确标机密的 SDK 内容

优先级：P0；状态：内容和文件声明已确认，项目是否持有仓库外授权未验证。
若无书面再分发授权，应按最高优先级合规事件处理。

当前 `HEAD` 跟踪 28 个 `mdl_sdk_2_13_234/include/` 头文件。下列至少 4 个
文件包含明确保密声明：

- `mdl_sdk_2_13_234/include/base/base.h`
- `mdl_sdk_2_13_234/include/mdl_api.h`
- `mdl_sdk_2_13_234/include/mdl_api_msg.h`
- `mdl_sdk_2_13_234/include/mdl_api_types.h`

例如 `mdl_api.h:1-21` 标有“通联数据机密”“DataYes CONFIDENTIAL”，并声明
未经书面许可严禁传播或复制。仓库内未找到可证明再分发授权的 LICENSE/NOTICE；
但这不能证明不存在仓库外合同或书面许可。

本地可达历史共出现 56 个 SDK 路径，另外包括：

- `mdl_sdk_2_13_234/libs/win64/mdl_api.dll`：24,109,056 bytes，直接 Git blob
- 多份 `.lib`、`.a`
- `mdl_sdk_2_13_234.tar.gz`：Git LFS 对象在本机为 78,358,609 bytes
- Linux `libmdl_api.so`：Git LFS 对象在本机为 242,357,680 bytes
- demo、配置和第三方头文件

本地 `origin/main` 快照仍直接包含 `Config.json`、Windows DLL/库和 Linux SO
的 LFS pointer。远端仓库当前是否公开，以及远端 LFS 对象是否可下载，本次均未验证。

建议：

1. 立即让代码所有者、法务/采购和供应商确认仓库、开发机、CI、fork 的授权范围。
2. 若无明确授权，将 SDK 放入受权限控制的制品仓库；源码仓库仅保留最小适配接口。
3. 删除当前 tree 不足以清除历史；确认授权结论后处理所有 refs、LFS 对象和副本。
4. CI 阻止供应商包、二进制和带 `CONFIDENTIAL` 声明文件重新进入 Git。

### CL-03：生产 runtime token 经命令行传入

优先级：P0；状态：已确认，使用假 token 完成动态复现。

代码证据：

- `include/l2flow/runtime/realtime_pipeline_v1.h:35-37` 明确说明当前部署把
  runtime credential token 放在 vendor `user_name` 字段
- `apps/mdl_production_main.cpp:153-165` 把 `--user-name VALUE` 定义为必需参数
- `apps/mdl_production_main.cpp:334-396` 直接从 `argv` 解析
- `apps/mdl_production_main.cpp:509-515` 要求非空
- `apps/mdl_production_main.cpp:630-634` 传入 SDK 配置
- `README.md:432-452` 的生产示例也把 token 写在命令行

无害动态实验启动了带
`--user-name L2FLOW_AUDIT_FAKE_TOKEN_DO_NOT_USE` 的短生命周期进程；同 UID
读取 `/proc/<pid>/cmdline` 得到了完整 canary。没有使用真实凭证。

实际可见范围取决于 procfs `hidepid`、进程 UID、容器、审计和进程管理配置；
不能据此断言任意不同 UID 均能读取。但是同 UID 工具、进程监控、作业编排器及
交互式 shell history 均可能持久化 argv。

验收工具已有 `--user-name-file`：
`tools/accept_realtime_pipeline.cpp:581-619` 使用 `O_NOFOLLOW`、regular-file
和 4 KiB 限制，这是可复用的方向；但该实现还没有校验 owner、link count 或
group/other mode，不能原样当作完整安全实现。

建议：

1. 生产入口改用已打开 FD、systemd credential、受控 secret file 或密钥管理器。
2. 文件输入要求绝对路径、可信 owner、regular file、`st_nlink == 1`、
   mode 不宽于 `0600`，并用 `openat`/可信目录避免路径替换。
3. 用 canary 覆盖 argv、stderr、journal、SDK log、core dump 和诊断报告。
4. 擦除临时字符串可缩短内存驻留，但不能替代取消 argv 传参。

### CL-04：Git 跟踪行情抓取数据和内部绝对路径

优先级：P0/P1，取决于数据许可和主机信任模型；状态：内容、Git 跟踪、本机
mode 已确认，行情许可/保密等级未验证。

当前 `HEAD` 跟踪：

- `artifacts/feeder_capture_20260721_60s.csv`
  - 2,290,183 bytes
  - 10,969 行，即 1 行 header 和 10,968 行数据
  - 字段包含证券标识、vendor/应用序列、精确时间、价格、数量、成交量、
    成交额和盘口汇总
- `artifacts/feeder_capture_20260721_60s_comparison.csv`
  - 349 bytes
  - 1 行 header 和 2 行数据
  - 记录 6,864 与 4,104 行匹配结果，并包含 `/home/<user>/...` 绝对路径

两文件合计 2,290,532 bytes，Git mode 均为 `100644`，由提交
`ea558532e0b562cba0541e103b2120c9aca04775` 引入。

`.gitignore:3` 的 `/artifacts/` 只影响未跟踪文件，不会保护已经提交的文件。

当前主机上：

- `/home/sustech` 为 `0755`
- `code/L2Flow` 与 `artifacts` 为 `0775`
- 两个 CSV 均为 `0664`
- 当前 umask 是 `0002`

因此，在没有额外 ACL/MAC 限制时，标准 Unix DAC 的其他本机用户可以读取。
文件名、字段和对比结果支持“feeder 行情抓取数据”的判断；是否为受限实盘数据、
是否允许进入源码仓库，必须由数据所有者确认，本报告不作许可结论。

建议：

1. 立即确认行情许可、数据分类和允许的读者范围。
2. 若不允许提交，在处理所有 Git 历史/refs 后，用最小合成 fixture 替代。
3. 报告默认只保存逻辑 source ID 或 basename，绝对路径改为显式 opt-in。
4. 为源码仓库、artifacts、build 和 CI workspace 使用最小权限目录。
5. CI 同时检查“被 ignore 但已 tracked”的目录和 `/home/` 绝对路径。

### CL-05：`--replace-wal` 保留已有文件的宽松 mode

优先级：P0；状态：源码和无害动态实验均已确认。

代码证据：

- `include/l2flow/realtime/optional_wal_sink_v1.h:13-16` 说明 WAL 包含 immutable
  ingress metadata、23-byte vendor head、body 和 CRC32C
- `src/realtime/optional_wal_sink_v1.cpp:341-352` 使用
  `open(path, O_RDWR|O_CREAT|..., 0600)`
- 非 replacement 路径增加 `O_EXCL`，是有效的安全控制
- replacement 路径对已有 inode 打开后，只检查 regular file 并 `ftruncate`
  （`src/realtime/optional_wal_sink_v1.cpp:358-383`）
- 没有 `fchmod`、owner 或 link-count 校验
- `src/realtime/optional_wal_sink_v1.cpp:448-520` 写入完整 metadata、vendor head
  和 body

POSIX `open` 的 mode 参数只用于创建新文件，不会改变已有 inode 的权限。

动态复现：

1. 创建空 regular file 并设为 `0644`
2. 用 `replace_existing=true` 启动 WAL sink
3. sink 状态为 accepting，failure 为 0
4. 停止后文件仍为 `0644`

实验没有写真实行情；权限保持由实验直接证明，实际 payload 内容由序列化代码证明。
现有测试覆盖 disabled、缺失目录、FIFO 和正常 framing，但没有覆盖已有 `0644/0664`
replacement。

建议：

1. `fstat` 后、`ftruncate` 前要求 `st_uid == geteuid()`，评估
   `st_nlink == 1`，并强制 `fchmod(fd, 0600)`；也可对不满足 exact mode 的文件直接拒绝。
2. 验证所有父目录不是不可信用户可写，优先使用可信 dirfd + `openat`。
3. 新增 `0600/0640/0644/0664`、foreign owner、hard link、symlink、FIFO 矩阵测试。

### CL-06：Python 自动候选允许条件性动态库劫持

优先级：P1；状态：条件路径已确认，无害 DSO 构造器执行已动态复现。

代码证据：

- `python/l2flow_realtime/native.py:78-87` 的候选顺序是：
  environment、`find_library`、仓库 `build-live-latest`、当前工作目录
  `build-live-latest/libl2flow_shm_reader.so`
- `native.py:90-107` 在 `_bind_library` 符号验证前调用 `ctypes.CDLL`
- `python/l2flow_realtime/client.py:103-122` 先取得 read-only memfd
- `native.py:220-238` 在该 FD 仍打开时加载 DSO，并把 FD 交给 native reader

无害 probe 在满足候选条件时观察到 DSO constructor canary
`HIJACK_CONSTRUCTOR_EXECUTED`。这证明“符号校验失败即可阻止执行”是不成立的：
constructor 已先执行。

必须同时满足下列前提才构成 CWD 候选利用：

1. 调用方没有显式提供 `library_path`
2. environment、system 和仓库候选缺失或加载失败
3. 攻击者可以写当前目录的 `build-live-latest` 或替换其中目录项

当前仓库和 `build-live-latest` 目录为 `0775`。候选 `.so` 是 symlink，跟随后的
目标文件也是 group-writable；只有当同组中存在不可信主体时，这才形成实际篡改条件。
本次没有审计组成员信任关系。

建议：

1. 删除 CWD fallback；安装并引用包内受控 native library，或要求显式绝对路径。
2. 校验路径所有组件、owner、group/other write bits 和 digest/signature。
3. 失败信息不要把所有绝对候选路径原样暴露给低信任日志。
4. 保留无害 constructor fixture，修复后要求 hostile CWD 永远不会被自动加载。

### CL-07：生产 SDK DSO 完全依赖 operator/deployment trust

优先级：P1；状态：条件性供应链/部署风险，不是无条件远程漏洞。

- `src/sdk/direct_sdk_runtime_v1.cpp:438-465` 只检查路径非空/NUL，然后直接
  `dlopen(..., RTLD_NOW|RTLD_LOCAL)`
- 不要求绝对路径，也不检查 owner、mode、symlink、父目录或 digest
- `apps/mdl_production_main.cpp:193-196` 明确告诉操作者没有 SDK digest、
  baseline、archive、ELF、ABI 或 exact-byte approval
- `src/runtime/realtime_pipeline_v1.cpp:1901-1913` 把配置路径直接交给 loader
- 加载后 runtime 会设置 token、endpoint 并接收行情

因此，能控制启动参数、搜索语义、DSO 文件或父目录的低信任主体可以在生产进程中
执行代码并接触 token 和行情。若路径及所有父目录由不可变部署系统专有控制，这个
前提不成立；本报告不把它描述为远程漏洞。

建议使用可信绝对路径、secure dirfd、owner/mode 校验以及 digest/signature 或
sealed snapshot，并覆盖 symlink/相对路径/TOCTOU/错误 owner 的回归测试。

### DL-01：future/rollback exchange time 可污染 latest、K 线和因子

优先级：P1；状态：数据流和缺少约束已确认，不是“尚未到达数据泄漏”。

关键证据链：

- receive-date guard 只检查回调接收日是否为配置交易日：
  `src/runtime/realtime_pipeline_v1.cpp:1292-1333`
- decoder 接受当天 `00:00:00.000` 到 `23:59:59.999` 的合法 exchange time，
  不与 receive time 比较：
  `src/market/market_decoder.cpp:312-376,687-711`
- history 原样保存 event time：
  `src/market/realtime_history_v1.cpp:642-701`
- latest 按 ingress sequence 更新，不按 event time：
  `src/market/realtime_latest_read_model_v1.cpp:267-323`
- 默认因子检查 latest snapshot 的价格有效/为正，但没有 future-skew 或 event-time
  质量门：`src/factor/realtime_factor_engine_v1.cpp:182-229`
- K 线明确只使用 exchange time，不使用 receive clock：
  `include/l2flow/market/kline_aggregator_v1.h:12-18`
- latest K 线取最大 window start：
  `src/market/kline_aggregator_v1.cpp:460-481`

所以，如果进程在 09:30 已经收到一条 payload time 为 14:59、价格合法的消息：

- snapshot 可以立即成为 latest，并产生有效因子；
- trade 可以立即落入 14:59 K 线，并成为 latest bar；
- 较晚 ingress 携带较早 exchange time 时，latest/因子还可能语义回退。

这里的数据必须已经进入进程。代码没有从未到达的 queue、未来 generation 或外部
文件“偷看”数据，因此应称为**时间戳投毒、乱序/重放污染或语义前视**，不能称为
真正未来信息泄漏。

现有测试已经覆盖组成行为：

- 全天时间解码边界：`tests/test_phase4_market_decoder.cpp:2405-2520`
- receive time 不参与 K 线：`tests/test_realtime_history_v1.cpp:662-746`
- 迟到修订和最大窗口：`tests/test_kline_aggregator_v1.cpp:183-318`
- latest 只防 ingress 序号回退：
  `tests/test_realtime_latest_read_model_v1.cpp:467-484`

建议：

1. 增加可配置的最大 future skew、交易 session 窗口和 quarantine/quality flag。
2. 因子默认拒绝 invalid/future-time snapshot，除非调用方显式选择宽松策略。
3. 增加可注入 receive clock 的端到端测试 seam，确定性覆盖 future/rollback。
4. 把“arrival-prefix 完整”和“event-time 有效”作为两个独立健康指标。

### CL-08：opaque SDK error 与 SDK log 的条件性泄漏

优先级：P1/P2；状态：传播链已确认，真实 SDK 是否回显 token 未验证。

- `src/sdk/direct_sdk_runtime_v1.cpp:255-268` 原样复制 vendor `Connect()` 返回的
  最多 4,096 bytes
- `src/runtime/realtime_pipeline_v1.cpp:1948-1952` 拼接
  `"SDK Connect failed: " + connect_error`
- `src/runtime/realtime_pipeline_v1.cpp:1963-1969` 也原样拼接 `exception.what()`
- `apps/mdl_production_main.cpp:701-711` 把 detail 原样写入 stderr
- `src/runtime/realtime_pipeline_v1.cpp:1923-1924` 只把 log prefix 透传给闭源 SDK

如果 vendor error 回显 token、endpoint 或请求上下文，它们会进入终端、journal 或
CI 日志。但本次没有证据证明真实 SDK 必然回显。相反，
`docs/acceptance/cleaned-branch-20260724-20m.md:67-68` 明确记录那一次验收中
token 没有进入 SDK logs；这只是一次运行的反向证据，不是所有错误路径的保证。

建议正常日志只输出稳定错误类别；opaque detail 进入权限受控的 debug sink，并在
落盘前用已知 canary/token 做精确脱敏。SDK log 文件 mode 和内容需用失败登录、
断线重连、超长错误和异常路径单独验证。

### CL-09：抓取/对比/验收工具依赖 umask，并输出绝对路径

优先级：P1；状态：代码和当前生成物 mode 已确认。

- `tools/mdl_sdk_feeder_probe.cpp:891-912` 先 `exists` 后用 `std::ofstream`
  创建完整行情 CSV；没有强制 `0600`，且存在 check/create TOCTOU
- `tools/compare_feeder_capture.py:485-523` 把 capture 和 backup 的绝对路径
  放入 report
- `tools/compare_feeder_capture.py:526-560` 用 `Path.open("x")` 创建 JSON/CSV，
  mode 依赖 umask
- `tools/compare_feeder_capture.py:563-575` 把完整 report 输出到 stdout
- `tools/accept_realtime_pipeline.cpp:1733-1771,2106-2117` 也用流创建
  CSV/JSON 临时文件
- `tools/accept_realtime_pipeline.cpp:2143-2163` 的报告包含 SDK library path、
  server address 和 registry identity

当前 umask 为 `0002`，现有 artifacts 为 `0664`，说明当前部署默认值不足以保护
敏感抓取。报告进入 CI artifact 或日志后还会扩大路径和拓扑暴露。

建议使用 `openat(O_CREAT|O_EXCL|O_NOFOLLOW, 0600)` 后包装流；敏感目录 `0700`；
stdout 默认输出摘要而非完整绝对路径；full report 需要显式 opt-in。

### DL-02：若 SDK 重连重放同日消息，当前链路不会去重

优先级：P1/P2；状态：本地不去重已确认，供应商 SequenceID 语义和真实重放行为
未验证。

- 每个 callback 都获得新的本地 global/source sequence：
  `src/runtime/realtime_pipeline_v1.cpp:1336-1380`
- vendor `SequenceID` 被复制到 origin/wire，但未参与 admission 去重：
  `src/runtime/realtime_pipeline_v1.cpp:2017-2036`
- K 线对每条 trade 无条件累加 volume/trade count：
  `src/market/kline_aggregator_v1.cpp:778-816`
- 测试 fake 多条消息复用同一个 `SequenceID=9988` 仍会被接收：
  `tests/test_realtime_pipeline_v1.cpp:255-274,624-650`

因此，如果 SDK 在重连时重送同一成交，当前代码会重复累计。是否会发生必须先依据
供应商协议确认 SequenceID 的作用域、单调性和重连重置规则；不能把它写成已经观察
到的生产故障。

建议基于确认后的供应商语义设计 `(source, epoch, sequence)` 或交易所业务序列的
幂等策略，并增加单包重复、重连 epoch、sequence wrap/reset 和 gap 测试。

## 5. 已核实的安全控制与未发现项

### 5.1 Generation 没有发现 future-ingress 越界

generation cut 在 admission mutex 下捕获水位并把 marker 写入所有 source queue；
marker 全部进入前不解锁：
`src/runtime/realtime_pipeline_v1.cpp:1492-1611`。

history worker 按 source fence 冻结一致代际：
`src/market/realtime_history_v1.cpp:1314-1371`。
因子发布前后检查 exact current healthy store 并原子替换整代：
`src/factor/realtime_factor_engine_v1.cpp:366-456`。

本次没有找到 post-cut callback 越过 marker、进入旧 generation 的路径。
这只证明本进程 arrival prefix，不证明 event-time 顺序或上游完整性。

### 5.2 没有发现跨标的串线

decoder 用 market/source/security ID 精确查 registry，history/store/latest/factor
又分别校验 instrument、ordinal、worker、session epoch 和 universe 顺序。主要证据：

- `src/market/market_decoder.cpp:714-743`
- `src/market/realtime_history_v1.cpp:642-701,1045-1071`
- `src/market/intraday_instrument_store_v1.cpp:1515-1616`
- `src/market/realtime_latest_read_model_v1.cpp:267-287`
- `src/factor/realtime_factor_engine_v1.cpp:85-134`

现有测试覆盖 ordinal/instrument mismatch 和固定 universe。本次未找到 A 标的数据
落入 B 标的槽位的路径。

### 5.3 正常生产跨日隔离良好

- 启动要求 `--trade-date` 等于当前 UTC+8 日期：
  `apps/mdl_production_main.cpp:545-558`
- 生产启用 receive-date guard：
  `apps/mdl_production_main.cpp:599-625`
- 主循环最多约 100 ms 检查换日并退出：
  `apps/mdl_production_main.cpp:91-115,760-777`
- 新 IPC session 创建新 memfd、整体清零，并写入新的
  run ID/session epoch/trade date：
  `src/ipc/realtime_shared_service_v1.cpp:474-500,879-895`

已持有旧 memfd 的客户端在 `STOPPED_CLEAN` 后仍可读最终旧日映射，这是显式的
最终前缀保留设计，不是新 session 混入旧状态。消费者仍应核对
trade date/run ID/session epoch；建议 Python API 增加 `expected_trade_date`。

### 5.4 IPC 没有发现跨 UID 或未初始化内容泄漏

已确认的护栏：

- control socket 父目录要求 service euid 所有且无 group/other 权限
- socket bind 后强制 `0600`
- `SO_PEERCRED` 要求客户端 effective UID 与服务相同
- 发送给客户端的是 read-only、CLOEXEC 的 memfd
- memfd 有 grow/shrink/future-write/seal seals
- mapping 创建后整体 `memset` 为零
- wire projection 使用值初始化，reader 校验 reserved 字段为零

主要证据：

- `src/ipc/realtime_shared_service_v1.cpp:474-510,1011-1093,1365-1440`
- `src/ipc/realtime_shm_reader_c_v1.cpp:371-396,478-554`
- `src/ipc/realtime_wire_projection_v1.cpp:178-377`
- `tests/test_realtime_ipc_v1.cpp:1131-1218`

剩余信任边界：任何同 UID 进程都可以请求完整 registry/universe/行情映射，没有
客户端或证券级 ACL。若同 UID 仅用于专用可信服务，这是设计选择；若多个不互信租户
共享 UID，则需要新的授权层。

### 5.5 内存池残留是防御缺口，不是已发现正常导出

`src/realtime/owned_ingress_message_v1.cpp:223-273,395-413` 在回收 block 时不擦除
raw vendor body，free-list 只覆盖少量 allocator metadata。仓库中未找到
`MADV_DONTDUMP` 或显式 secure erase。

但是：

- `OwnedIngressMessageV1::body()` 只返回当前 `body_size`
- WAL 只写当前 body span
- IPC 投影从值初始化的结构构建

本次没有找到旧 tail 经正常 WAL/IPC/API 导出的路径。残留会扩大 core dump、UAF 或
其他内存披露缺陷的影响面，适合按 P2 处理；高吞吐场景应评估擦除成本和
`MADV_DONTDUMP`。

### 5.6 新建 WAL 的默认路径是安全控制

缺陷只在显式 replacement 路径。默认新建使用 `O_EXCL`、`O_NOFOLLOW` 和 `0600`；
已有路径会被拒绝，FIFO 也会在截断前被 regular-file 检查拒绝。

## 6. 动态测试结果

### 6.1 全新 Debug 构建

使用全新 `/tmp` 构建目录：

```bash
cmake -S . -B <tmp-debug> \
  -DCMAKE_BUILD_TYPE=Debug \
  -DL2FLOW_BUILD_TESTS=ON \
  -DL2MOCK_BUILD_TESTS=OFF \
  -DL2FLOW_ENABLE_ASAN_UBSAN=OFF \
  -DL2FLOW_ENABLE_TSAN=OFF \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build <tmp-debug> -j4
ctest --test-dir <tmp-debug> --output-on-failure
```

结果：

- 沙箱内 17/18 通过
- 唯一失败是 IPC 测试的 Unix-domain socket `bind` 返回 `EPERM`
- 在允许 socket 操作的同一构建上单独重跑 IPC：1/1 通过
- 有效功能结果：18/18 通过
- 包含 decoder deterministic fuzz smoke 50,150 cases 和 Python 测试

这个结果说明现有功能没有因审计而回归，但现有测试并不覆盖本报告的 argv canary、
WAL permissive-mode、hostile CWD 和 opaque-error canary。

### 6.2 ASAN/UBSAN

全新 Debug 构建启用 AddressSanitizer 和 UndefinedBehaviorSanitizer。

- `ASAN_OPTIONS=detect_leaks=0` 下，按同样的 socket 环境拆分后功能合计 18/18 通过
- `detect_leaks=1` 下，16 个非 IPC 的纯 C++ 测试通过；另一个不启用
  Python e2e 宏的 IPC binary 手工运行通过
- 含 Python 的 IPC 测试在进程退出时报告 CPython 分配残留；不能据此归因
  L2Flow，也不能把该组合记为完整 LSan clean

ASAN/UBSAN 通过不能证明不存在内容泄漏；它只提供内存越界/未定义行为方面的辅助证据。

### 6.3 TSAN

标准 PIE TSAN 构建成功，但 17 个 C++ 测试在进入 `main` 前出现
`ThreadSanitizer: unexpected memory mapping` 或段错；仅不加载 C++ TSAN runtime
的 Python 测试通过。沙箱外结果相同。

non-PIE/ASLR 诊断组合仍产生与互斥锁保护路径冲突的报告，环境和分析器结果不可靠。
因此本报告既不声称“TSAN clean”，也不把这些输出定性为真实竞态。

### 6.4 定向泄漏 probe

| Probe | 输入 | 结果 |
|---|---|---|
| argv canary | 假 token | 同 UID `/proc/<pid>/cmdline` 可见完整 canary |
| WAL mode | 已有 `0644` 空文件，replacement | accepting，无 failure，结束后仍 `0644` |
| Python DSO | 临时 CWD 无害 constructor fixture | 满足候选前提时 constructor 在符号校验前执行 |
| Python candidate list | 从 `/tmp` 运行包 | 候选列表确实包含 `/tmp/build-live-latest/...` |

所有 probe 都使用合成值，没有连接真实 SDK 服务，也没有把真实 token 写入日志。

## 7. 静态扫描结果和限制

### 7.1 常见密钥签名

对本地可达的 22 个提交逐一进行 `git grep -I -E`，覆盖：

- PEM private key header
- AWS access key ID
- GitHub/GitLab token
- Slack token
- OpenAI/Anthropic 常见 key 前缀
- JWT-like 三段 token
- URL userinfo

结果：0 个匹配提交、0 条匹配。

这个结果不能覆盖：

- 当前规则未知的新格式
- 短 token
- 加密、压缩或二进制中的秘密
- 语义上敏感但不符合格式的 endpoint、用户路径和行情数据

历史 `Config.json` 的三个短 token 正是通过对象枚举和字段语义检查发现，而不是
常见 token 正则命中。

### 7.2 工具限制

环境未安装 `gitleaks`、`trufflehog`、`semgrep`、`cppcheck`、`clang-tidy`、
`valgrind` 或 `scan-build`。本次使用 Git object 枚举、定向正则、人工数据流审计、
编译器 sanitizer 和无害动态 probe 组合替代。

### 7.3 外部状态限制

以下均未联网验证，因此不能作为事实宣称：

- GitHub 仓库是公开还是私有
- 当前远端 refs 是否仍等于本地 remote-tracking refs
- 远端 Git LFS 大对象是否可下载
- 历史 token 是否有效
- SDK/行情是否已有再分发授权
- 真实 SDK 在全部失败路径中是否记录或回显 token

## 8. 修复顺序

### 立即处理（P0）

1. 核验历史 `Config.json` 的三个 token；若曾真实使用，先轮换/吊销。
2. 核验供应商 SDK 再分发授权和行情数据许可。
3. 停止生产 token 的 argv 传递，改为受控 FD/credential file/secret manager。
4. 修复 WAL replacement：owner/mode/link 校验，`fchmod(0600)` 后才能 truncate/write。
5. 在确认许可前限制仓库、`.git`、artifacts、build 和 CI workspace 的本机访问。

### 近期处理（P1）

1. 从所有需要处理的 refs 和 LFS 历史清除 token、供应商包和未授权 capture。
2. 删除 Python CWD fallback，固定 native library identity。
3. 固定生产 SDK DSO 的可信路径和 digest/signature。
4. 给 event time 增加 future-skew/session/quality gate。
5. 让抓取、对比、验收输出固定 `0600`，去除默认绝对路径。
6. 用 canary 测试 opaque SDK error、vendor log、stderr 和 journal。
7. 与供应商确认 SequenceID/reconnect 语义后实现幂等或明确告警。

### 防御加固（P2）

1. 明确同 UID IPC 是否符合部署租户模型；不符合则增加显式客户端授权。
2. Python client 支持 `expected_trade_date`，防止消费者误持旧 session。
3. 评估 ingress arena 擦除和 `MADV_DONTDUMP`。
4. 在 CI 加入 Git 历史 DLP、tracked-ignore、权限矩阵、hostile path 和时间投毒测试。

## 9. 建议新增的确定性回归

1. **argv canary**：修复后 `/proc/<pid>/cmdline`、stderr、journal 和 SDK log
   均不得包含 canary。
2. **WAL mode matrix**：预建 `0600/0640/0644/0664`；成功路径最终必须为
   `0600`，错误 owner/hard link/不安全父目录必须拒绝。
3. **Git DLP gate**：同时扫描 `git ls-files` 与 `git rev-list --objects --all`，
   阻断 credential-key JSON、供应商二进制、未批准的 confidential header、
   capture 和绝对 `/home/` 路径。
4. **Python hostile CWD**：临时 CWD 放同名 constructor fixture；自动发现不得加载。
5. **SDK trust**：relative path、symlink、group/world-writable、错误 owner 和
   digest mismatch 均应拒绝。
6. **future/rollback time**：可注入 receive clock，覆盖未来 snapshot、未来 trade、
   更大 ingress 的旧 event time 和 invalid-time 因子隔离。
7. **重放幂等**：同一业务消息重复、重连 epoch、SequenceID reset/wrap/gap。
8. **opaque error canary**：Fake `Connect()` 返回含 canary 的 error；所有普通
   输出必须脱敏。
9. **输出权限**：在 umask `0002` 下运行 feeder/compare/accept，文件仍必须为
   `0600`，stdout 默认不得含绝对路径。
10. **IPC 身份矩阵**：同 UID 专用客户端成功、不同 UID 拒绝；如共享 UID，
    验证新增授权策略。
11. **旧 session**：旧日 `STOPPED_CLEAN` fd 与新日 session 并存时，
    `expected_trade_date` 应 fail closed。
12. **内存残留**：测试 hook 回收带 canary 的 body，按选定的安全策略验证复用 block。

## 10. 最终判断

当前代码的 generation、跨标的、跨日新 session 和 IPC 初始化/跨 UID 控制总体上有
较强的 fail-closed 设计；本次没有发现真正的 future-ingress 跨代泄漏或跨标的串线。

但仓库内容、凭证传递、WAL replacement 和动态库信任边界存在足以阻止“无泄漏”
结论的证据。最紧急的不是调整算法，而是：

1. 核验并可能轮换历史 token；
2. 核验供应商 SDK 与行情数据的授权范围；
3. 移除 argv token；
4. 修复 WAL 权限继承；
5. 收紧仓库和产物权限。

在这些事项完成并加入对应回归前，不应把当前版本标记为“内容泄漏审计通过”。
