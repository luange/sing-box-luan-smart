# Smart

Smart 出站会根据真实连接观测，为每条新 flow 选择一个叶子出站。TCP 与 UDP
独立学习，并按网络指纹和目标站点隔离历史。

### 结构

```json
{
  "type": "smart",
  "tag": "smart-global",

  "outbounds": [
    "proxy-a",
    "proxy-b"
  ],
  "providers": [
    "provider-a"
  ],
  "exclude": "",
  "include": "",
  "use_all_providers": false,

  "url": "https://www.gstatic.com/generate_204",
  "probe_interval": "10m",
  "probe_timeout": "5s",

  "reach_tests": [
    {
      "tag": "gemini",
      "preset": "gemini",
      "interval": "30m",
      "timeout": "10s",
      "unmeasured_policy": "fallback"
    }
  ],

  "max_attempts": 3,
  "attempt_timeout": "4s",
  "site_stickiness": "10m",
  "switch_margin": 0.08,
  "exploration": 0.08,
  "min_samples": 3,

  "breaker_failures": 3,
  "breaker_cooldown": "2m",
  "half_life": "30m",

  "history_path": "smart-history-smart-global.json",
  "history_retention": "168h",
  "max_history_entries": 50000,

  "interrupt_exist_connections": false
}
```

### 选择模型

Smart 综合使用：

* 带置信度修正的连接可靠性
* 建连和首包延迟
* 延迟偏差
* 对重复出现大流量样本的站点使用持续吞吐
* 站点黏性和切换余量
* 有上限的探索
* 熔断状态

TCP 和 UDP 使用不同的统计键和权重，UDP 不继承 TCP 的健康结论。站点特有失败会
强烈更新该站点、轻微更新网络全局历史，但不会打开全局熔断。

Provider 节点和嵌套出站组会递归展开为不重复的叶子出站，循环引用会被忽略。

### 字段

#### outbounds

出站或出站组标签列表。

#### providers

[订阅](/zh/configuration/provider)标签列表。

#### exclude

排除 provider 节点的正则表达式。

#### include

包含 provider 节点的正则表达式。

#### use_all_providers

使用所有已配置 provider。默认值为 `false`。

#### url

低频主动探测地址。默认使用
`https://www.gstatic.com/generate_204`。

#### probe_interval

主动探测间隔。默认值为 `10m`。

#### probe_timeout

单次主动探测超时。默认值为 `5s`。

主动探测使用固定 worker 池。如果同一轮所有候选均失败，将抑制节点惩罚，因为
此时更可能是本地网络共同故障。

#### reach_tests

可选的应用可达性测试。每项测试只匹配它自己的目标域名；某节点被 Gemini 限制，
不会因此失去 ChatGPT、GitHub 或普通网站的使用资格。探测使用独立 HTTP 请求，
不会中间人解析或重放用户流量。

内置 preset 为 `gemini`、`chatgpt`、`claude` 和 `git-ssh`，显式字段可覆盖 preset。
每项支持：

* `tag`：唯一测试名；缺省使用 preset 名称。
* `preset`：可选内置默认值。
* `domains`：精确或后缀匹配的目标域名。
* `url`：HTTP 或 HTTPS 探测地址。
* `interval`：该项探测间隔；默认 `5m`。
* `timeout`：单候选超时；默认 `10s`。
* `accepted_status`：判定可达的 HTTP 状态码。
* `blocked_status`：判定服务受限的 HTTP 状态码。
* `request_headers`：只用于探测请求的标头。
* `reachable_body`：判定可达时必须出现的正文标记。
* `blocked_body`：表示服务受限的正文标记。
* `blocked_headers`：表示服务受限的响应标头标记。
* `unmeasured_policy`：`allow`、`fallback`、`suspend` 或 `reject`；默认
  `fallback`。
* `max_body_bytes`：正文检查上限；默认 128 KiB，最高 1 MiB。

候选状态为 `reachable`、`blocked`、`unreachable` 和 `unmeasured`。服务测试结果
不会打开传输层熔断。临时覆盖或永久固定会有意绕过服务排除。同一进程内所有 Smart
组共用最多八路 reach probe 并发。

首次启动、provider 内容变化和手动强制检查会测试全部候选。稳定运行时，每个
`interval` 优先检查当前候选、排名前八、尚未测量和已到刷新期限的候选；可达结果最迟
在三个 interval 后刷新，受限或不可达结果最迟在四个 interval 后重试。未到期结果会
合并保留，provider 已删除的候选会同时从 reach 状态中移除。

#### max_attempts

一条新 flow 建立时最多尝试的候选数。默认值为 `3`。连接建立后不会重放应用数据。

#### attempt_timeout

单个 TCP 或 UDP 建立尝试的最长时间。默认值为 `4s`。

#### site_stickiness

健康状态下同一站点保持当前出口的时间。默认值为 `10m`。

#### switch_margin

当前或站点黏性候选与最佳分数相差不超过此值时保持原出口。默认值为 `0.08`；
设为 `0` 可关闭余量。

#### exploration

低样本候选的受控探索强度。默认值为 `0.08`；设为 `0` 可关闭探索。

#### min_samples

候选结束预热所需的样本数。默认值为 `3`。

#### breaker_failures

站点或全局熔断打开前允许的连续失败次数。默认值为 `3`。

#### breaker_cooldown

首次熔断冷却时间。重复失败会在有上限的范围内指数增加冷却时间。默认值为
`2m`。

同一个候选、站点、网络和 transport 在 half-open 状态下，同一时间只允许一条
真实 flow 做恢复试流。

#### half_life

旧观测数据的衰减半衰期。默认值为 `30m`。

#### history_path

JSON 历史文件。默认位于工作目录：

```text
smart-history-<清理后的组标签>.json
```

网络和站点标识会先哈希再持久化。

#### history_retention

持久化观测的最长保留时间。默认值为 `168h`。

#### max_history_entries

持久化 metric 的最大条数。默认值为 `50000`。

#### interrupt_exist_connections

所选候选变化时中断外部入站连接。默认值为 `false`。需要保持旧连接时应保持关闭。

### Clash API

代理响应会包含 `smart` 对象：

```text
selected
pinned
network
site
reason
updated_at
candidate_count
candidate_details_count
candidate_details_truncated
state_counts
reach_tests
candidates（前 32 名）
```

每个候选包含状态、分数、可靠性、建连延迟、首包、吞吐、样本数和原因。

`candidate_count` 和 `state_counts` 覆盖完整叶节点集合。候选明细仍限制为 32 条，
`candidate_details_truncated` 会明确说明是否发生截断。

每项 reach-test 状态会给出匹配域名、最后检查时间、完整状态计数、候选总数和最多
32 条候选明细。请求标头不会通过状态 API 返回。

远程规则集将 Smart 设为 `download_detour` 时，如果要求冷启动阶段也能下载，
请在规则集上显式配置 `download_fallback_detour`。程序不会隐式绕过策略直连。

使用普通 proxy 更新接口可以固定候选；提交空名称可以清除固定。

Zashboard（已验证 v3.15.0）会把 Smart 显示为标准可选择策略组，不需要 fork 或注入
前端代码。点击候选会建立 30 分钟临时覆盖；选择 `♻️ 智能选择` 会清除临时覆盖与
永久固定并恢复自动排名。刷新页面后状态由 Clash API 重新读取，不依赖浏览器本地状态。

存在多个 Smart 组时，自动项会显示为全局唯一的
`♻️ 智能选择 · <组标签>`，避免 Zashboard 的 delay 请求误测到另一个组；只有一个
Smart 组时仍保留较短的 `♻️ 智能选择`。

API 除 `name` 外还支持 `temporary`、`ttl`、`persistent` 和 `reason`。TTL 限制为
60-86400 秒。状态会返回 `temporary_override`、`override_expires_at`、
`override_remaining_seconds` 和 `override_reason`。

### 无感重载

`SIGHUP` 会先构建并完整启动一个私有 runtime generation，成功后才发布。已有
TCP 和 UDP flow 继续使用旧 generation，新 flow 使用新 generation。

出站、provider、路由规则、rule-set 和 DNS runtime 配置可以热更。listener、
inbound、TUN、endpoint、service、NTP、证书、日志或 experimental API 发生变化
时会返回 `restart-required`，需要重启进程。
