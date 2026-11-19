# 公共模型网关改造方案（定稿 v1.0）

> 基于 Qoder-2API-Go v0.6.3（commit 1f8b4e2）只读源码评审后制定。
> 决策来源：2026-11-19 问卷确认。
> 本文档为设计与实施计划，不含已实施的代码变更。

## 0. 已确认决策

| 决策项 | 结论 |
|:--|:--|
| 定位 | **仅自用**：聚合多家供应商，给自己和自己的项目调用（无租户/计费） |
| 首批供应商 | DeepSeek、硅基流动、Moonshot Kimi、阿里云百炼、智谱 GLM、OpenAI、OpenRouter、Anthropic Claude、Google Gemini（9 家） |
| 网络 | 仅内网（局域网），不暴露公网 |
| 部署 | 单实例（单个容器） |
| 租户隔离 | 不需要（随"仅自用"自动豁免） |
| 管理界面 | 中文，沿用现有面板风格扩展 |
| 节奏 | 一次到位，按三阶段稳步实施 |
| 改造方式 | **直接在 `deploy/qoder2api/src` 上 fork 修改**，以后自己维护（不再追上游） |
| 平台模型命名 | **业务名为主 + 保留上游原名别名**（两者都支持） |
| Claude / Gemini | **推迟到阶段二接入**（MVP 不含这两家，含兼容层也不做） |
| 实例范围 | **独立新容器 `model-gateway`（:2325）+ 独立数据卷 `model-gateway-data`**；现有 qoder2api（:2323/:2324）容器/镜像/卷一律不动 |
| 验证方式 | 允许用真实供应商 Key 做小额真实请求验收 |
| 密钥现状 | Qoder 上游 PAT 已在数据卷（多账号池），直接复用；新增供应商 Key 接入时经面板 `Provider.Auth` 配置 |

## 1. 可行性结论

**有条件可行。**

现有代码已具备：多账号负载均衡（sticky / round-robin / least-inflight + 平滑加权轮询）、失败冷却摘除、跨账号故障转移、会话粘性、区域过滤、阈值松弛、饱和排队、被动额度扣减 + 主动探测。核心资产 `routing.go` 与上游无关（回调注入），可泛化复用。

核心缺口：**没有供应商抽象**（全库仅 logs/stats 两处 `Persister` 接口），协议转换层（transform.go 1026 行）与会话层（session.go 538 行）和 Qoder 私有协议深度耦合。

改造本质：把"Qoder 账号池"泛化为"供应商成员池"，把"模型名透传"升级为"平台模型 → 成员模型映射"，并新增供应商适配层。路由引擎、健康模型、故障转移、管理面板骨架、测试安全网（1.16 万行）全部复用。

## 2. 供应商适配分组

| 组 | 供应商 | 接入方式 | 阶段 |
|:--|:--|:--|:--|
| A：原生 OpenAI-compatible | DeepSeek、硅基流动、Moonshot、智谱 GLM、OpenAI、OpenRouter | 一个 `openai-compat` adapter 通吃（BaseURL + Bearer，SSE 直通） | 阶段一 |
| A'：OpenAI 兼容模式 | 阿里云百炼 DashScope | 同上（`compatible-mode/v1` 端点） | 阶段一 |
| B：非 OpenAI 协议 | Anthropic Claude、Google Gemini | 原生协议适配器（`anthropic` / `gemini`），**推迟到阶段二**，MVP 不含 | 阶段二 |
| C：现有链路 | Qoder 网关 | 现有 transform + session + bridge 整体封装为 `qoder` adapter，对外行为不变 | 阶段一 |

## 3. 目标架构

```
客户端(OpenAI SDK) → 公共网关层(/v1/chat/completions, 鉴权/限流/平台模型解析/统一错误/日志)
                   → 负载均衡引擎(复用 routing.go: 策略/加权/粘性/冷却/排除重试/熔断)
                   → Provider Adapter(openai-compat | qoder | anthropic | gemini)
                   → 各上游
```

- 网关层：对外协议不变（OpenAI 格式），`model` 字段 = 平台模型名。
- 路由引擎：路由对象由 `store.Account` 泛化为 `Target`（成员 = 供应商模型实例），策略/加权/粘性/冷却/排除重试算法原样保留（routing.go:297/:345/:409/:445）。
- 适配层接口：
  - `Probe(ctx) (Health, error)` — 健康探测（替代 Qoder 专属配额检查）
  - `ChatStream(ctx, Req) (Stream, error)` — 统一入口，返回规范化 SSE
  - `Models(ctx) ([]ModelInfo, error)` — 上游目录
  - `Capabilities() Caps` — tools/vision/reasoning 支持声明
- 配置：延续"内存态 + 30s 落盘 + 面板写路径即时生效"模式；配置本体仍是单 JSON（小 blob、读路径全在内存，SQLite 化无收益），字段全部 `omitempty` 向后兼容。
- 请求日志与每日统计：**已迁移 SQLite**（2026-11-19，pure-Go 驱动 modernc.org/sqlite，WAL + synchronous=NORMAL，零 CGO）：
  - `request_logs` 表（ts 索引）承载请求日志，实时写入，日志留 30 天、统计留 90 天，每小时自动清理；
  - `daily_stats` 表（day+provider_id+model 主键，UPSERT 累加）承载用量统计，随请求实时更新（原先在配置 JSON 内、随 30s 落盘）；
  - 启动时自动一次性迁移旧 `requests.jsonl`（改名 `.imported`）与配置内 `daily_stats`，迁移后从配置中移除；
  - `/admin/api/stats` 支持 `?from=&to=`（闭区间）/`?range=all`，缺省近 7 天；面板统计页提供 今天/近7天/近30天/全部 + 自定义起止筛选。

## 4. 数据模型（草案）

```go
type Provider struct {
    ID      string     `json:"id"`       // p_deepseek / p_qoder_cn ...
    Name    string     `json:"name"`
    Kind    string     `json:"kind"`     // openai-compat | qoder | anthropic | gemini
    BaseURL string     `json:"base_url"`
    Auth    AuthConfig `json:"auth"`     // {Type: bearer|api-key|qoder-session, Key, Extra}
    Region  string     `json:"region,omitempty"`
    Enabled bool       `json:"enabled"`
}

type ProviderModel struct {
    ID         string   `json:"id"`   // pm_deepseek_v3
    ProviderID string   `json:"provider_id"`
    Model      string   `json:"model"` // 上游真实模型名
    Caps       []string `json:"caps"`  // chat/tools/vision/reasoning
    Enabled    bool     `json:"enabled"`
}

type PlatformModel struct {
    ID          string `json:"id"`   // mplat_fast
    Name        string `json:"name"` // 对外 model 名
    Enabled     bool   `json:"enabled"`
    Strategy    string `json:"strategy"`  // sticky|round-robin|least-inflight
    StickyLimit int    `json:"sticky_limit,omitempty"`
    MaxRetry    int    `json:"max_retry_members,omitempty"`
}

type PlatformModelMember struct {
    ProviderModelID string `json:"provider_model_id"`
    Weight          int    `json:"weight"`   // 1..100 平滑加权轮询
    Priority        int    `json:"priority"` // 越小越优先（故障转移顺序）
    Enabled         bool   `json:"enabled"`
}
```

## 5. 管理 API（草案）

```
GET/POST/DELETE      /admin/api/providers
GET/PUT/DELETE       /admin/api/providers/{id}
GET/POST             /admin/api/providers/{id}/keys        # 密钥池：列表(含运行态)/新增
PUT/DELETE           /admin/api/providers/{id}/keys/{key}  # 密钥池：更新/删除
GET/POST/DELETE      /admin/api/provider-models
GET/POST/PUT/DELETE  /admin/api/platform-models
POST                 /admin/api/platform-models/{id}/members
POST                 /admin/api/platform-models/{id}/probe
GET                  /admin/api/platform-models/{id}/stats
```

推理 API 对外不变：`POST /v1/chat/completions`（`model` = 平台模型名）；`GET /v1/models` 返回平台模型目录。

### 5.1 模型自动发现与勾选导入（2026-11-19 新增需求）

需求：配置供应商时**不手工填写模型名**，由网关实时拉取该供应商当前可用的模型列表，管理员勾选后自动创建供应商模型，再挂载到平台模型成员。

**交互流程**

```
面板「供应商」页 → 填/选好 BaseURL + 密钥
  → 点「获取模型列表」→ POST /admin/api/providers/{id}/models/remote
  → 面板展示候选清单（复选框，含上游原始 model 名、建议能力标注、已导入标记）
  → 勾选 → POST /admin/api/providers/{id}/models/import
  → 批量创建 ProviderModel（按 provider_id+model 去重，已存在则跳过/更新）
  → 之后在「平台模型」编辑页把这些供应商模型勾选为成员（权重/优先级/启用）
```

**管理 API（草案，新增）**

```
GET  /admin/api/providers/{id}/models/remote    # 实时拉上游目录，返回候选（不落库）
POST /admin/api/providers/{id}/models/import    # body: {"models":["gpt-4o",...]} 批量导入
GET  /admin/api/providers/{id}/models           # 已导入的供应商模型
```

**各协议的发现方式**

| 协议 | 拉取方式 | 说明 |
|:--|:--|:--|
| openai-compat（A/A' 组 7 家） | `GET {base_url}/models`，Bearer 鉴权，取 `data[].id` | 标准行为一致 |
| qoder（C 组） | 复用现有 `auth.FetchModelCatalog`（auth.go:108） | 已实现 |
| anthropic | `GET {base}/v1/models`，`x-api-key` + `anthropic-version` 头 | 阶段二原生 adapter |
| gemini | `GET {base}/v1beta/models?key=...`，过滤 `supportedGenerationMethods` 含 `generateContent` | 阶段二原生 adapter |

**设计要点**

1. 适配器接口已预留 `Models(ctx) ([]ModelInfo, error)`，自动发现就是该方法的第一个消费方，不需新增抽象；
2. **能力标注用启发式**：OpenAI 风格 `/models` 不返回能力信息，按模型名关键词推断（含 `embed|embedding|whisper|tts|dall-e|moderation|rerank` 的标记为非对话模型，默认不勾选；`o1/o3|reasoning` 标记 reasoning；`4o|vision|vl|multimodal` 标记 vision），最终以人工勾选为准；
3. **导入即去重**：同一 `provider_id + model` 已存在时不重复建，仅回填能力标注；
4. **失败可见**：上游 401/403/超时原样返回给面板（如 "Billing daily count exceeded"），不静默吞掉；
5. **同步策略**：配置时手动拉取为主；预留 `auto_sync` 开关（每日拉取一次、仅新增不删除），防止上游目录变化（参考 QoDuck 2026-11-08 才出现、且出现过 `Efficient/Ultimate` 异常条目的抖动）导致平台模型成员被误删；
6. 删除保护：上游目录里消失的模型**不会**自动禁用已导入的供应商模型（避免上游抖动打断在途会话），仅在面板标注"上游已不可见"。

### 5.2 供应商密钥池：同平台多 Key 负载均衡（2026-11-19 新增需求）

需求：同一供应商（平台）下配置**多把 API Key**，网关按 key 做负载均衡；某把 key 失效/限流时自动摘除并轮换，不影响业务请求。

**数据模型**

```go
type ProviderKey struct {
    Key       string `json:"key"`
    Note      string `json:"note,omitempty"`
    Weight    int    `json:"weight,omitempty"`   // 分流权重，默认 1
    Priority  int    `json:"priority,omitempty"` // 备用序，小者优先
    CreatedAt int64  `json:"created_at,omitempty"`
    Disabled  bool   `json:"disabled,omitempty"`
}
// Provider 增加 Keys []ProviderKey；旧 api_key 字段 Normalize 时自动并入 Keys（向后兼容）
```

**路由语义：两级负载均衡**

```
客户端请求 → 平台模型（成员级：sticky/WRR/least-inflight，沿用现有路由）
           → 命中某供应商模型 → 密钥池（key 级：按权重平滑轮询）
           → 单 key 失败(401/402/403/408/429/5xx/网络) → 冷却摘除 → 轮换下一把（最多 5 把）
           → 全部 key 失败 → 该成员判失败 → 交回成员级故障转移（换下一个成员）
```

**设计要点**

1. key 级路由复用同一套 Router（Candidate.Key = `providerID|k|<key>`），冷却/权重/松弛逻辑与成员级完全一致；
2. **非 key 级 4xx（400/404 等请求本身有误）不轮换 key**，直接写回客户端——换 key 也不会成功，避免无谓放大；
3. 单 key 冷却时间沿用 Settings.CooldownBaseSec/MaxSec 指数退避；401/403 等长期失效 key 会被持续摘除，恢复后自动回归；
4. 日志记录每次尝试实际使用的上游密钥掩码（`upstream_key`），便于定位是哪把 key 出问题；
5. 探测（probe）改为逐 key 探测，能直接发现失效 key；
6. 面板：供应商弹窗支持"每行一个 key"批量录入；每个供应商有独立「密钥池」管理弹窗（启停/备注/权重/删除/冷却状态展示）。

**已验证**（2026-11-19）：17 项单测全绿（新增 key 轮换冷却、双 key 均分、密钥管理 API 3 项）；真实环境向 Qoder 供应商注入假 key 后，第 2 笔请求命中假 key → 401 → 自动轮换真 key 成功，日志可见 `sk-bogus...0000 | 401` 失败记录与冷却倒计时，随后删除假 key 恢复单 key。

## 6. 负载均衡设计

复用现有实现（routing.go），三处泛化：

1. 路由对象 `Account` → `Target`；`routerDeps` 回调签名同步调整。
2. `routeKey` 由 `region|model` 改为 `platformModelID|conversationSeed`。
3. `usable()`（routing.go:111）中 Qoder 专属配额判断改为调用 adapter `Health()`；保留阈值松弛、饱和排队、冷却摘除三个通用行为。

新增熔断/降级（现有未实现）：

- 每成员滑动窗口失败率；连续 N 次失败或失败率超阈值 → 打开 30s；
- 半开：到期放 1 个探测请求，成功闭合；
- 全部熔断时按 Priority 取最优成员放行（可用性优先）。

故障转移顺序：同 Priority 内按权重 WRR；跨 Priority 严格从低到高。重试预算沿用 `MaxRetryAccounts` → 更名 `MaxRetryMembers`。

## 7. 实施路线图

### 阶段一 MVP（约 2–3 人周）

范围：数据模型 + openai-compat adapter（覆盖 A/A' 组 7 家）+ qoder 链路封装 + 路由泛化 + 平台模型解析 + 自动迁移 + 最小管理 API（含供应商模型自动发现/导入接口，见 §5.1）。

- 新增 `provider/` 包：`Adapter` 接口 + `openai` 实现；
- 现有 transform/session/bridge 收敛为 `provider/qoder`；
- `store.Config` 追加 Provider/ProviderModel/PlatformModel（`omitempty`）；
- 启动自动迁移：无 Provider 时，用现有 Accounts 合成 `qoder` Provider，上游模型逐个建同名平台模型（单成员、权重 1、Priority 0）；
- B 组（Claude/Gemini）先用官方兼容层接入。

风险：Qoder 封装回归（签名对字段顺序敏感，transform.go:116）。
验收：现有测试全绿；DeepSeek + Qoder 双供应商平台模型按权重分流；旧调用方无感。

### 阶段二 增强（约 2–3 人周）

范围：主动健康探测泛化 + 熔断 + Claude/Gemini 原生 adapter + 中文管理面板（供应商/模型/平台模型/成员管理，含「获取模型列表 → 勾选导入」交互，见 §5.1）+ 按成员统计。

风险：熔断参数误配抖动（半开 + 保守阈值缓解）。
验收：kill 一个 mock 供应商，流量在 MaxRetry 内切换且客户端无感；恢复自动回归；面板可见各成员健康态与用量。

### 阶段三 生产化（约 3–4 人周）

范围（按"仅自用"裁剪后）：

- Prometheus `/metrics`（QPS/延迟/错误率/credits，按 provider/model 维度）；
- 审计日志独立留存（不随环形队列滚动覆盖）；
- 配置版本化 + 一键回滚；
- 每密钥配额（复用现有 API Keys，不做租户）；
- 供应商密钥加密存储；
- 压测与运维文档。

验收：指标可见；密钥不再明文；配置回滚演练通过。

## 8. 兼容与迁移

- 对外协议不变；`/v1/models` 语义从"上游目录并集"变为"平台模型目录"，但迁移时一一映射同名，对外集合不变。
- 新旧二进制读写同一 `data.json`，新字段旧版本忽略，可随时换回旧镜像回滚（补丁 tar 已备份）。
- 调用方零改动：端点、密钥、模型名、协议均不变。

## 9. 风险登记

| 风险 | 缓解 |
|:--|:--|
| Qoder 协议封装回归（字段顺序敏感） | 以现有 1.16 万行测试为护栏，封装时先建 characterization 测试 |
| Claude/Gemini 兼容层能力受限 | 阶段二补原生 adapter |
| 9 家供应商密钥明文集中存储 | 阶段三加密；内网部署降低暴露面 |
| 上游模型目录动态变化（QoDuck 11-08 才出现） | 平台模型成员人工维护 + 定期同步工具 |
| data.json 写放大 | 单实例自用量级可控；必要时阶段三迁 SQLite |
| 单实例粘性状态进程内 | 已确认单实例，无此问题 |

## 10. 待办与开放问题

- [ ] 新增供应商（DeepSeek 等）API Key 由用户接入时逐家在面板配置（Qoder 上游 PAT 已在数据卷，无需重配）
- [ ] 阶段一动工：按 §7 路线图实施，代码直接改在 `deploy/qoder2api/src`（fork 自维护）

## 11. 部署记录（2026-11-19 阶段一落地）

### 已部署形态

| 项 | 值 |
|:--|:--|
| 服务 | 公共模型网关 `model-gateway`（fork 自 Qoder-2API-Go，新增 `gateway/` 包 + `cmd/gateway` 入口，旧代码零改动） |
| 端口 | **2325:8080**（桥接网络端口映射，见 compose） |
| 数据卷 | `model-gateway-data`（独立，配置文件 `/app/data/data.json`） |
| 镜像 | `model-gateway:latest`（本机构建，Dockerfile.gateway，多阶段 golang:1.22-alpine → alpine:3.20） |
| compose | `deploy/model-gateway/docker-compose.yml`（独立 compose 项目，与 qoder2api 项目互不影响） |
| 管理面板 | http://\<host\>:2325/admin （初始密码见 `model-gateway/.env`，不入库；登录后请修改；风格与 Qoder-2API 原版面板一致——整份复用其 `app.css` 设计令牌与组件类，并经浏览器截图逐页比对校准） |
| 客户端接入 | Base URL `http://\<host\>:2325/v1`，API Key 见 `model-gateway/.env`（不入库；与旧服务相同，换端口即用） |
| Qoder 上游 | 作为首个供应商接入：经 docker 内网直连（本机部署时亦可用 `http://127.0.0.1:2323/v1`；容器→宿主IP 方向可能被防火墙拦截，优先走容器名） |

### 首启自动播种（已验证）

空配置首次启动时：创建默认密钥 + 将现有 Qoder 服务登记为供应商 + 拉取其模型目录（12 个）+ 逐个建立同名平台模型（单成员、sticky、权重 1）。旧调用方换端口即获得完全等价能力，且随时可在面板添加新供应商与聚合平台模型。

### 已验证项

- [x] `go vet` + 14 个单元测试全绿（选路/冷却/粘性/WRR/故障转移/管理流）
- [x] `/health`、`/v1/models`（12 个平台模型）
- [x] 真实对话非流式（回复 + usage/credits 透传）
- [x] 真实对话流式（SSE 透传 + usage 块）
- [x] 管理端登录/供应商列表/统计/探测/面板加载
- [x] 现有 qoder2api :2323 全程未动

### 待接入（需用户提供 Key）

DeepSeek、硅基流动、Moonshot、阿里云百炼、智谱 GLM、OpenAI、OpenRouter —— 在面板「供应商 → 新增供应商」填 Base URL + Key，点「获取模型列表」勾选导入，再在「平台模型」里把供应商模型配置为成员（可聚合多个供应商到同一业务名，按权重分流、按优先级故障转移）。

### 网络形态（2026-11-19 二次调整）

- 标准桥接模式，端口映射 **2325:8080**（docker ps 可见）；
- 上游 Qoder 服务经 docker 内网直连容器名 `http://qoder2api:10081/v1`（加入外部网络 `qoder2api_default`）——部分环境防火墙会拦容器→宿主 IP 流量，故不走宿主地址；
- 数据卷内供应商 Base URL 已同步改为容器名地址（面板可查）。
