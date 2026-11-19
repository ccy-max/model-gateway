# model-gateway · 公共模型网关

多供应商 LLM 聚合网关：OpenAI 兼容对外协议 + 中文管理面板。代码 fork 自
[Qoder-2API-Go](https://github.com/EchoPing07/Qoder-2API-Go)（v0.6.3, `1f8b4e2`）自维护，
在其上叠加自研 `gateway` 包实现多供应商路由。

## 能力一览

- **多供应商接入**：openai-compat 供应商（DeepSeek、硅基流动、Moonshot、百炼、GLM、OpenAI、OpenRouter 等）+ Qoder 链路
- **平台模型两级路由**：业务名 + 上游原名别名均可调用；sticky / 加权轮询 / 最少并发三策略 + 成员故障转移
- **供应商密钥池**：同供应商多 Key 按权重负载均衡，失败（401/403/429/5xx）冷却摘除、恢复自动回归
- **模型目录自动发现**：按供应商拉取上游模型列表、勾选导入（只增不删，导入按供应商+模型名去重）
- **日志与统计**：请求日志 / 每日统计 SQLite(WAL) 落库，重启不丢；输入/输出/总 tokens 与缓存命中率；日期范围筛选
- **运维**：健康探测、管理密码修改、`/health` 健康检查

## 目录结构

```
model-gateway/
├── src/                  # Go 源码（fork + gateway 包 + Dockerfile.gateway）
├── GATEWAY_PLAN.md       # 方案设计与实施路线图
├── qoder2api/            # 上游 Qoder 服务编排（docker-compose.yml + .env.example）
└── model-gateway/        # 网关服务编排（docker-compose.yml + .env.example）
```

## 快速开始

```bash
# 1. 准备环境变量（两个 compose 目录各一份）
cp qoder2api/.env.example qoder2api/.env
cp model-gateway/.env.example model-gateway/.env
# 按注释填入真实值（.env 已被 .gitignore 忽略，不入库）

# 2. 构建网关镜像
docker build -t model-gateway:latest -f src/Dockerfile.gateway src/

# 3. 启动（先起上游 qoder2api，再起网关；网关经 docker 内网容器名直连上游）
cd qoder2api && docker compose up -d && cd ..
cd model-gateway && docker compose up -d && cd ..
```

- 管理面板：`http://<host>:2325/admin`
- OpenAI 兼容端点：`http://<host>:2325/v1`（模型目录见 `/v1/models`）

## 开发

```bash
cd src
go vet ./gateway/... ./cmd/...
go test ./gateway/... -count=1
```

路线图（阶段二/三：Claude/Gemini 原生 adapter、熔断增强、按成员统计、/metrics、密钥加密等）
见 `GATEWAY_PLAN.md` §7。
