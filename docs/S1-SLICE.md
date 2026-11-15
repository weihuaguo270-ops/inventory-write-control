# S1：预占 → 超时释放 / 支付确认

> 范围见 [SCOPE.md](SCOPE.md)。本刀只做预占主路径，不含改价/改库存与模型草案。

## 目标

证明同一 SKU 并发预占不超卖、幂等键不重复扣减；超时释放与支付确认可幂等消费。

## 边界

| 做 | 不做 |
|---|---|
| `POST /reservations` 预占 | 改价 / 改可售库存 |
| 确认占用 / 释放预占 | 人工审批与方案指纹 |
| Redis 热库存原子扣减 + PG 行锁落账 | Python Agent |
| RocketMQ 消费 `payment.succeeded` / `order.timeout_cancelled`（与 `POST /events` 同一分发） | 完整支付与电商前台 |
| `cmd/demo` 并发验收 | 微服务拆分 |

## 运行

```powershell
docker compose up -d postgres
docker compose --profile mq up -d rocketmq-namesrv rocketmq-broker
# 首次需建 topic（autoCreate 对 Go 订阅不够）：
docker exec inventory-write-control-rocketmq-broker-1 sh -c "cd /home/rocketmq/rocketmq-5.3.1/bin && sh mqadmin updateTopic -n rocketmq-namesrv:9876 -t inventory-events -c DefaultCluster"
# Redis：本机已有实例可用 REDIS_ADDR=localhost:6379

$env:SEED_RESET='1'
$env:DISABLE_MQ='0'
$env:DATABASE_URL='postgres://inv:inv@localhost:5432/inventory?sslmode=disable'
$env:REDIS_ADDR='localhost:6379'
$env:ROCKETMQ_NAMESRV='127.0.0.1:9876'   # 勿用 localhost，client-go 会报 IP addr error
go run ./cmd/server

# 另开终端
go run ./cmd/demo -mode=all -base=http://127.0.0.1:8080
go run ./cmd/demo -mode=mq  -base=http://127.0.0.1:8080
```

环境变量见 `.env.example`。`DISABLE_MQ=1` 时可用 `POST /events`（与 MQ 同一 `DispatchEvent`）。

## 验收证据（2026-11-15）

```text
== idempotent replay == PASS
== payment confirm idempotent == PASS
== timeout release idempotent == PASS
== concurrent no-oversell ==
available=8 workers=28 success=8 conflict=20
after sellable=0 reserved=9
PASS

== rocketmq payment + timeout ==
mq confirmed / mq released（同 event_id 重投）
PASS
```

四条均与 SCOPE §10 对齐；RocketMQ 真消费路径已验证。
