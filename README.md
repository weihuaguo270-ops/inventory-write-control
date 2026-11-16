# 库存受控写入（inventory-write-control）

`inventory-write-control` 面向需要占用库存、改价改可售库存的商家与订单元。它把同一批库存的并发写入做成可拦住超卖、错价和误改已预占库存的过程：预占必须幂等，超时未支付要释放，支付成功才确认占用；改价或改库存须先通过可写死的规则，再由人工审批钉住方案指纹后才能写库。Go 单服务、PostgreSQL、Redis 和 RocketMQ 是实现这些路径的工程能力，不是单独对外的中间件展示。

## 一句话定位

库存受控写入帮助两件库存上的事：**下单预占**和**改价改库存**。模型可以提出改价或补货草案，但不能直接写库。当前可演示的是预占主路径；改价改库存与 Agent 出草案尚未实现。

## 做什么 / 不做什么

| 做什么 | 不做什么 |
|---|---|
| 对同一 SKU 并发预占，重复请求不重复扣减，库存不足时挡住超卖 | 不提供完整电商前台或完整支付系统 |
| 超时未支付释放预占，支付成功确认占用；事件可重投且只生效一次 | 不宣称生产多仓、多租户或长期线上 SLA |
| 为改价改库存预留规则检查与指纹审批写入（尚未实现） | 不做内容审核台或通用客服工单，也不拆微服务 |

当前证据是可运行的工程原型。S1 已在本机跑通预占、确认、释放和并发不超卖；消息队列可用 RocketMQ，Broker 不可用时也可用同一套事件分发做演示，不代表生产交易流量已经接入。

## 两条路径

### 库存预占（已可演示）

一笔订单到来时，服务按幂等键预占可售库存：热库存在 Redis 原子扣减，预占单在 PostgreSQL 行锁落账。超时未支付则释放；支付成功则确认占用。同一请求重放不得再扣一次，同一支付或超时事件重投只生效一次；并发抢同一 SKU 时，成功数不得超过可售数量。

演示闭环是：下单预占 → 超时释放或支付确认 → 用重放和并发请求核对库存账。入口：

```powershell
docker compose up -d postgres
docker compose --profile mq up -d rocketmq-namesrv rocketmq-broker

$env:SEED_RESET='1'
$env:DISABLE_MQ='0'
$env:DATABASE_URL='postgres://inv:inv@localhost:5432/inventory?sslmode=disable'
$env:REDIS_ADDR='localhost:6379'
$env:ROCKETMQ_NAMESRV='127.0.0.1:9876'
go run ./cmd/server
```

另开终端验收：

```
go run ./cmd/demo -mode=all -base=http://127.0.0.1:8080
go run ./cmd/demo -mode=mq  -base=http://127.0.0.1:8080
```

`DISABLE_MQ=1` 时可用 `POST /events` 走与 MQ 消费相同的分发路径。范围与 S1 证据见 [docs/SCOPE.md](docs/SCOPE.md)、[docs/S1-SLICE.md](docs/S1-SLICE.md)。

### 改价 / 改可售库存（尚未实现）

商家改价、改可售数量或改生效时间时，先按上限、生效窗口、同一 SKU 冲突、是否会把已预占库存改没等规则检查；通过后由人工审批钉住方案指纹，指纹一致才写库。这一段留到后续切片，不在当前演示里展开。

## 演示讲法

先对同一 SKU 并发发起多笔预占，说明成功数不超过可售库存、同一幂等键重放不会再扣。再分别走超时释放和支付确认，并把同一事件重投一次，确认库存只变一次。最后说明改价改库存还要经过规则检查和指纹审批，模型只出草案，不能直接写库；现在这一段尚未实现，项目仍是可运行原型。

## 进一步阅读

- 范围说明：[docs/SCOPE.md](docs/SCOPE.md)
- S1 切片与验收：[docs/S1-SLICE.md](docs/S1-SLICE.md)
- 环境变量：[.env.example](.env.example)
