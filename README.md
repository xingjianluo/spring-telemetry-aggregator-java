# Spring Telemetry Aggregator（本地遥测采集与聚合）

一个纯内存、零外部依赖的 Spring Boot 服务，提供统一入口接收不同来源（调用链片段
`TRACE`、度量点 `METRIC`）上报的数据，按**事件时间窗口**给出可解释的聚合结果，
并对**重复上报、乱序到达、迟到数据、异常数据**给出稳定且可区分的处理结论。

## 1. 上报协议

### 1.1 批量上报

`POST /api/telemetry/reports`

```json
{
  "records": [
    {
      "id": "rec-001",
      "kind": "TRACE",
      "timestamp": 1700000000000,
      "durationNanos": 1500,
      "name": "GET /orders",
      "service": "order-service",
      "traceId": "tr-abc",
      "spanId": "sp-1",
      "attributes": {"region": "cn", "http.status": "200"}
    },
    {
      "id": "rec-002",
      "kind": "METRIC",
      "timestamp": 1700000000500,
      "durationNanos": 12,
      "name": "queue.lag",
      "service": "order-service",
      "attributes": {"region": "cn"}
    }
  ]
}
```

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `id` | 是 | 记录唯一标识，**去重与语义冲突的判定依据** |
| `kind` | 是 | `TRACE`（调用链片段）或 `METRIC`（度量点），大小写不敏感 |
| `timestamp` | 是 | 事件时间，epoch 毫秒；必须为正且不超过当前时间一年 |
| `durationNanos` | 否 | 耗时（纳秒），非负；为空表示该记录无耗时语义 |
| `name` / `service` | 否 | 名称 / 来源服务，同时作为分组维度 |
| `traceId` / `spanId` | 否 | 调用链标识，归一后并入统一属性维度 |
| `attributes` | 否 | 自定义键值对维度；键或值为 null 的条目被忽略 |

TRACE 与 METRIC 归一为同一个内部模型 `TelemetryRecord`（id/kind/timestamp/duration/
name/service/统一属性表/内容指纹）。`traceId`、`spanId`、`name`、`service` 都会并入
同一张属性维度表，因此查询时可用同一套 `groupBy` 维度聚合两类数据。

### 1.2 逐条处理结论（彼此可区分，不静默丢弃）

响应逐元素给出 `status` + `reason` + `detail` + `windowStart`，并带汇总计数：

| status | reason | 含义 |
| --- | --- | --- |
| `ACCEPTED` | — | 归一通过并并入对应窗口 |
| `DUPLICATE` | — | **幂等重复**：同 `id` 且内容指纹完全相同，计数/耗时不变 |
| `REROUTED_LATE` | `TOO_LATE` | 超出可接受迟到范围，进入**迟到兜底存储**，不并入任何窗口 |
| `REJECTED_FORMAT` | `MISSING_ID` / `MISSING_KIND` / `UNKNOWN_KIND` / `INVALID_TIMESTAMP` / `INVALID_DURATION` / `MALFORMED_PAYLOAD` | **格式问题**：字段缺失或字段值非法 |
| `REJECTED_CONFLICT` | `ID_CONFLICT` | **语义冲突**：同 `id` 已接收过，但业务内容指纹不同 |
| `REJECTED_LIMIT` | `TOO_MANY_DIMENSIONS` / `WINDOW_FULL` | 维度膨胀或单窗口超量，被资源限制拒绝 |

- “格式问题”与“语义冲突”严格区分：前者在归一阶段判定（字段都还没进窗口），
  后者在窗口层基于已接收记录的内容指纹判定。
- 单条异常不影响同批其他记录，批内逐条处理、逐条返回结论。
- 请求体不是合法 JSON 时返回 `400 {"reason":"MALFORMED_PAYLOAD", ...}`。

## 2. 窗口与迟到规则

- **固定窗口对齐**：窗口长度 `telemetry.window-millis`（默认 10000ms）。事件时间
  `ts` 属于窗口 `[floor(ts/W)*W, floor(ts/W)*W + W)`，窗口边界与到达顺序无关，
  因此**乱序到达**的同一批事件仍聚合到正确窗口。
- **水位线（watermark）**：`WM = max(已观察到的事件时间) - telemetry.allowed-lateness-millis`
  （默认 30000ms）。水位线只随更大的事件时间推进，不回退。
- **窗口关闭**：当某窗口结束时刻 `ws + W <= WM` 时视为关闭。
  - 事件仍落在关闭窗口之前/之内 → 判定 `TOO_LATE`，进入**迟到兜底**，绝不并入当前窗口；
  - 落在窗口结束时刻的边界事件（`ws + W == WM` 时）按“关闭”处理（左闭右开）。
- **迟到兜底**：过晚事件单独留存在有界的 late sink 中（不参与聚合），并通过
  `GET /api/telemetry/rejections` 可查。迟到数据若再次重复提交，结论是 `DUPLICATE`，
  兜底存储中也只保留一份。
- 判定顺序：身份（重复/冲突） → 水位线推进与迟到判定 → 维度上限 → 窗口容量 → 提交。

## 3. 维度与资源限制

均在 `application.properties` 中可调（前缀 `telemetry`）：

| 参数 | 默认 | 含义 | 拒绝原因 |
| --- | --- | --- | --- |
| `telemetry.window-millis` | 10000 | 窗口长度（ms） | — |
| `telemetry.allowed-lateness-millis` | 30000 | 可接受迟到（ms） | `TOO_LATE` |
| `telemetry.max-dimensions-per-window` | 100 | 单窗口不同 `key=value` 维度数上限 | `TOO_MANY_DIMENSIONS` |
| `telemetry.max-records-per-window` | 10000 | 单窗口记录条数上限 | `WINDOW_FULL` |
| `telemetry.max-windows-retained` | 100 | 内存保留窗口数，超出淘汰最旧窗口 | — |
| `telemetry.max-rejected-events` | 1000 | 拒绝/兜底留痕与 late sink 条数上限 | — |

被限流的记录返回 `REJECTED_LIMIT` 且 `detail` 中包含命中的窗口与当时的阈值，
最近的拒绝/兜底事件可通过 `GET /api/telemetry/rejections?limit=100` 排查。

## 4. 聚合查询

`GET /api/telemetry/aggregations?from={ms}&to={ms}&groupBy=service&groupBy=region`

- 查询事件时间落在 `[from,to)` 的窗口（要求 `0 <= from < to`，否则 400）；
- `groupBy` 可重复指定任意属性维度（含 `service`/`name`/`traceId` 等）；
  不传则只返回窗口总量；
- 每组返回 `count`、`durationNanosSum/Min/Max/Avg`（无耗时记录时 avg 为 null）；
- 窗口视图带 `windowStart/windowEnd/watermark/closed`，`closed=true` 表示该窗口
  已被水位线关闭。

## 5. 并发一致性

窗口与身份表的“检查 + 提交”在同一把锁内原子完成（迟到存储使用独立锁，加锁顺序
固定），因此多请求并发写入同一窗口的结果与逐个串行处理一致：唯一记录恰好接收一次，
重复不抬高计数，冲突不覆盖原内容，容量上限不会被并发击穿。相关不变量由
`ConcurrencyTest` 在 8~10 线程下验证。

## 6. 本地验证

```bash
# 运行全部测试（离线）：16 个用例，覆盖归一、窗口、乱序、重复、冲突、迟到、限流、并发组合、HTTP 端到端
mvn test -o

# 只跑某一组
mvn test -o -Dtest=WindowManagerTest
mvn test -o -Dtest=ConcurrencyTest

# 启动服务（application.properties 默认 server.port=0 随机端口；
# 如需固定端口，改为 server.port=8080，或启动时 --server.port=8080）
mvn spring-boot:run
```

启动后可用 curl 验证：

```bash
curl -s -XPOST localhost:8080/api/telemetry/reports -H 'Content-Type: application/json' -d '{
  "records": [
    {"id":"a1","kind":"METRIC","timestamp":1700000000000,"durationNanos":100,"service":"s1","attributes":{"region":"cn"}},
    {"id":"a1","kind":"METRIC","timestamp":1700000000000,"durationNanos":100,"service":"s1","attributes":{"region":"cn"}}
  ]}'
# => accepted=1, duplicate=1

curl -s 'localhost:8080/api/telemetry/aggregations?from=1699990000000&to=1700010000000&groupBy=service'
curl -s 'localhost:8080/api/telemetry/rejections?limit=20'
```

测试日志会打印**输入窗口、判定依据（detail）和最终聚合值**，例如：

```
[重复] 输入窗口=10000 同一份数据提交2次 -> 判定=DUPLICATE 依据=幂等重复...; 最终 count=1 sum=100
[并发混合] ... 判定统计 accepted=48, duplicate=103, conflict=49, reroutedLate=32, rejectedLimit=248;
           窗口10000最终count=50, 水位线=8000, 迟到兜底=32
```

### 测试索引

| 测试类 | 覆盖内容 |
| --- | --- |
| `RecordNormalizerTest` | 字段缺失、未知 kind、非法时间戳/耗时、指纹差异、TRACE/METRIC 归一 |
| `WindowManagerTest` | 窗口对齐、乱序、幂等重复、语义冲突、迟到边界与兜底、维度上限、容量上限、分组聚合 |
| `ConcurrencyTest` | 多线程同窗口去重；重复+冲突+迟到+容量组合下的串行等价不变量 |
| `TelemetryControllerTest` | HTTP 端到端：批量上报、逐条结论、聚合查询、拒绝留痕、非法 JSON/区间 |
