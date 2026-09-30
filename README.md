# Spring Telemetry Aggregator

本地遥测采集与聚合服务：调用链片段（SPAN）与度量点（METRIC）经统一入口归一到同一内部模型，
按时间窗口给出可解释的聚合结果；重复上报、乱序到达与异常数据都有稳定且可区分的处理结论。

## 上报协议

`POST /telemetry/ingest`，请求体为 JSON 数组，每条记录字段：

| 字段 | 类型 | 说明 |
|---|---|---|
| `recordId` | string | 记录唯一标识，去重与冲突判定的依据，必填 |
| `kind` | string | `SPAN` 或 `METRIC`（大小写不敏感），必填 |
| `name` | string | 操作名 / 指标名，必填 |
| `timestampMillis` | long | 事件发生时间（epoch 毫秒），必须为正且不超过当前时间 + 5 分钟时钟偏移 |
| `durationMillis` | long | SPAN 必填，非负 |
| `value` | double | METRIC 必填 |
| `attributes` | object | 属性维度，键数上限见下 |

响应是与输入一一对应的结果数组，每条含 `status` 与 `reason`（可排查的判定依据）：

| status | 含义 |
|---|---|
| `ACCEPTED` | 正常入窗 |
| `DUPLICATE` | 同 `recordId` 同内容重复提交，不抬高任何统计 |
| `CONFLICT` | 同 `recordId` 但内容指纹不同，语义冲突，不并入统计 |
| `REJECTED_FORMAT` | 字段缺失 / 时间戳非法等格式问题（reason 指明具体字段） |
| `LATE_FALLBACK` | 超出可接受迟到范围，落入兜底计数，不并入任何窗口的统计值 |
| `REJECTED_LIMIT` | 触发维度 / 容量限制（reason 指明超限项与阈值） |

格式问题（`REJECTED_FORMAT`）与语义冲突（`CONFLICT`）是两类不同的结论，不会混同，也不会静默丢弃。

## 窗口与迟到规则

- 窗口边界：起点向下对齐到 `telemetry.window-size-millis`（默认 60000）的整数倍，左闭右开 `[start, start+size)`。
- 去重：窗口内按 `recordId` 记录内容指纹（kind+name+时间戳+时长/值+排序后的属性）。
  指纹相同判 `DUPLICATE`，不同判 `CONFLICT`；两者都不影响窗口统计值。
- 迟到：`now - timestampMillis > telemetry.max-lateness-millis`（默认 30000）判 `LATE_FALLBACK`，
  只在其所属窗口累加兜底计数 `lateFallbackCount`，耗时 / 数值不并入任何窗口。
  迟到记录的重发仍判 `DUPLICATE`，结论稳定。
- 查询：`GET /telemetry/windows/{windowStartMillis}` 返回该窗口快照：
  各类状态计数、SPAN 数 / 总耗时 / 平均 / 最大 / 最小耗时、METRIC 数 / 数值总和。

## 维度与容量限制

| 配置 | 默认 | 超限行为 |
|---|---|---|
| `telemetry.max-attribute-keys` | 32 | 单条记录属性键超限 → `REJECTED_LIMIT`，reason 含实际键数与阈值 |
| `telemetry.max-records-per-window` | 100000 | 单窗口去重后记录数超限 → `REJECTED_LIMIT`，reason 含窗口起点与阈值 |

## 并发一致性

每个窗口的内部状态变更都在该窗口对象的监视器上串行化，
多个请求同时写入同一窗口的最终结果与逐个处理完全一致（由并发对拍测试验证）。

## 本地验证

```bash
# 全部测试（含乱序、重复、冲突、格式、迟到、限制、并发组合场景）
mvn test

# 只看窗口聚合测试日志：每条用例打印输入窗口、判定依据与最终聚合值
mvn test -Dtest=WindowAggregationTests

# 启动服务本地试报
mvn spring-boot:run
curl -X POST localhost:8080/telemetry/ingest -H 'Content-Type: application/json' -d '[
  {"recordId":"r1","kind":"SPAN","name":"op","timestampMillis":'"$(date +%s000)"',"durationMillis":120,"attributes":{"svc":"a"}}
]'
curl localhost:8080/telemetry/windows/$(($(date +%s000) / 60000 * 60000))
```
