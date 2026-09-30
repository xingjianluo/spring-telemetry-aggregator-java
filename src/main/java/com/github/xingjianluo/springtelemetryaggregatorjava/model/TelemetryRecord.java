package com.github.xingjianluo.springtelemetryaggregatorjava.model;

import java.util.Map;

/**
 * 调用链片段与度量点归一后的内部模型。
 *
 * @param id            上报记录唯一标识（去重/冲突判定依据）
 * @param kind          记录类型
 * @param timestamp     事件时间，epoch 毫秒
 * @param durationNanos 耗时（纳秒），可为空表示该记录无耗时语义
 * @param name          片段/度量名称
 * @param service       来源服务
 * @param attributes    归一后的属性维度（含 traceId/spanId 等）
 * @param fingerprint   归一化内容指纹（规范化字符串），同一 id 指纹不同即语义冲突
 */
public record TelemetryRecord(
        String id,
        RecordKind kind,
        long timestamp,
        Long durationNanos,
        String name,
        String service,
        Map<String, String> attributes,
        String fingerprint
) {
}
