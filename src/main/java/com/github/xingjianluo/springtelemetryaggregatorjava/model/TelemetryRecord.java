package com.github.xingjianluo.springtelemetryaggregatorjava.model;

import java.util.Map;

/**
 * 统一内部模型：调用链片段与度量点归一到同一结构。
 */
public record TelemetryRecord(
        String recordId,
        RecordKind kind,
        String name,
        long timestampMillis,
        Long durationMillis,
        Double value,
        Map<String, String> attributes) {

    /** 内容指纹，用于区分“重复提交”与“同 ID 语义冲突”。 */
    public String contentFingerprint() {
        // 属性按 key 排序后拼接，保证同一逻辑内容指纹稳定
        String attrs = attributes == null
                ? ""
                : attributes.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .map(e -> e.getKey() + "=" + e.getValue())
                        .reduce((a, b) -> a + "," + b)
                        .orElse("");
        return kind + "|" + name + "|" + timestampMillis + "|" + durationMillis + "|" + value + "|" + attrs;
    }
}
