package com.github.xingjianluo.springtelemetryaggregatorjava.ingest;

import java.util.Map;

/**
 * 上报协议 DTO：字段原样承载，归一与校验在 IngestionService 完成。
 */
public record IngestRequest(
        String recordId,
        String kind,
        String name,
        Long timestampMillis,
        Long durationMillis,
        Double value,
        Map<String, String> attributes) {
}
