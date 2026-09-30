package com.github.xingjianluo.springtelemetryaggregatorjava.model;

/**
 * 单条记录的处理结果：状态 + 可排查的原因说明。
 */
public record IngestResult(IngestStatus status, String reason) {

    public static IngestResult of(IngestStatus status, String reason) {
        return new IngestResult(status, reason);
    }
}
