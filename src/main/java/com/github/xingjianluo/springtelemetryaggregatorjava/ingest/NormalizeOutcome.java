package com.github.xingjianluo.springtelemetryaggregatorjava.ingest;

import com.github.xingjianluo.springtelemetryaggregatorjava.model.TelemetryRecord;

/**
 * 归一结果：成功时携带内部记录；失败时携带可区分的原因。
 */
public record NormalizeOutcome(
        boolean ok,
        TelemetryRecord record,
        String reasonCode,
        String detail
) {
    public static NormalizeOutcome success(TelemetryRecord record) {
        return new NormalizeOutcome(true, record, null, null);
    }

    public static NormalizeOutcome failure(String reasonCode, String detail) {
        return new NormalizeOutcome(false, null, reasonCode, detail);
    }
}
