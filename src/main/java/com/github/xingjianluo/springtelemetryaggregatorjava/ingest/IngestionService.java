package com.github.xingjianluo.springtelemetryaggregatorjava.ingest;

import com.github.xingjianluo.springtelemetryaggregatorjava.aggregation.WindowAggregator;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.IngestStatus;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.RecordKind;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.TelemetryRecord;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 统一入口：归一化 -> 格式校验 -> 交给窗口聚合器做去重/冲突/迟到/限制判定。
 * 批次内每条记录独立判定，结果顺序与输入一一对应。
 */
@Service
public class IngestionService {

    /** 允许的最大时钟超前（毫秒），超出视为非法时间戳 */
    static final long MAX_CLOCK_SKEW_MILLIS = 5 * 60_000L;

    private final WindowAggregator aggregator;

    public IngestionService(WindowAggregator aggregator) {
        this.aggregator = aggregator;
    }

    public List<IngestResult> ingest(List<IngestRequest> batch) {
        return ingest(batch, System.currentTimeMillis());
    }

    /** 便于测试：显式指定判定用的当前时间。 */
    public List<IngestResult> ingest(List<IngestRequest> batch, long nowMillis) {
        List<IngestResult> results = new ArrayList<>(batch.size());
        for (IngestRequest request : batch) {
            results.add(ingestOne(request, nowMillis));
        }
        return results;
    }

    private IngestResult ingestOne(IngestRequest request, long nowMillis) {
        IngestResult formatError = validateFormat(request, nowMillis);
        if (formatError != null) {
            return formatError;
        }
        TelemetryRecord record = normalize(request);
        return aggregator.accept(record, nowMillis);
    }

    /** 格式校验：任何字段缺失或非法都返回带原因的 REJECTED_FORMAT，绝不静默丢弃。 */
    private IngestResult validateFormat(IngestRequest request, long nowMillis) {
        if (request == null) {
            return IngestResult.of(IngestStatus.REJECTED_FORMAT, "request is null");
        }
        if (isBlank(request.recordId())) {
            return IngestResult.of(IngestStatus.REJECTED_FORMAT, "recordId is missing or blank");
        }
        if (isBlank(request.name())) {
            return IngestResult.of(IngestStatus.REJECTED_FORMAT, "name is missing or blank");
        }
        RecordKind kind = parseKind(request.kind());
        if (kind == null) {
            return IngestResult.of(IngestStatus.REJECTED_FORMAT,
                    "kind must be SPAN or METRIC, got: " + request.kind());
        }
        Long ts = request.timestampMillis();
        if (ts == null || ts <= 0) {
            return IngestResult.of(IngestStatus.REJECTED_FORMAT,
                    "timestampMillis is missing or non-positive: " + ts);
        }
        if (ts - nowMillis > MAX_CLOCK_SKEW_MILLIS) {
            return IngestResult.of(IngestStatus.REJECTED_FORMAT,
                    "timestampMillis " + ts + " is beyond allowed clock skew of now " + nowMillis);
        }
        if (kind == RecordKind.SPAN) {
            if (request.durationMillis() == null || request.durationMillis() < 0) {
                return IngestResult.of(IngestStatus.REJECTED_FORMAT,
                        "SPAN requires non-negative durationMillis, got: " + request.durationMillis());
            }
        } else {
            if (request.value() == null) {
                return IngestResult.of(IngestStatus.REJECTED_FORMAT, "METRIC requires value");
            }
        }
        return null;
    }

    /** 归一化：外部 DTO -> 统一内部模型，属性空值归一为不可变空表。 */
    private TelemetryRecord normalize(IngestRequest request) {
        Map<String, String> attributes = request.attributes() == null
                ? Map.of()
                : Map.copyOf(request.attributes());
        return new TelemetryRecord(
                request.recordId().trim(),
                parseKind(request.kind()),
                request.name().trim(),
                request.timestampMillis(),
                request.durationMillis(),
                request.value(),
                attributes);
    }

    private RecordKind parseKind(String kind) {
        if (kind == null) {
            return null;
        }
        try {
            return RecordKind.valueOf(kind.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
