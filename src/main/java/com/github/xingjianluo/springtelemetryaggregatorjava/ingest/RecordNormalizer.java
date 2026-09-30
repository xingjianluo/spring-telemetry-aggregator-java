package com.github.xingjianluo.springtelemetryaggregatorjava.ingest;

import com.github.xingjianluo.springtelemetryaggregatorjava.dto.TelemetryReportRequest;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.RecordKind;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.RejectReason;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.TelemetryRecord;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.TreeMap;

/**
 * 把上报 DTO 归一为内部模型，并对字段缺失、非法时间戳等格式问题做校验。
 *
 * <p>校验顺序（先格式、后语义）：请求体非空 → id → kind → timestamp → duration。
 * 语义冲突（同一 id 不同内容）不在本类判定，由窗口层基于已接收指纹判定。</p>
 */
@Component
public class RecordNormalizer {

    /** 时间戳必须为正数，且不超过接收时刻一年（防呆，避免明显非法/未来值）。 */
    private static final long MAX_FUTURE_MILLIS = 365L * 24 * 60 * 60 * 1000;

    public NormalizeOutcome normalize(TelemetryReportRequest request) {
        if (request == null) {
            return NormalizeOutcome.failure(RejectReason.MALFORMED_PAYLOAD.name(), "记录为空");
        }

        String id = trimToNull(request.getId());
        if (id == null) {
            return NormalizeOutcome.failure(RejectReason.MISSING_ID.name(), "缺少必填字段 id");
        }

        String kindRaw = trimToNull(request.getKind());
        if (kindRaw == null) {
            return NormalizeOutcome.failure(RejectReason.MISSING_KIND.name(),
                    "缺少必填字段 kind (id=" + id + ")");
        }
        RecordKind kind;
        try {
            kind = RecordKind.valueOf(kindRaw.toUpperCase());
        } catch (IllegalArgumentException e) {
            return NormalizeOutcome.failure(RejectReason.UNKNOWN_KIND.name(),
                    "未知 kind=" + kindRaw + " (id=" + id + ")，仅支持 TRACE/METRIC");
        }

        Long ts = request.getTimestamp();
        if (ts == null) {
            return NormalizeOutcome.failure(RejectReason.INVALID_TIMESTAMP.name(),
                    "缺少必填字段 timestamp (id=" + id + ")");
        }
        if (ts <= 0 || ts > System.currentTimeMillis() + MAX_FUTURE_MILLIS) {
            return NormalizeOutcome.failure(RejectReason.INVALID_TIMESTAMP.name(),
                    "timestamp=" + ts + " 非正或超出当前时间一年以上 (id=" + id + ")");
        }

        Long duration = request.getDurationNanos();
        if (duration != null && duration < 0) {
            return NormalizeOutcome.failure(RejectReason.INVALID_DURATION.name(),
                    "durationNanos=" + duration + " 不能为负 (id=" + id + ")");
        }

        // 属性归一：用户属性 + trace/span 标识合并到同一套维度，按 key 排序保证指纹稳定。
        Map<String, String> attributes = new TreeMap<>();
        if (request.getAttributes() != null) {
            request.getAttributes().forEach((k, v) -> {
                if (k != null && v != null) {
                    attributes.put(k, v);
                }
            });
        }
        putIfPresent(attributes, "traceId", request.getTraceId());
        putIfPresent(attributes, "spanId", request.getSpanId());
        putIfPresent(attributes, "name", request.getName());
        putIfPresent(attributes, "service", request.getService());

        String fingerprint = buildFingerprint(kind, ts, duration,
                trimToNull(request.getName()), trimToNull(request.getService()), attributes);

        TelemetryRecord record = new TelemetryRecord(
                id, kind, ts, duration,
                trimToNull(request.getName()),
                trimToNull(request.getService()),
                Map.copyOf(attributes),
                fingerprint);
        return NormalizeOutcome.success(record);
    }

    private static void putIfPresent(Map<String, String> target, String key, String value) {
        String v = trimToNull(value);
        if (v != null) {
            target.put(key, v);
        }
    }

    /**
     * 规范化指纹：类型|时间戳|耗时|名称|服务|有序属性键值。
     * 同 id 但任一业务字段不同 => 指纹不同 => 语义冲突。
     */
    private static String buildFingerprint(RecordKind kind, long timestamp, Long duration,
                                           String name, String service, Map<String, String> attrs) {
        StringBuilder sb = new StringBuilder();
        sb.append(kind).append('|').append(timestamp).append('|')
          .append(duration == null ? "" : duration).append('|')
          .append(name == null ? "" : name).append('|')
          .append(service == null ? "" : service);
        attrs.forEach((k, v) -> sb.append('|').append(k).append('=').append(v));
        return sb.toString();
    }

    private static String trimToNull(String s) {
        if (s == null) {
            return null;
        }
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }
}
