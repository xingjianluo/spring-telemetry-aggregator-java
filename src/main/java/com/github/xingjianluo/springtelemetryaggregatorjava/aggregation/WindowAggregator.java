package com.github.xingjianluo.springtelemetryaggregatorjava.aggregation;

import com.github.xingjianluo.springtelemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.IngestStatus;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.RecordKind;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.TelemetryRecord;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按时间窗口聚合：窗口边界、迟到兜底、去重、维度与容量限制都在这里判定。
 * 每个窗口状态的变更都在该窗口对象的监视器上串行化，
 * 因此并发写入同一窗口的结果与逐个处理完全一致。
 */
@Component
public class WindowAggregator {

    private final TelemetryProperties properties;

    /** windowStartMillis -> 窗口状态 */
    private final Map<Long, WindowState> windows = new ConcurrentHashMap<>();

    public WindowAggregator(TelemetryProperties properties) {
        this.properties = properties;
    }

    /** 窗口起点：向下对齐到窗口大小的整数倍，边界 [start, start+size) 左闭右开。 */
    public long windowStartOf(long timestampMillis) {
        long size = properties.getWindowSizeMillis();
        return Math.floorDiv(timestampMillis, size) * size;
    }

    public IngestResult accept(TelemetryRecord record, long nowMillis) {
        // 维度限制：单条记录属性键数量超限，属于可预期的资源约束拒绝
        int attrKeys = record.attributes() == null ? 0 : record.attributes().size();
        if (attrKeys > properties.getMaxAttributeKeys()) {
            return IngestResult.of(IngestStatus.REJECTED_LIMIT,
                    "attribute keys " + attrKeys + " exceed limit " + properties.getMaxAttributeKeys());
        }

        long windowStart = windowStartOf(record.timestampMillis());
        WindowState state = windows.computeIfAbsent(windowStart, k -> new WindowState());

        synchronized (state) {
            // 1. 去重 / 冲突判定：同 recordId 先看内容指纹
            String fingerprint = record.contentFingerprint();
            String seen = state.fingerprints.get(record.recordId());
            if (seen != null) {
                if (seen.equals(fingerprint)) {
                    state.duplicateCount++;
                    return IngestResult.of(IngestStatus.DUPLICATE,
                            "recordId " + record.recordId() + " already ingested with identical content");
                }
                state.conflictCount++;
                return IngestResult.of(IngestStatus.CONFLICT,
                        "recordId " + record.recordId() + " already ingested with different content");
            }
            state.fingerprints.put(record.recordId(), fingerprint);

            // 2. 迟到判定：超出可接受迟到范围，落兜底计数，不并入窗口统计值
            if (nowMillis - record.timestampMillis() > properties.getMaxLatenessMillis()) {
                state.lateFallbackCount++;
                return IngestResult.of(IngestStatus.LATE_FALLBACK,
                        "record is " + (nowMillis - record.timestampMillis())
                                + "ms late, exceeds max lateness " + properties.getMaxLatenessMillis()
                                + "ms; counted in fallback of window " + windowStart);
            }

            // 3. 容量限制：单窗口去重后的记录数超限
            if (state.acceptedCount >= properties.getMaxRecordsPerWindow()) {
                state.rejectedCount++;
                return IngestResult.of(IngestStatus.REJECTED_LIMIT,
                        "window " + windowStart + " reached max records "
                                + properties.getMaxRecordsPerWindow());
            }

            // 4. 正常入窗
            state.acceptedCount++;
            if (record.kind() == RecordKind.SPAN) {
                long duration = record.durationMillis() == null ? 0L : record.durationMillis();
                state.spanCount++;
                state.totalDurationMillis += duration;
                state.maxDurationMillis = Math.max(state.maxDurationMillis, duration);
                state.minDurationMillis = Math.min(state.minDurationMillis, duration);
            } else {
                state.metricCount++;
                state.metricValueSum += record.value() == null ? 0.0 : record.value();
            }
            return IngestResult.of(IngestStatus.ACCEPTED, "accepted into window " + windowStart);
        }
    }

    /** 查询某个窗口的聚合快照；窗口不存在时返回全零快照。 */
    public WindowStats snapshot(long windowStartMillis) {
        WindowState state = windows.get(windowStartMillis);
        long size = properties.getWindowSizeMillis();
        if (state == null) {
            return new WindowStats(windowStartMillis, size, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        }
        synchronized (state) {
            return new WindowStats(windowStartMillis, size,
                    state.acceptedCount, state.duplicateCount, state.conflictCount,
                    state.lateFallbackCount, state.rejectedCount,
                    state.spanCount, state.totalDurationMillis,
                    state.maxDurationMillis,
                    state.spanCount == 0 ? 0 : state.minDurationMillis,
                    state.metricCount, state.metricValueSum);
        }
    }

    /** 单窗口内部状态，所有变更必须持有本对象监视器。 */
    private static final class WindowState {
        final Map<String, String> fingerprints = new ConcurrentHashMap<>();
        long acceptedCount;
        long duplicateCount;
        long conflictCount;
        long lateFallbackCount;
        long rejectedCount;
        long spanCount;
        double totalDurationMillis;
        double maxDurationMillis = Long.MIN_VALUE;
        double minDurationMillis = Long.MAX_VALUE;
        long metricCount;
        double metricValueSum;
    }
}
