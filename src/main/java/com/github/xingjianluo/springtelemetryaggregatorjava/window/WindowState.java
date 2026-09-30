package com.github.xingjianluo.springtelemetryaggregatorjava.window;

import com.github.xingjianluo.springtelemetryaggregatorjava.model.TelemetryRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 单个时间窗口的可变聚合状态。
 *
 * <p>所有读写都必须在持有本对象 monitor 的前提下进行（见 {@link WindowManager}），
 * 保证并发提交与逐个串行处理的结果一致。分组聚合在查询时基于 {@code records}
 * 按指定维度现算，因此同一份原始数据可以按不同维度反复查询。</p>
 */
public class WindowState {

    private final long start;
    private final long end;
    private long count;
    private long durationSum;
    private long durationMin = Long.MAX_VALUE;
    private long durationMax = Long.MIN_VALUE;
    private long durationCount;
    private final Set<String> dimensions = new HashSet<>();
    private final Map<String, String> acceptedHashes = new java.util.HashMap<>();
    private final List<TelemetryRecord> records = new ArrayList<>();

    public WindowState(long start, long end) {
        this.start = start;
        this.end = end;
    }

    public long getStart() { return start; }
    public long getEnd() { return end; }
    public long getCount() { return count; }
    public long getDurationSum() { return durationSum; }
    public long getDurationMin() { return durationMin; }
    public long getDurationMax() { return durationMax; }
    public long getDurationCount() { return durationCount; }
    public Set<String> getDimensions() { return dimensions; }
    public Map<String, String> getAcceptedHashes() { return acceptedHashes; }

    /** 不可变视图，仅用于在持锁期间读取，调用方不得在锁外保留引用。 */
    public List<TelemetryRecord> records() { return Collections.unmodifiableList(records); }

    /** 并入一条记录：计数、耗时统计、维度登记、指纹登记。 */
    public void accept(TelemetryRecord record) {
        records.add(record);
        count++;
        acceptedHashes.put(record.id(), record.fingerprint());
        if (record.durationNanos() != null) {
            long d = record.durationNanos();
            durationSum += d;
            durationCount++;
            if (d < durationMin) {
                durationMin = d;
            }
            if (d > durationMax) {
                durationMax = d;
            }
        }
        record.attributes().keySet().forEach(k -> dimensions.add(k + "=" + record.attributes().get(k)));
    }
}
