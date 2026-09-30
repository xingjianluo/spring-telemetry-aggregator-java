package com.github.xingjianluo.springtelemetryaggregatorjava.aggregation;

/**
 * 单个窗口的聚合结果快照。
 */
public record WindowStats(
        long windowStartMillis,
        long windowSizeMillis,
        long acceptedCount,
        long duplicateCount,
        long conflictCount,
        long lateFallbackCount,
        long rejectedCount,
        long spanCount,
        double totalDurationMillis,
        double maxDurationMillis,
        double minDurationMillis,
        long metricCount,
        double metricValueSum) {

    public double avgDurationMillis() {
        return spanCount == 0 ? 0.0 : totalDurationMillis / spanCount;
    }
}
