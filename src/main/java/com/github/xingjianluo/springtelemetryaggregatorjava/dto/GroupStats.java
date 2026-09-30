package com.github.xingjianluo.springtelemetryaggregatorjava.dto;

/**
 * 单个属性维度组合下的聚合值。
 */
public record GroupStats(
        long count,
        long durationNanosSum,
        long durationNanosMin,
        long durationNanosMax,
        Double durationNanosAvg
) {
}
