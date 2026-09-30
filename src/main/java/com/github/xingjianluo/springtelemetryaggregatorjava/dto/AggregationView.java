package com.github.xingjianluo.springtelemetryaggregatorjava.dto;

import java.util.Map;

/**
 * 某时间窗口的聚合结果：窗口边界、水位线、按属性分组的统计。
 */
public record AggregationView(
        long windowStart,
        long windowEnd,
        long watermark,
        boolean closed,
        long totalCount,
        Map<String, GroupStats> groups
) {
}
