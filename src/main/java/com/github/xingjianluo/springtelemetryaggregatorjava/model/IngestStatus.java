package com.github.xingjianluo.springtelemetryaggregatorjava.model;

/**
 * 单条上报的处理结论，彼此可区分：
 * ACCEPTED 正常入窗；DUPLICATE 幂等重复；REROUTED_LATE 过晚数据兜底；
 * REJECTED_FORMAT 格式问题；REJECTED_CONFLICT 语义冲突；REJECTED_LIMIT 资源限制。
 */
public enum IngestStatus {
    ACCEPTED,
    DUPLICATE,
    REROUTED_LATE,
    REJECTED_FORMAT,
    REJECTED_CONFLICT,
    REJECTED_LIMIT
}
