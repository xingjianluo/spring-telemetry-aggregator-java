package com.github.xingjianluo.springtelemetryaggregatorjava.model;

/**
 * 单条记录的处理结论，保证重复、乱序、异常数据都有可区分的稳定结果。
 */
public enum IngestStatus {
    /** 正常接收入窗 */
    ACCEPTED,
    /** 同 recordId 同内容重复提交，不抬高统计 */
    DUPLICATE,
    /** 同 recordId 但内容不一致，语义冲突 */
    CONFLICT,
    /** 字段缺失 / 时间戳非法等格式问题 */
    REJECTED_FORMAT,
    /** 超出可接受迟到范围，落入兜底计数，不并入当前窗口 */
    LATE_FALLBACK,
    /** 触发资源约束（维度膨胀 / 单窗口数据量过大） */
    REJECTED_LIMIT
}
