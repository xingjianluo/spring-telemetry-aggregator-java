package com.github.xingjianluo.springtelemetryaggregatorjava.model;

/**
 * 被拒绝或兜底处理的事件留痕，用于排查拒绝原因。
 *
 * @param receivedAtMillis 服务端接收时间
 * @param eventTimestamp   原始事件时间（非法时为 null）
 * @param windowStart      期望落入的窗口起点（无法计算时为 null）
 * @param id               记录 id
 * @param status           处理结论
 * @param reason           判定原因
 * @param detail           判定说明
 */
public record RejectedEvent(
        long receivedAtMillis,
        Long eventTimestamp,
        Long windowStart,
        String id,
        IngestStatus status,
        RejectReason reason,
        String detail
) {
}
