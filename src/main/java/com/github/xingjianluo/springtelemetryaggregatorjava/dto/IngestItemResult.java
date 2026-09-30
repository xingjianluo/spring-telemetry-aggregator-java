package com.github.xingjianluo.springtelemetryaggregatorjava.dto;

import com.github.xingjianluo.springtelemetryaggregatorjava.model.IngestStatus;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.RejectReason;

/**
 * 单条上报的处理结果。
 *
 * @param id      记录 id（缺失时可能为 null）
 * @param status  处理结论
 * @param reason  判定原因（ACCEPTED/DUPLICATE 可为 null）
 * @param detail  可解释的判定说明
 * @param windowStart 命中窗口起点（epoch 毫秒，未入窗时为 null）
 */
public record IngestItemResult(
        String id,
        IngestStatus status,
        RejectReason reason,
        String detail,
        Long windowStart
) {
}
