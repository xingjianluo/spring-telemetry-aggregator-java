package com.github.xingjianluo.springtelemetryaggregatorjava.dto;

import java.util.List;

/**
 * 批量上报响应：逐条给出可区分的结论与汇总计数。
 */
public record BatchReportResponse(
        int total,
        int accepted,
        int duplicate,
        int reroutedLate,
        int rejectedFormat,
        int rejectedConflict,
        int rejectedLimit,
        List<IngestItemResult> results
) {
}
