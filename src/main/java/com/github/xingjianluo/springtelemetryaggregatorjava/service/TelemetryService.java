package com.github.xingjianluo.springtelemetryaggregatorjava.service;

import com.github.xingjianluo.springtelemetryaggregatorjava.dto.AggregationView;
import com.github.xingjianluo.springtelemetryaggregatorjava.dto.BatchReportResponse;
import com.github.xingjianluo.springtelemetryaggregatorjava.dto.IngestItemResult;
import com.github.xingjianluo.springtelemetryaggregatorjava.dto.TelemetryReportRequest;
import com.github.xingjianluo.springtelemetryaggregatorjava.ingest.NormalizeOutcome;
import com.github.xingjianluo.springtelemetryaggregatorjava.ingest.RecordNormalizer;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.IngestStatus;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.RejectedEvent;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.RejectReason;
import com.github.xingjianluo.springtelemetryaggregatorjava.window.WindowManager;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * 上报与查询的应用服务：串联归一与窗口管理，组装对外响应。
 * 逐条处理、逐条给出可区分结论，不因单条异常影响同批其他记录。
 */
@Service
public class TelemetryService {

    private final RecordNormalizer normalizer;
    private final WindowManager windowManager;

    public TelemetryService(RecordNormalizer normalizer, WindowManager windowManager) {
        this.normalizer = normalizer;
        this.windowManager = windowManager;
    }

    public BatchReportResponse report(List<TelemetryReportRequest> requests) {
        List<TelemetryReportRequest> items = requests == null ? List.of() : requests;
        List<IngestItemResult> results = new ArrayList<>(items.size());

        int accepted = 0;
        int duplicate = 0;
        int reroutedLate = 0;
        int rejectedFormat = 0;
        int rejectedConflict = 0;
        int rejectedLimit = 0;

        for (TelemetryReportRequest req : items) {
            NormalizeOutcome outcome = normalizer.normalize(req);
            if (!outcome.ok()) {
                // 格式问题（字段缺失/非法时间戳/未知类型等）
                RejectReason reason = parseReason(outcome.reasonCode());
                String id = req == null ? null : req.getId();
                results.add(new IngestItemResult(id, IngestStatus.REJECTED_FORMAT,
                        reason, outcome.detail(), null));
                rejectedFormat++;
                continue;
            }

            WindowManager.SubmitResult sr = windowManager.submit(outcome.record());
            results.add(new IngestItemResult(outcome.record().id(), sr.status(),
                    sr.reason(), sr.detail(), sr.windowStart()));
            switch (sr.status()) {
                case ACCEPTED -> accepted++;
                case DUPLICATE -> duplicate++;
                case REROUTED_LATE -> reroutedLate++;
                case REJECTED_CONFLICT -> rejectedConflict++;
                case REJECTED_LIMIT -> rejectedLimit++;
                default -> { /* REJECTED_FORMAT 不会由窗口层产生 */ }
            }
        }

        return new BatchReportResponse(items.size(), accepted, duplicate, reroutedLate,
                rejectedFormat, rejectedConflict, rejectedLimit, results);
    }

    public List<AggregationView> query(long from, long to, List<String> groupBy) {
        if (from < 0 || to <= from) {
            throw new IllegalArgumentException("查询区间非法，要求 0 <= from < to，实际 from=" + from + ", to=" + to);
        }
        return windowManager.query(from, to, groupBy).values().stream()
                .map(s -> new AggregationView(s.start(), s.end(), s.watermark(),
                        s.closed(), s.totalCount(), s.groups()))
                .toList();
    }

    public List<RejectedEvent> rejections(int limit) {
        int capped = Math.max(1, Math.min(limit, 1000));
        return windowManager.recentRejections(capped);
    }

    private static RejectReason parseReason(String code) {
        try {
            return RejectReason.valueOf(code);
        } catch (IllegalArgumentException e) {
            return RejectReason.MALFORMED_PAYLOAD;
        }
    }
}
