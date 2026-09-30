package com.github.xingjianluo.springtelemetryaggregatorjava.web;

import com.github.xingjianluo.springtelemetryaggregatorjava.dto.AggregationView;
import com.github.xingjianluo.springtelemetryaggregatorjava.dto.BatchReportResponse;
import com.github.xingjianluo.springtelemetryaggregatorjava.dto.TelemetryReportRequest;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.RejectedEvent;
import com.github.xingjianluo.springtelemetryaggregatorjava.service.TelemetryService;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 统一入口：批量上报、窗口聚合查询、拒绝事件排查。
 */
@RestController
@RequestMapping("/api/telemetry")
public class TelemetryController {

    private final TelemetryService service;

    public TelemetryController(TelemetryService service) {
        this.service = service;
    }

    @PostMapping("/reports")
    public BatchReportResponse report(@RequestBody Map<String, List<TelemetryReportRequest>> body) {
        return service.report(body == null ? List.of() : body.getOrDefault("records", List.of()));
    }

    @GetMapping("/aggregations")
    public List<AggregationView> aggregations(@RequestParam long from,
                                              @RequestParam long to,
                                              @RequestParam(required = false) List<String> groupBy) {
        return service.query(from, to, groupBy);
    }

    @GetMapping("/rejections")
    public List<RejectedEvent> rejections(@RequestParam(defaultValue = "100") int limit) {
        return service.rejections(limit);
    }
}
