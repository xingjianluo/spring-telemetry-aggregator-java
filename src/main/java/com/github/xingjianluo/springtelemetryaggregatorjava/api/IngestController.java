package com.github.xingjianluo.springtelemetryaggregatorjava.api;

import com.github.xingjianluo.springtelemetryaggregatorjava.aggregation.WindowAggregator;
import com.github.xingjianluo.springtelemetryaggregatorjava.aggregation.WindowStats;
import com.github.xingjianluo.springtelemetryaggregatorjava.ingest.IngestRequest;
import com.github.xingjianluo.springtelemetryaggregatorjava.ingest.IngestionService;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.IngestResult;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 上报与窗口查询 REST 入口。
 */
@RestController
@RequestMapping("/telemetry")
public class IngestController {

    private final IngestionService ingestionService;
    private final WindowAggregator aggregator;

    public IngestController(IngestionService ingestionService, WindowAggregator aggregator) {
        this.ingestionService = ingestionService;
        this.aggregator = aggregator;
    }

    @PostMapping("/ingest")
    public List<IngestResult> ingest(@RequestBody List<IngestRequest> batch) {
        return ingestionService.ingest(batch);
    }

    @GetMapping("/windows/{windowStartMillis}")
    public WindowStats window(@PathVariable long windowStartMillis) {
        return aggregator.snapshot(windowStartMillis);
    }
}
