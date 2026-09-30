package com.github.xingjianluo.springtelemetryaggregatorjava.tests;

import com.github.xingjianluo.springtelemetryaggregatorjava.aggregation.WindowAggregator;
import com.github.xingjianluo.springtelemetryaggregatorjava.aggregation.WindowStats;
import com.github.xingjianluo.springtelemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.springtelemetryaggregatorjava.ingest.IngestRequest;
import com.github.xingjianluo.springtelemetryaggregatorjava.ingest.IngestionService;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.IngestStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 本地构造数据验证：乱序、重复、冲突、格式、迟到、限制与并发组合场景。
 * 每个用例打印输入窗口、判定依据与最终聚合值。
 */
class WindowAggregationTests {

    private static final Logger log = LoggerFactory.getLogger(WindowAggregationTests.class);

    private static final long WINDOW = 10_000L;
    private static final long MAX_LATENESS = 5_000L;
    private static final long NOW = 1_005_000L; // 位于窗口 [1_000_000, 1_010_000) 中点
    private static final long CURRENT_WINDOW_START = NOW - (NOW % WINDOW); // 1_000_000

    private TelemetryProperties properties;
    private WindowAggregator aggregator;
    private IngestionService service;

    @BeforeEach
    void setUp() {
        properties = new TelemetryProperties();
        properties.setWindowSizeMillis(WINDOW);
        properties.setMaxLatenessMillis(MAX_LATENESS);
        properties.setMaxAttributeKeys(4);
        properties.setMaxRecordsPerWindow(10_000);
        aggregator = new WindowAggregator(properties);
        service = new IngestionService(aggregator);
    }

    private IngestRequest span(String id, long ts, long durationMs) {
        return new IngestRequest(id, "SPAN", "op-" + id, ts, durationMs, null, Map.of("svc", "a"));
    }

    private IngestRequest metric(String id, long ts, double value) {
        return new IngestRequest(id, "METRIC", "m-" + id, ts, null, value, Map.of("svc", "a"));
    }

    private void logVerdict(String caseName, IngestRequest input, IngestResult result) {
        long windowStart = input.timestampMillis() == null ? -1 : aggregator.windowStartOf(input.timestampMillis());
        log.info("[{}] 输入 recordId={} ts={} 窗口=[{}, {}) -> 判定={} 依据={}",
                caseName, input.recordId(), input.timestampMillis(),
                windowStart, windowStart + WINDOW, result.status(), result.reason());
    }

    private void logStats(String caseName, long windowStart) {
        WindowStats s = aggregator.snapshot(windowStart);
        log.info("[{}] 窗口 {} 最终聚合: accepted={} duplicate={} conflict={} lateFallback={} rejected={} "
                        + "spanCount={} totalDuration={} avgDuration={} minDuration={} maxDuration={} "
                        + "metricCount={} metricValueSum={}",
                caseName, windowStart, s.acceptedCount(), s.duplicateCount(), s.conflictCount(),
                s.lateFallbackCount(), s.rejectedCount(), s.spanCount(), s.totalDurationMillis(),
                s.avgDurationMillis(), s.minDurationMillis(), s.maxDurationMillis(),
                s.metricCount(), s.metricValueSum());
    }

    @Test
    void outOfOrderAndDuplicateDoNotInflateStats() {
        // 乱序：同一窗口内时间戳乱序到达；重复：r1/r2 原样重发
        List<IngestRequest> batch = List.of(
                span("r3", NOW - 1_000, 300),
                span("r1", NOW - 3_000, 100),
                span("r2", NOW - 2_000, 200),
                span("r1", NOW - 3_000, 100),   // 重复
                span("r2", NOW - 2_000, 200),   // 重复
                metric("m1", NOW - 500, 1.5),
                metric("m1", NOW - 500, 1.5));  // 重复
        List<IngestResult> results = service.ingest(batch, NOW);
        for (int i = 0; i < batch.size(); i++) {
            logVerdict("乱序+重复", batch.get(i), results.get(i));
        }
        assertEquals(IngestStatus.ACCEPTED, results.get(0).status());
        assertEquals(IngestStatus.ACCEPTED, results.get(1).status());
        assertEquals(IngestStatus.ACCEPTED, results.get(2).status());
        assertEquals(IngestStatus.DUPLICATE, results.get(3).status());
        assertEquals(IngestStatus.DUPLICATE, results.get(4).status());
        assertEquals(IngestStatus.ACCEPTED, results.get(5).status());
        assertEquals(IngestStatus.DUPLICATE, results.get(6).status());

        WindowStats s = aggregator.snapshot(CURRENT_WINDOW_START);
        logStats("乱序+重复", CURRENT_WINDOW_START);
        assertEquals(4, s.acceptedCount());
        assertEquals(3, s.duplicateCount());
        assertEquals(3, s.spanCount());
        assertEquals(600.0, s.totalDurationMillis());
        assertEquals(200.0, s.avgDurationMillis());
        assertEquals(100.0, s.minDurationMillis());
        assertEquals(300.0, s.maxDurationMillis());
        assertEquals(1, s.metricCount());
        assertEquals(1.5, s.metricValueSum());
    }

    @Test
    void sameIdDifferentContentIsConflictNotDuplicate() {
        List<IngestResult> results = service.ingest(List.of(
                span("r1", NOW - 1_000, 100),
                span("r1", NOW - 1_000, 999)), // 同 ID 不同内容
                NOW);
        logVerdict("冲突", span("r1", NOW - 1_000, 100), results.get(0));
        logVerdict("冲突", span("r1", NOW - 1_000, 999), results.get(1));
        assertEquals(IngestStatus.ACCEPTED, results.get(0).status());
        assertEquals(IngestStatus.CONFLICT, results.get(1).status());

        WindowStats s = aggregator.snapshot(CURRENT_WINDOW_START);
        logStats("冲突", CURRENT_WINDOW_START);
        assertEquals(1, s.acceptedCount());
        assertEquals(1, s.conflictCount());
        assertEquals(100.0, s.totalDurationMillis()); // 冲突记录不并入统计
    }

    @Test
    void formatProblemsAreRejectedWithDistinguishableReasons() {
        List<IngestRequest> batch = List.of(
                new IngestRequest(null, "SPAN", "op", NOW, 1L, null, null),       // 缺 recordId
                new IngestRequest("r1", "TRACE", "op", NOW, 1L, null, null),      // 非法 kind
                new IngestRequest("r2", "SPAN", "op", -5L, 1L, null, null),       // 非法时间戳
                new IngestRequest("r3", "SPAN", "op", NOW, null, null, null),     // SPAN 缺时长
                new IngestRequest("r4", "METRIC", "m", NOW, null, null, null));   // METRIC 缺值
        List<IngestResult> results = service.ingest(batch, NOW);
        for (int i = 0; i < batch.size(); i++) {
            logVerdict("格式", batch.get(i), results.get(i));
            assertEquals(IngestStatus.REJECTED_FORMAT, results.get(i).status());
            assertFalse(results.get(i).reason().isBlank());
        }
        // 每条原因可区分
        assertTrue(results.get(0).reason().contains("recordId"));
        assertTrue(results.get(1).reason().contains("kind"));
        assertTrue(results.get(2).reason().contains("timestamp"));
        assertTrue(results.get(3).reason().contains("duration"));
        assertTrue(results.get(4).reason().contains("value"));
        logStats("格式", CURRENT_WINDOW_START);
        assertEquals(0, aggregator.snapshot(CURRENT_WINDOW_START).acceptedCount());
    }

    @Test
    void lateDataFallsBackInsteadOfMergingIntoCurrentWindow() {
        long lateTs = NOW - MAX_LATENESS - 1; // 超出可接受迟到范围
        IngestRequest late = span("late1", lateTs, 500);
        IngestResult result = service.ingest(List.of(late), NOW).get(0);
        logVerdict("迟到", late, result);
        assertEquals(IngestStatus.LATE_FALLBACK, result.status());

        long lateWindow = aggregator.windowStartOf(lateTs);
        log.info("[迟到] 迟到记录属于窗口 {}，兜底计数落在该窗口，不并入当前窗口 {}", lateWindow, CURRENT_WINDOW_START);
        WindowStats lateStats = aggregator.snapshot(lateWindow);
        logStats("迟到", lateWindow);
        assertEquals(1, lateStats.lateFallbackCount());
        assertEquals(0, lateStats.acceptedCount());
        assertEquals(0.0, lateStats.totalDurationMillis()); // 耗时不并入任何窗口统计
        assertEquals(0, aggregator.snapshot(CURRENT_WINDOW_START).acceptedCount());

        // 迟到记录重复上报仍是 DUPLICATE，结论稳定
        IngestResult again = service.ingest(List.of(late), NOW).get(0);
        logVerdict("迟到-重发", late, again);
        assertEquals(IngestStatus.DUPLICATE, again.status());
    }

    @Test
    void limitRejectionsCarrySearchableReasons() {
        // 维度膨胀：属性键超限
        IngestRequest fat = new IngestRequest("fat1", "SPAN", "op", NOW, 1L, null,
                Map.of("k1", "v", "k2", "v", "k3", "v", "k4", "v", "k5", "v"));
        IngestResult r1 = service.ingest(List.of(fat), NOW).get(0);
        logVerdict("维度限制", fat, r1);
        assertEquals(IngestStatus.REJECTED_LIMIT, r1.status());
        assertTrue(r1.reason().contains("attribute keys"));

        // 单窗口容量超限
        properties.setMaxRecordsPerWindow(2);
        WindowAggregator small = new WindowAggregator(properties);
        IngestionService smallService = new IngestionService(small);
        List<IngestResult> results = smallService.ingest(List.of(
                span("c1", NOW - 100, 1),
                span("c2", NOW - 100, 1),
                span("c3", NOW - 100, 1)), NOW);
        for (IngestResult r : results) {
            log.info("[容量限制] 判定={} 依据={}", r.status(), r.reason());
        }
        assertEquals(IngestStatus.ACCEPTED, results.get(0).status());
        assertEquals(IngestStatus.ACCEPTED, results.get(1).status());
        assertEquals(IngestStatus.REJECTED_LIMIT, results.get(2).status());
        assertTrue(results.get(2).reason().contains("max records"));
    }

    @Test
    void concurrentIngestMatchesSequentialResult() throws InterruptedException {
        int threads = 8;
        int perThread = 250; // 每线程 250 条唯一记录 + 50 条重复
        int duplicatesPerThread = 50;

        // 构造确定性数据集：线程 t 的记录 id 为 "t-{t}-r-{i}"
        List<IngestRequest> dataset = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            for (int i = 0; i < perThread; i++) {
                dataset.add(span("t-" + t + "-r-" + i, NOW - 1_000 - (i % 4_000), i % 100));
            }
            for (int i = 0; i < duplicatesPerThread; i++) {
                dataset.add(span("t-" + t + "-r-" + i, NOW - 1_000 - (i % 4_000), i % 100)); // 重复
            }
        }

        // 顺序基线
        WindowAggregator seqAggregator = new WindowAggregator(properties);
        IngestionService seqService = new IngestionService(seqAggregator);
        seqService.ingest(dataset, NOW);
        WindowStats expected = seqAggregator.snapshot(CURRENT_WINDOW_START);
        logStats("并发-顺序基线", CURRENT_WINDOW_START);

        // 并发执行：同一数据集按线程分片并行写入
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            final int thread = t;
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                List<IngestRequest> slice = new ArrayList<>();
                for (int i = 0; i < perThread; i++) {
                    slice.add(span("t-" + thread + "-r-" + i, NOW - 1_000 - (i % 4_000), i % 100));
                }
                for (int i = 0; i < duplicatesPerThread; i++) {
                    slice.add(span("t-" + thread + "-r-" + i, NOW - 1_000 - (i % 4_000), i % 100));
                }
                service.ingest(slice, NOW);
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        WindowStats actual = aggregator.snapshot(CURRENT_WINDOW_START);
        logStats("并发-实际", CURRENT_WINDOW_START);
        assertEquals(expected, actual, "并发写入同一窗口的结果必须与逐个处理一致");
        assertEquals(threads * perThread, actual.acceptedCount());
        assertEquals(threads * duplicatesPerThread, actual.duplicateCount());
    }
}
