package com.github.xingjianluo.springtelemetryaggregatorjava.tests;

import com.github.xingjianluo.springtelemetryaggregatorjava.config.AggregatorProperties;
import com.github.xingjianluo.springtelemetryaggregatorjava.dto.GroupStats;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.IngestStatus;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.RecordKind;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.RejectReason;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.TelemetryRecord;
import com.github.xingjianluo.springtelemetryaggregatorjava.window.WindowManager;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 窗口规则：固定窗口对齐、乱序到达、重复幂等、语义冲突、迟到兜底、维度/容量限制。
 */
class WindowManagerTest {

    private static final Logger log = LoggerFactory.getLogger(WindowManagerTest.class);
    private static final long W = 1_000L;
    private static final long LATENESS = 2_000L;

    private WindowManager manager() {
        AggregatorProperties p = new AggregatorProperties();
        p.setWindowMillis(W);
        p.setAllowedLatenessMillis(LATENESS);
        return new WindowManager(p);
    }

    private static TelemetryRecord metric(String id, long ts, Long dur, Map<String, String> attrs) {
        Map<String, String> a = attrs == null ? Map.of() : attrs;
        String fp = id + "|" + ts + "|" + dur + "|" + new java.util.TreeMap<>(a);
        return new TelemetryRecord(id, RecordKind.METRIC, ts, dur, null, null, Map.copyOf(a), fp);
    }

    private static TelemetryRecord metric(String id, long ts, long dur) {
        return metric(id, ts, dur, Map.of("service", "svc-a"));
    }

    @Test
    void outOfOrderEventsAggregateByCorrectWindow() {
        WindowManager m = manager();
        // 故意乱序：11500,10000,11000,10099 -> 窗口[10000,11000) 2条，[11000,12000) 2条
        long[] tss = {11_500, 10_000, 11_000, 10_099};
        int i = 0;
        for (long ts : tss) {
            WindowManager.SubmitResult r = m.submit(metric("e" + (i++), ts, 100));
            log.info("[乱序] 输入窗口={} 事件ts={} -> 判定={} 依据={}",
                    r.windowStart(), ts, r.status(), r.detail());
            assertEquals(IngestStatus.ACCEPTED, r.status());
        }
        Map<Long, WindowManager.WindowSnapshot> views = m.query(10_000, 12_000, List.of());
        assertEquals(2, views.size());
        assertEquals(2L, views.get(10_000L).totalCount());
        assertEquals(2L, views.get(11_000L).totalCount());
        log.info("[乱序] 最终聚合：窗口10000 count={}, 窗口11000 count={}, 水位线={}",
                views.get(10_000L).totalCount(), views.get(11_000L).totalCount(), m.currentWatermark());
    }

    @Test
    void duplicateSubmissionDoesNotInflateStats() {
        WindowManager m = manager();
        TelemetryRecord r = metric("dup-1", 10_000, 100);
        assertEquals(IngestStatus.ACCEPTED, m.submit(r).status());
        WindowManager.SubmitResult again = m.submit(metric("dup-1", 10_000, 100));
        assertEquals(IngestStatus.DUPLICATE, again.status());

        WindowManager.WindowSnapshot s = m.query(10_000, 11_000, List.of()).get(10_000L);
        GroupStats g = s.groups().get("");
        assertEquals(1L, g.count());
        assertEquals(100L, g.durationNanosSum());
        log.info("[重复] 输入窗口=10000 同一份数据提交2次 -> 判定=DUPLICATE 依据={}; 最终 count={} sum={}",
                again.detail(), g.count(), g.durationNanosSum());
    }

    @Test
    void sameIdDifferentContentIsConflictNotDuplicate() {
        WindowManager m = manager();
        assertEquals(IngestStatus.ACCEPTED, m.submit(metric("c1", 10_000, 100)).status());
        WindowManager.SubmitResult conflict = m.submit(metric("c1", 10_000, 200));
        assertEquals(IngestStatus.REJECTED_CONFLICT, conflict.status());
        assertEquals(RejectReason.ID_CONFLICT, conflict.reason());
        assertEquals(1L, m.query(10_000, 11_000, List.of()).get(10_000L).totalCount());
        log.info("[冲突] 相同id不同耗时 -> 判定=REJECTED_CONFLICT 依据={}; 窗口count保持=1", conflict.detail());
    }

    @Test
    void tooLateEventsAreReroutedNotMerged() {
        WindowManager m = manager();
        // 先推进水位线到 20000-2000=18000
        assertEquals(IngestStatus.ACCEPTED, m.submit(metric("future", 20_000, 1)).status());
        assertEquals(18_000L, m.currentWatermark());

        // ts=17500 属窗口[17000,18000)，窗口结束时刻 18000 <= 水位线18000 => 过晚
        WindowManager.SubmitResult late = m.submit(metric("late-1", 17_500, 50));
        assertEquals(IngestStatus.REROUTED_LATE, late.status());
        assertEquals(RejectReason.TOO_LATE, late.reason());

        // 边界：ts=18000 属窗口[18000,19000)，结束时刻 19000 > 18000 => 仍在可接受迟到范围
        WindowManager.SubmitResult onEdge = m.submit(metric("edge-1", 18_000, 60));
        assertEquals(IngestStatus.ACCEPTED, onEdge.status());

        // 迟到数据重复提交仍是 DUPLICATE，且不进兜底两次
        assertEquals(IngestStatus.DUPLICATE, m.submit(metric("late-1", 17_500, 50)).status());

        assertNull(m.query(17_000, 18_000, List.of()).get(17_000L), "过晚数据不得并入任何窗口");
        assertEquals(1, m.lateSinkSize());
        String latestRejection = latest(m);
        log.info("[迟到] 输入窗口=17000 ts=17500 -> 判定=REROUTED_LATE 依据={}; 兜底事件原因={}, 边界ts=18000判定=ACCEPTED",
                late.detail(), latestRejection);
        assertEquals(RejectReason.TOO_LATE, m.recentRejections(10).get(0).reason());
    }

    @Test
    void dimensionCardinalityIsBoundedWithClearReason() {
        AggregatorProperties p = new AggregatorProperties();
        p.setWindowMillis(W);
        p.setAllowedLatenessMillis(LATENESS);
        p.setMaxDimensionsPerWindow(4);
        WindowManager m = new WindowManager(p);

        // 每条记录自带 service 维度；第1条 service=svc-a，第2条新增 service=svc-b（共2维度），
        // 第3条 service=svc-c 将达到3维仍接受；为精确控制，这里直接用单属性记录
        TelemetryRecord r1 = metric("d1", 10_000, null, Map.of("k", "v1"));
        TelemetryRecord r2 = metric("d2", 10_000, null, Map.of("k", "v2"));
        TelemetryRecord r3 = metric("d3", 10_000, null, Map.of("k", "v3"));
        TelemetryRecord r4 = metric("d4", 10_000, null, Map.of("k", "v4"));
        TelemetryRecord r5 = metric("d5", 10_000, null, Map.of("k", "v5"));
        assertEquals(IngestStatus.ACCEPTED, m.submit(r1).status());
        assertEquals(IngestStatus.ACCEPTED, m.submit(r2).status());
        assertEquals(IngestStatus.ACCEPTED, m.submit(r3).status());
        assertEquals(IngestStatus.ACCEPTED, m.submit(r4).status());
        WindowManager.SubmitResult rejected = m.submit(r5);
        assertEquals(IngestStatus.REJECTED_LIMIT, rejected.status());
        assertEquals(RejectReason.TOO_MANY_DIMENSIONS, rejected.reason());
        log.info("[维度] 输入窗口=10000 第5个不同维度值 -> 判定=REJECTED_LIMIT 依据={}", rejected.detail());
    }

    @Test
    void windowCapacityIsBoundedWithClearReason() {
        AggregatorProperties p = new AggregatorProperties();
        p.setWindowMillis(W);
        p.setAllowedLatenessMillis(LATENESS);
        p.setMaxRecordsPerWindow(2);
        WindowManager m = new WindowManager(p);
        m.submit(metric("w1", 10_000, 1));
        m.submit(metric("w2", 10_000, 1));
        WindowManager.SubmitResult full = m.submit(metric("w3", 10_000, 1));
        assertEquals(IngestStatus.REJECTED_LIMIT, full.status());
        assertEquals(RejectReason.WINDOW_FULL, full.reason());
        log.info("[容量] 输入窗口=10000 第3条 -> 判定=REJECTED_LIMIT 依据={}", full.detail());
    }

    @Test
    void groupByDimensionsProducesExplainableStats() {
        WindowManager m = manager();
        m.submit(metric("g1", 10_000, 100L, Map.of("service", "a")));
        m.submit(metric("g2", 10_000, 300L, Map.of("service", "a")));
        m.submit(metric("g3", 10_000, 50L, Map.of("service", "b")));
        WindowManager.WindowSnapshot s = m.query(10_000, 11_000, List.of("service")).get(10_000L);
        GroupStats a = s.groups().get("service=a");
        GroupStats b = s.groups().get("service=b");
        assertEquals(2L, a.count());
        assertEquals(400L, a.durationNanosSum());
        assertEquals(100L, a.durationNanosMin());
        assertEquals(300L, a.durationNanosMax());
        assertEquals(200.0, a.durationNanosAvg(), 1e-9);
        assertEquals(1L, b.count());
        log.info("[分组] 输入窗口=10000 groupBy=service -> 最终聚合 service:a={}, service:b={}", a, b);
    }

    private static String latest(WindowManager m) {
        return m.recentRejections(1).get(0).reason() + "/" + m.recentRejections(1).get(0).status();
    }
}
