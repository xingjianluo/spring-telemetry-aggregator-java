package com.github.xingjianluo.springtelemetryaggregatorjava.tests;

import com.github.xingjianluo.springtelemetryaggregatorjava.config.AggregatorProperties;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 并发组合：多线程同时写入同一窗口，混入重复、冲突、乱序、迟到与容量上限，
 * 最终计数必须与逐个串行处理一致。
 */
class ConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyTest.class);

    private static TelemetryRecord rec(String id, long ts, Long dur, Map<String, String> attrs) {
        Map<String, String> a = attrs == null ? Map.of() : attrs;
        String fp = id + "|" + ts + "|" + dur + "|" + new java.util.TreeMap<>(a);
        return new TelemetryRecord(id, RecordKind.METRIC, ts, dur, null, null, Map.copyOf(a), fp);
    }

    @Test
    void concurrentUniqueAndDuplicateWritesMatchSerialResult() throws InterruptedException {
        AggregatorProperties p = new AggregatorProperties();
        p.setWindowMillis(10_000);
        p.setAllowedLatenessMillis(60_000);
        WindowManager m = new WindowManager(p);

        int unique = 300;
        int threads = 10;
        int rounds = 3; // 每条唯一记录被提交 3 次
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads * rounds);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicate = new AtomicInteger();

        for (int round = 0; round < rounds; round++) {
            for (int t = 0; t < threads; t++) {
                final int base = t;
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = base; i < unique; i += threads) {
                            long ts = 10_000 + (i % 3) * 1_000 + (i % 50); // 乱序但都落在前3个窗口
                            WindowManager.SubmitResult r =
                                    m.submit(rec("u-" + i, ts, 10L, Map.of("service", "svc")));
                            if (r.status() == IngestStatus.ACCEPTED) {
                                accepted.incrementAndGet();
                            } else if (r.status() == IngestStatus.DUPLICATE) {
                                duplicate.incrementAndGet();
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdown();

        // 串行等价结论：恰好 unique 条 ACCEPTED，其余 rounds*unique-unique 条 DUPLICATE
        assertEquals(unique, accepted.get(), "并发后接收数必须等于唯一记录数");
        assertEquals(unique * (rounds - 1), duplicate.get(), "重复提交数必须精确");
        long total = m.query(10_000, 13_000, List.of()).values().stream()
                .mapToLong(WindowManager.WindowSnapshot::totalCount).sum();
        assertEquals(unique, total, "窗口总计数不得被重复提交抬高");
        log.info("[并发去重] 输入窗口=10000~12000, {}线程 x {}轮, {}条唯一记录 -> "
                        + "判定 accepted={}, duplicate={}; 最终窗口总count={}",
                threads, rounds, unique, accepted.get(), duplicate.get(), total);
    }

    @Test
    void concurrentMixedDuplicateConflictLateAndLimitIsStable() throws InterruptedException {
        AggregatorProperties p = new AggregatorProperties();
        p.setWindowMillis(1_000);
        p.setAllowedLatenessMillis(2_000);
        p.setMaxRecordsPerWindow(50);
        WindowManager m = new WindowManager(p);

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        Map<IngestStatus, AtomicInteger> stats = new ConcurrentHashMap<>();
        for (IngestStatus s : IngestStatus.values()) {
            stats.put(s, new AtomicInteger());
        }

        // ---- 栅栏放开前先做确定性播种，使各结论计数与线程交错无关 ----
        // 1) 共享重复 id 先接收一次：之后所有线程的同内容提交都应是 DUPLICATE
        assertEquals(IngestStatus.ACCEPTED,
                m.submit(rec("shared-dup", 10_000, 1L, Map.of())).status());
        // 2) 共享冲突 id 以 ti=0 的指纹播种：ti=0 的提交变为 DUPLICATE，ti=1..7 全部 CONFLICT
        assertEquals(IngestStatus.ACCEPTED,
                m.submit(rec("shared-conflict", 10_000, 0L, Map.of())).status());
        // 3) 推进水位线到 9000-2000=7000（实际种子先把水位线推到8000）：
        //    关闭窗口[5000,6000)，同时保持窗口[10000,11000)开放，使两类结论互不干扰
        m.submit(rec("watermark-pump", 9_000, 1L, Map.of()));
        assertEquals(8_000L, m.currentWatermark());

        for (int t = 0; t < threads; t++) {
            final int ti = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < 60; i++) {
                        TelemetryRecord r;
                        if (i % 5 == 0) {
                            // 已播种的共享 id、完全相同内容 -> DUPLICATE
                            r = rec("shared-dup", 10_000, 1L, Map.of());
                        } else if (i % 7 == 0) {
                            // 已播种的共享 id：ti=0 指纹相同=DUPLICATE，其余=CONFLICT
                            r = rec("shared-conflict", 10_000, (long) ti, Map.of());
                        } else if (i % 11 == 0) {
                            // 窗口[5000,6000) 已被水位线关闭 -> 迟到兜底
                            r = rec("late-" + ti + "-" + i, 5_000, 1L, Map.of());
                        } else {
                            // 唯一记录冲击窗口[10000,11000)的容量上限50
                            r = rec("uniq-" + ti + "-" + i, 10_000, 1L, Map.of());
                        }
                        WindowManager.SubmitResult sr = m.submit(r);
                        stats.get(sr.status()).incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdown();

        long windowCount = m.query(10_000, 11_000, List.of()).get(10_000L).totalCount();

        // 每线程：i%5==0 共12条；i%7==0 且不与%5重叠 共7条；i%11==0 且不与前两者重叠 共4条；唯一37条
        // 重复：12*8 个 shared-dup + ti0 的 7 个 shared-conflict = 103
        // 冲突：ti1..7 各 7 个 = 49；迟到兜底：8*4 = 32；唯一 296 个争 48 个剩余容量
        log.info("[并发混合] 输入窗口=10000(容量50,已播种2) + 迟到窗口5000, {}线程 x 60条 -> "
                        + "判定统计 accepted={}, duplicate={}, conflict={}, reroutedLate={}, rejectedLimit={}; "
                        + "窗口10000最终count={}, 水位线={}, 迟到兜底={}",
                threads,
                stats.get(IngestStatus.ACCEPTED).get(),
                stats.get(IngestStatus.DUPLICATE).get(),
                stats.get(IngestStatus.REJECTED_CONFLICT).get(),
                stats.get(IngestStatus.REROUTED_LATE).get(),
                stats.get(IngestStatus.REJECTED_LIMIT).get(),
                windowCount, m.currentWatermark(), m.lateSinkSize());

        // 296 个唯一记录抢占剩余 48 容量：48 ACCEPTED + 248 WINDOW_FULL
        // 串行等价的确定性结论（与线程交错无关）
        assertEquals(48, acceptedCount(stats), "唯一记录恰好填满剩余容量48");
        assertEquals(248, stats.get(IngestStatus.REJECTED_LIMIT).get(), "超出容量的248条必须被拒绝且原因明确");
        assertEquals(103, stats.get(IngestStatus.DUPLICATE).get(), "重复提交计数精确");
        assertEquals(49, stats.get(IngestStatus.REJECTED_CONFLICT).get(), "同id不同指纹=语义冲突7x7");
        assertEquals(32, stats.get(IngestStatus.REROUTED_LATE).get(), "迟到数据全部兜底");
        assertEquals(32, m.lateSinkSize(), "兜底事件数与迟到数一致，重复迟到不会重复兜底");
        assertEquals(50, windowCount, "窗口最终计数恰为容量上限，与逐个串行处理一致");
        assertEquals(0, stats.get(IngestStatus.REJECTED_FORMAT).get());
    }

    private static int acceptedCount(Map<IngestStatus, AtomicInteger> stats) {
        return stats.get(IngestStatus.ACCEPTED).get();
    }
}
