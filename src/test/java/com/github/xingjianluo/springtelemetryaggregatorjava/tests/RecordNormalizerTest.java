package com.github.xingjianluo.springtelemetryaggregatorjava.tests;

import com.github.xingjianluo.springtelemetryaggregatorjava.dto.TelemetryReportRequest;
import com.github.xingjianluo.springtelemetryaggregatorjava.ingest.NormalizeOutcome;
import com.github.xingjianluo.springtelemetryaggregatorjava.ingest.RecordNormalizer;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.RejectReason;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 归一与格式校验：字段缺失 / 时间戳非法 / 类型未知必须与语义冲突区分开。
 */
class RecordNormalizerTest {

    private static final Logger log = LoggerFactory.getLogger(RecordNormalizerTest.class);

    private final RecordNormalizer normalizer = new RecordNormalizer();

    @Test
    void validTraceAndMetricNormalizeIntoSameModel() {
        TelemetryReportRequest trace = req("t1", "TRACE", 1_700_000_000_000L, 1500L);
        trace.setTraceId("tr-1");
        trace.setSpanId("sp-1");
        NormalizeOutcome o1 = normalizer.normalize(trace);
        TelemetryReportRequest metric = req("m1", "metric", 1_700_000_000_500L, 12L);
        NormalizeOutcome o2 = normalizer.normalize(metric);

        assertTrue(o1.ok() && o2.ok());
        // 小写 kind 归一为枚举；traceId/spanId 并入同一套属性维度
        assertEquals("TRACE", o1.record().kind().name());
        assertEquals("METRIC", o2.record().kind().name());
        assertEquals("tr-1", o1.record().attributes().get("traceId"));
        assertNotNull(o1.record().fingerprint());
        log.info("[归一] 输入 TRACE(traceId=tr-1,spanId=sp-1) 与小写 metric -> 判定: 均归一成功，"
                + "共用内部模型; trace指纹={}, metric指纹={}", o1.record().fingerprint(), o2.record().fingerprint());
    }

    @Test
    void formatProblemsAreDistinguished() {
        checkFormat(req(" ", "TRACE", 1L, null), RejectReason.MISSING_ID);
        checkFormat(req("x", null, 1L, null), RejectReason.MISSING_KIND);
        checkFormat(req("x", "SPAN", 1L, null), RejectReason.UNKNOWN_KIND);
        checkFormat(req("x", "TRACE", null, null), RejectReason.INVALID_TIMESTAMP);
        checkFormat(req("x", "TRACE", 0L, null), RejectReason.INVALID_TIMESTAMP);
        checkFormat(req("x", "TRACE", 1_700_000_000_000L, -1L), RejectReason.INVALID_DURATION);
        checkFormat(null, RejectReason.MALFORMED_PAYLOAD);
    }

    @Test
    void fingerprintChangesWhenContentChanges() {
        TelemetryReportRequest a = req("same-id", "TRACE", 1_700_000_000_000L, 10L);
        TelemetryReportRequest b = req("same-id", "TRACE", 1_700_000_000_000L, 11L);
        String fa = normalizer.normalize(a).record().fingerprint();
        String fb = normalizer.normalize(b).record().fingerprint();
        assertNotEquals(fa, fb, "耗时不同的同 id 记录指纹必须不同，供窗口层判语义冲突");
        log.info("[指纹] 相同id不同耗时 -> 指纹A={} 指纹B={} 判定: 可区分语义冲突", fa, fb);
    }

    private void checkFormat(TelemetryReportRequest r, RejectReason expected) {
        NormalizeOutcome o = normalizer.normalize(r);
        assertFalse(o.ok(), "应当归一失败");
        assertEquals(expected.name(), o.reasonCode());
        log.info("[格式校验] 输入 kind={}, ts={}, dur={}, id={} -> 判定依据={} 说明={}",
                r == null ? null : r.getKind(),
                r == null ? null : r.getTimestamp(),
                r == null ? null : r.getDurationNanos(),
                r == null ? null : r.getId(),
                o.reasonCode(), o.detail());
    }

    private static TelemetryReportRequest req(String id, String kind, Long ts, Long dur) {
        TelemetryReportRequest r = new TelemetryReportRequest();
        r.setId(id);
        r.setKind(kind);
        r.setTimestamp(ts);
        r.setDurationNanos(dur);
        r.setName("op");
        r.setService("svc-a");
        r.setAttributes(Map.of("region", "cn"));
        return r;
    }
}
