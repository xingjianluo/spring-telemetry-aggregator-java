package com.github.xingjianluo.springtelemetryaggregatorjava.tests;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * 端到端：统一入口协议、逐条可区分结论、窗口聚合查询、拒绝留痕、非法请求体。
 */
@SpringBootTest
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class TelemetryControllerTest {

    private static final Logger log = LoggerFactory.getLogger(TelemetryControllerTest.class);

    @Autowired
    private MockMvc mockMvc;

    @Test
    void reportNormalizesTraceAndMetricAndGivesPerItemVerdict() throws Exception {
        String body = """
                {
                  "records": [
                    {"id":"t1","kind":"TRACE","timestamp":10000,"durationNanos":1200,
                     "traceId":"tr-1","spanId":"sp-1","service":"svc-a","attributes":{"region":"cn"}},
                    {"id":"m1","kind":"METRIC","timestamp":10300,"durationNanos":800,
                     "name":"cpu","service":"svc-a","attributes":{"region":"cn"}},
                    {"id":"t1","kind":"TRACE","timestamp":10000,"durationNanos":1200,
                     "traceId":"tr-1","spanId":"sp-1","service":"svc-a","attributes":{"region":"cn"}},
                    {"id":"bad1","kind":"TRACE"},
                    {"id":"bad2","kind":"WEIRD","timestamp":10000},
                    {"id":"cf1","kind":"METRIC","timestamp":10100,"durationNanos":10},
                    {"id":"cf1","kind":"METRIC","timestamp":10100,"durationNanos":11}
                  ]
                }
                """;
        MvcResult res = mockMvc.perform(post("/api/telemetry/reports")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(7))
                .andExpect(jsonPath("$.accepted").value(3))
                .andExpect(jsonPath("$.duplicate").value(1))
                .andExpect(jsonPath("$.rejectedFormat").value(2))
                .andExpect(jsonPath("$.rejectedConflict").value(1))
                .andReturn();
        log.info("[端到端上报] 输入窗口=10000 共7条(t1/m1/cf1接收3+t1重复1+格式坏2+cf1冲突1) -> 响应={}",
                res.getResponse().getContentAsString());

        // 聚合查询：按 region 分组，重复不抬高计数
        MvcResult agg = mockMvc.perform(get("/api/telemetry/aggregations")
                        .param("from", "10000").param("to", "11000")
                        .param("groupBy", "region").param("groupBy", "service"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].windowStart").value(10000))
                .andExpect(jsonPath("$[0].totalCount").value(3))
                .andReturn();
        log.info("[端到端查询] 输入窗口=10000 groupBy=region,service -> 最终聚合={}",
                agg.getResponse().getContentAsString());
    }

    @Test
    void lateRejectionAndLimitReasonsAreInspectable() throws Exception {
        // 推进水位线：ts=9000 -> WM=8000(默认迟到容忍30s下，窗口5000未关)。
        // 默认 allowedLateness=30000，改为直接构造关闭场景：先发31秒后的事件
        String future = "{\"records\":[{\"id\":\"f1\",\"kind\":\"METRIC\",\"timestamp\":41000}]}";
        mockMvc.perform(post("/api/telemetry/reports")
                .contentType(MediaType.APPLICATION_JSON).content(future)).andExpect(status().isOk());

        // ts=10000 属窗口[10000,20000)，结束20000 <= WM(41000-30000=11000)? 20000<=11000 false => 不算迟到。
        // 改为 ts=-? 不行；用 0~10000 窗口：ts=5000 属[0,10000)，结束10000 <= 11000 => 过晚兜底
        String late = "{\"records\":[{\"id\":\"late-1\",\"kind\":\"METRIC\",\"timestamp\":5000}]}";
        mockMvc.perform(post("/api/telemetry/reports")
                        .contentType(MediaType.APPLICATION_JSON).content(late))
                .andExpect(jsonPath("$.reroutedLate").value(1))
                .andExpect(jsonPath("$.results[0].reason").value("TOO_LATE"));

        MvcResult rej = mockMvc.perform(get("/api/telemetry/rejections").param("limit", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].reason").value("TOO_LATE"))
                .andExpect(jsonPath("$[0].status").value("REROUTED_LATE"))
                .andReturn();
        log.info("[端到端迟到] 输入窗口=0 ts=5000, 水位线=11000 -> 判定=REROUTED_LATE；留痕={}",
                rej.getResponse().getContentAsString());
    }

    @Test
    void malformedJsonAndBadRangeReturnStableErrors() throws Exception {
        mockMvc.perform(post("/api/telemetry/reports")
                        .contentType(MediaType.APPLICATION_JSON).content("{not-json"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.reason").value("MALFORMED_PAYLOAD"))
                .andDo(r -> log.info("[端到端坏包] 非法JSON -> 判定=400 MALFORMED_PAYLOAD"));

        mockMvc.perform(get("/api/telemetry/aggregations")
                        .param("from", "20000").param("to", "10000"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.reason").value("INVALID_REQUEST"))
                .andDo(r -> log.info("[端到端坏查询] from>to -> 判定=400 INVALID_REQUEST"));
    }
}
