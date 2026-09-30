package com.github.xingjianluo.springtelemetryaggregatorjava.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;

/**
 * 统一上报协议的单条请求。kind=TRACE 或 METRIC，时间戳为 epoch 毫秒。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TelemetryReportRequest {

    private String id;
    private String kind;
    private Long timestamp;
    private Long durationNanos;
    private String name;
    private String service;
    private String traceId;
    private String spanId;
    private Map<String, String> attributes;

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getKind() { return kind; }
    public void setKind(String kind) { this.kind = kind; }

    public Long getTimestamp() { return timestamp; }
    public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }

    public Long getDurationNanos() { return durationNanos; }
    public void setDurationNanos(Long durationNanos) { this.durationNanos = durationNanos; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getService() { return service; }
    public void setService(String service) { this.service = service; }

    public String getTraceId() { return traceId; }
    public void setTraceId(String traceId) { this.traceId = traceId; }

    public String getSpanId() { return spanId; }
    public void setSpanId(String spanId) { this.spanId = spanId; }

    public Map<String, String> getAttributes() { return attributes; }
    public void setAttributes(Map<String, String> attributes) { this.attributes = attributes; }
}
