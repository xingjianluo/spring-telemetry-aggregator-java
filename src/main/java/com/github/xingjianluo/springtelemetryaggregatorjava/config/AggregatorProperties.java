package com.github.xingjianluo.springtelemetryaggregatorjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 聚合器可调参数，前缀 telemetry。
 */
@ConfigurationProperties(prefix = "telemetry")
public class AggregatorProperties {

    /** 聚合窗口长度（毫秒），事件时间按此值对齐窗口边界。 */
    private long windowMillis = 10_000;

    /** 可接受的最大迟到时间（毫秒），早于 (水位线 - 该值) 的数据进入兜底。 */
    private long allowedLatenessMillis = 30_000;

    /** 单窗口内允许的不同属性键值对（维度）数量上限。 */
    private int maxDimensionsPerWindow = 100;

    /** 单窗口允许接收的记录条数上限。 */
    private int maxRecordsPerWindow = 10_000;

    /** 内存中保留的窗口数量上限，超出后淘汰最旧窗口。 */
    private int maxWindowsRetained = 100;

    /** 拒绝/兜底事件列表保留条数，便于排查。 */
    private int maxRejectedEvents = 1_000;

    public long getWindowMillis() { return windowMillis; }
    public void setWindowMillis(long windowMillis) { this.windowMillis = windowMillis; }

    public long getAllowedLatenessMillis() { return allowedLatenessMillis; }
    public void setAllowedLatenessMillis(long allowedLatenessMillis) { this.allowedLatenessMillis = allowedLatenessMillis; }

    public int getMaxDimensionsPerWindow() { return maxDimensionsPerWindow; }
    public void setMaxDimensionsPerWindow(int maxDimensionsPerWindow) { this.maxDimensionsPerWindow = maxDimensionsPerWindow; }

    public int getMaxRecordsPerWindow() { return maxRecordsPerWindow; }
    public void setMaxRecordsPerWindow(int maxRecordsPerWindow) { this.maxRecordsPerWindow = maxRecordsPerWindow; }

    public int getMaxWindowsRetained() { return maxWindowsRetained; }
    public void setMaxWindowsRetained(int maxWindowsRetained) { this.maxWindowsRetained = maxWindowsRetained; }

    public int getMaxRejectedEvents() { return maxRejectedEvents; }
    public void setMaxRejectedEvents(int maxRejectedEvents) { this.maxRejectedEvents = maxRejectedEvents; }
}
