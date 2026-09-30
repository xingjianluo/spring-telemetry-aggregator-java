package com.github.xingjianluo.springtelemetryaggregatorjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 窗口与资源约束配置。
 */
@Component
@ConfigurationProperties(prefix = "telemetry")
public class TelemetryProperties {

    /** 窗口大小（毫秒） */
    private long windowSizeMillis = 60_000;

    /** 可接受的最大迟到时长（毫秒），超出则落兜底 */
    private long maxLatenessMillis = 30_000;

    /** 单条记录允许的最大属性键数量 */
    private int maxAttributeKeys = 32;

    /** 单窗口允许的最大去重记录数（容量上限） */
    private int maxRecordsPerWindow = 100_000;

    public long getWindowSizeMillis() {
        return windowSizeMillis;
    }

    public void setWindowSizeMillis(long windowSizeMillis) {
        this.windowSizeMillis = windowSizeMillis;
    }

    public long getMaxLatenessMillis() {
        return maxLatenessMillis;
    }

    public void setMaxLatenessMillis(long maxLatenessMillis) {
        this.maxLatenessMillis = maxLatenessMillis;
    }

    public int getMaxAttributeKeys() {
        return maxAttributeKeys;
    }

    public void setMaxAttributeKeys(int maxAttributeKeys) {
        this.maxAttributeKeys = maxAttributeKeys;
    }

    public int getMaxRecordsPerWindow() {
        return maxRecordsPerWindow;
    }

    public void setMaxRecordsPerWindow(int maxRecordsPerWindow) {
        this.maxRecordsPerWindow = maxRecordsPerWindow;
    }
}
