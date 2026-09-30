package com.github.xingjianluo.springtelemetryaggregatorjava.model;

/** 拒绝/兜底的具体判定原因，便于排查时区分。 */
public enum RejectReason {
    MISSING_ID,
    MISSING_KIND,
    UNKNOWN_KIND,
    INVALID_TIMESTAMP,
    INVALID_DURATION,
    MALFORMED_PAYLOAD,
    ID_CONFLICT,
    TOO_LATE,
    TOO_MANY_DIMENSIONS,
    WINDOW_FULL
}
