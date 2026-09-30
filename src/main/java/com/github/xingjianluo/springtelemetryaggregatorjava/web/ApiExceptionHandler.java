package com.github.xingjianluo.springtelemetryaggregatorjava.web;

import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.Map;

/**
 * 把格式错误的请求体、非法查询参数等异常转换为稳定、可区分的错误响应，
 * 而不是默认的 500：MALFORMED_PAYLOAD=400，内部错误=500。
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(HttpMessageNotReadableException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, Object> handleUnreadable(HttpMessageNotReadableException e) {
        return Map.of(
                "timestamp", Instant.now().toEpochMilli(),
                "status", 400,
                "reason", "MALFORMED_PAYLOAD",
                "message", "请求体不是合法 JSON 或字段类型错误：" + e.getMostSpecificCause().getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, Object> handleIllegalArgument(IllegalArgumentException e) {
        return Map.of(
                "timestamp", Instant.now().toEpochMilli(),
                "status", 400,
                "reason", "INVALID_REQUEST",
                "message", String.valueOf(e.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Map<String, Object> handleOther(Exception e) {
        return Map.of(
                "timestamp", Instant.now().toEpochMilli(),
                "status", 500,
                "reason", "INTERNAL_ERROR",
                "message", String.valueOf(e.getMessage()));
    }
}
