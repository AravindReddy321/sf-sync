package com.dev.sfsync.dto;

public record ErrorLogDto(
        String message,
        String processName,
        String recordId,
        String sourceName,
        String exceptionType,
        String stackTrace
) {
}
