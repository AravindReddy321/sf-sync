package com.dev.sfsync.dto;

import java.util.List;

public record SfQueryResponseWrapper<T>(
        int totalSize,
        boolean done,
        List<T> records
) {
}
