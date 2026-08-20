package com.dev.sfsync.dto;

import java.time.Instant;

public record CaseDto(
        String id,
        String subject,
        String priority,
        String status,
        String reason,
        String closedDate
) {
}
