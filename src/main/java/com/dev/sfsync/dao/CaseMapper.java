package com.dev.sfsync.dao;

import com.dev.sfsync.dto.CaseDto;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Optional;

@Component
public class CaseMapper {

    public Case convertCaseDtoToCase(CaseDto caseDto){
        return Case.builder()
                .sfId(caseDto.id())
                .subject(caseDto.subject())
                .priority(caseDto.priority())
                .status(caseDto.status())
                .reason(caseDto.reason())
                .closedDate(parseDate(caseDto.closedDate()))
                .createdDate(parseDate(caseDto.createdDate()))
                .lastModifiedDate(parseDate(caseDto.lastModifiedDate()))
                .build();

    }

    private Instant parseDate(String dateString){
        // Instant closedDateInstant = (caseDto.closedDate() == null ||caseDto.closedDate().isEmpty()) ? null : Instant.parse(caseDto.closedDate());
        return Optional.ofNullable(dateString)
                .filter(d->!d.isEmpty())
                .map(Instant::parse)
                .orElse(null);
    }
}
