package com.dev.sfsync.dao;

import com.dev.sfsync.dto.CaseDto;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class CaseMapper {

    public Case convertCaseDtoToCase(CaseDto caseDto){
        return Case.builder()
                .sfId(caseDto.id())
                .subject(caseDto.subject())
                .priority(caseDto.priority())
                .status(caseDto.status())
                .reason(caseDto.reason())
                .closedDate(Instant.parse(caseDto.closedDate()))
                .build();

    }
}
