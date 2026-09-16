package com.dev.sfsync.dao;

import com.dev.sfsync.dto.ErrorLogDto;
import org.springframework.stereotype.Component;

@Component
public class ErrorLogMapper {

    public ErrorLog convertErrorlogDtoToErrorLog(ErrorLogDto errorLogDto){
        return ErrorLog.builder()
                .message(errorLogDto.message())
                .processName(errorLogDto.processName())
                .recordId(errorLogDto.recordId())
                .sourceName(errorLogDto.sourceName())
                .exceptionType(errorLogDto.exceptionType())
                .stackTrace(errorLogDto.stackTrace())
                .build();
    }
}
