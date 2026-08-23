package com.dev.sfsync.service;

import com.dev.sfsync.dao.ErrorLog;
import com.dev.sfsync.dao.ErrorLogMapper;
import com.dev.sfsync.dto.ErrorLogDto;
import com.dev.sfsync.repository.ErrorLogRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class ErrorLogService {
    private static final Logger logger = LoggerFactory.getLogger(ErrorLogService.class);

    private final ErrorLogRepository errorLogRepository;
    private final ErrorLogMapper errorLogMapper;

    public ErrorLogService(ErrorLogRepository errorLogRepository, ErrorLogMapper errorLogMapper) {
        this.errorLogRepository = errorLogRepository;
        this.errorLogMapper = errorLogMapper;
    }

    public void logError(List<ErrorLogDto> errorLogDtoList){
        List<ErrorLog> errorLogList = new ArrayList<>();
        errorLogDtoList.forEach(errorLogDto -> errorLogList.add(errorLogMapper.convertErrorlogDtoToErrorLog(errorLogDto)));
        errorLogRepository.saveAll(errorLogList);
    }

}
