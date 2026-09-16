package com.dev.sfsync.service;

import com.dev.sfsync.dao.ErrorLog;
import com.dev.sfsync.dao.ErrorLogMapper;
import com.dev.sfsync.dto.AccountDto;
import com.dev.sfsync.dto.ErrorLogDto;
import com.dev.sfsync.repository.ErrorLogRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Service
public class ErrorLogService {
    private static final Logger logger = LoggerFactory.getLogger(ErrorLogService.class);

    private final ErrorLogRepository errorLogRepository;
    private final ErrorLogMapper errorLogMapper;
    private final DlqService dlqService;

    public ErrorLogService(ErrorLogRepository errorLogRepository, ErrorLogMapper errorLogMapper, DlqService dlqService) {
        this.errorLogRepository = errorLogRepository;
        this.errorLogMapper = errorLogMapper;
        this.dlqService = dlqService;
    }

    @Retryable(
            retryFor = CannotCreateTransactionException.class,
            maxAttempts = 3,
            backoff = @Backoff(delay = 5000)
    )
    @Transactional
    public void logErrorList(List<ErrorLogDto> errorLogDtoList){
        List<ErrorLog> errorLogList = new ArrayList<>();
        errorLogDtoList.forEach(errorLogDto -> errorLogList.add(errorLogMapper.convertErrorlogDtoToErrorLog(errorLogDto)));
        errorLogRepository.saveAll(errorLogList);
    }

    @Retryable(
            retryFor = CannotCreateTransactionException.class,
            maxAttempts = 3,
            backoff = @Backoff(delay = 5000)
    )
    @Transactional
    public boolean logError(ErrorLogDto errorLogDto){
        try{
            errorLogRepository.save(errorLogMapper.convertErrorlogDtoToErrorLog(errorLogDto));
            return true;
        } catch( DataIntegrityViolationException e){
            return false;
        }
    }

    @Recover
    public void logErrorListRecover(CannotCreateTransactionException e, List<ErrorLogDto> errorLogDtoList){
        dlqService.moveToDlq(errorLogDtoList);
    }

    @Recover
    public boolean logErrorRecover(CannotCreateTransactionException e, AccountDto accountDto){
        logger.info("inside logErrorRecover");
        dlqService.moveToDlq(accountDto);
        return false;
    }


}
