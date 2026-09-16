package com.dev.sfsync.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;

@ControllerAdvice
public class SfSyncExceptionHandler {
    private static final Logger logger = LoggerFactory.getLogger(SfSyncExceptionHandler.class);

    @ExceptionHandler(SfSyncException.class)
    public ResponseEntity<String> handleSfSyncException(SfSyncException sfSyncException, WebRequest webRequest){
        logger.info("inside handleSfSyncException");
        return new ResponseEntity<>("sync error", HttpStatus.BAD_REQUEST);
    }
}
