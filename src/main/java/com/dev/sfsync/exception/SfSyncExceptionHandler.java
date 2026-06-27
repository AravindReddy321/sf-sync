package com.dev.sfsync.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;

@ControllerAdvice
public class SfSyncExceptionHandler {

    @ExceptionHandler(SfSyncException.class)
    public ResponseEntity<String> handleSfSyncException(SfSyncException sfSyncException, WebRequest webRequest){
        return new ResponseEntity<>("sync error", HttpStatus.BAD_REQUEST);
    }
}
