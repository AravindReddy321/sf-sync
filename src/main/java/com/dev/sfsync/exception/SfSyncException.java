package com.dev.sfsync.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

//@ResponseStatus(value = HttpStatus.BAD_REQUEST)
public class SfSyncException extends RuntimeException{

    public SfSyncException(String message){
        super(message);
    }
}
