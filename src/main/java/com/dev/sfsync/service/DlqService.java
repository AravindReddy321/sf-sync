package com.dev.sfsync.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Component
public class DlqService {
    private static final Logger logger = LoggerFactory.getLogger(DlqService.class);
    private final RabbitTemplate rabbitTemplate;

    public DlqService(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    @Async
    public void moveToDlq(Object object){
        logger.info("inside moveToDlq {}", object);
        rabbitTemplate.convertAndSend("sf.sync.dlx","sf.sync.global.error",object);
    }
}
