package com.dev.sfsync.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMqConfig {

    @Bean
    public MessageConverter messageConverter(){
        return new JacksonJsonMessageConverter("*");
    }

    @Bean
    public Queue sfSyncGlobalDlq(){
        return QueueBuilder.durable("sf.sync.global.dlq").build();
    }

    @Bean
    public TopicExchange sfSyncDlx(){
        return new TopicExchange("sf.sync.dlx");
    }

    @Bean
    public Binding sfSyncGlobalDlqBinding(){
        return BindingBuilder.bind(sfSyncGlobalDlq())
                .to(sfSyncDlx())
                .with("sf.sync.*.error");
    }
}
