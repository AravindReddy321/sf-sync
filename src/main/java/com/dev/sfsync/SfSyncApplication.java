package com.dev.sfsync;

import com.salesforce.eventbus.protobuf.PubSubGrpc;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.grpc.client.ImportGrpcClients;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableScheduling;

@ImportGrpcClients(basePackageClasses = PubSubGrpc.class, target = "salesforce", types = {
        PubSubGrpc.PubSubStub.class,
        PubSubGrpc.PubSubBlockingStub.class
})
@SpringBootApplication
@EnableScheduling
@EnableRetry
@EnableJpaAuditing
public class SfSyncApplication {

    public static void main(String[] args) {
        ApplicationContext applicationContext = SpringApplication.run(SfSyncApplication.class, args);
//        SfGrpcClient sfGrpcClient = applicationContext.getBean(SfGrpcClient.class);
//        sfGrpcClient.getTopicInfo("/data/AccountChangeEvent");
//        sfGrpcClient.startCdcSubscription("/data/AccountChangeEvent",5);
    }

}
