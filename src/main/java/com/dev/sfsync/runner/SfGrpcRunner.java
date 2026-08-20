package com.dev.sfsync.runner;

import com.dev.sfsync.client.SfGrpcClient;
import com.dev.sfsync.exception.SfSyncException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class SfGrpcRunner implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(SfGrpcRunner.class);

    private final SfGrpcClient sfGrpcClient;
    private final List<String> topicsList;

    public SfGrpcRunner(SfGrpcClient sfGrpcClient, @Value("${sf.channels.account-cdc}") String accountTopicName, @Value("${sf.channels.contact-cdc}") String contactTopicName){
        this.sfGrpcClient = sfGrpcClient;
        this.topicsList = Arrays.asList(accountTopicName, contactTopicName);
    }

    @Override
    public void run(String... args) throws Exception {
        logger.info("SfGrpcRunner starting...");
        topicsList.forEach(topicName -> {
            try{
            sfGrpcClient.getTopicInfo(topicName);
//            sfGrpcClient.startCdcSubscription(topicName,5, null);
            } catch(Exception e){
                logger.error("error in SfGrpc run {}",e.getMessage());
                throw new SfSyncException(e.getMessage());
            }
        });
    }
}
