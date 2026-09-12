package com.dev.sfsync.runner;

import com.dev.sfsync.client.SfGrpcClient;
import com.dev.sfsync.dto.ErrorLogDto;
import com.dev.sfsync.exception.SfSyncException;
import com.dev.sfsync.service.ErrorLogService;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class SfGrpcRunner implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(SfGrpcRunner.class);

    private final ErrorLogService errorLogService;
    private final SfGrpcClient sfGrpcClient;
    private final List<String> topicsList;
    private final ApplicationContext applicationContext;

    public SfGrpcRunner(ErrorLogService errorLogService,SfGrpcClient sfGrpcClient, @Value("${sf.channels.account-cdc}") String accountTopicName, @Value("${sf.channels.contact-cdc}") String contactTopicName,
                        ApplicationContext applicationContext){
        this.errorLogService = errorLogService;
        this.sfGrpcClient = sfGrpcClient;
        this.topicsList = Arrays.asList(accountTopicName, contactTopicName);
        this.applicationContext = applicationContext;
    }

    @Override
    public void run(String... args) throws Exception {
        logger.info("SfGrpcRunner starting...sfGrpcClient {}", sfGrpcClient);
//        printAllBeans();
        invoke();
    }

    public void invoke(){
        List<StatusRuntimeException> grpcExceptions = new ArrayList<>();
        for(String topicName : topicsList){
            try{
                sfGrpcClient.getTopicInfo(topicName);
                sfGrpcClient.startCdcSubscription(topicName,5, null);
            } catch(StatusRuntimeException e){
                grpcExceptions.add(e);
                logger.info("StatusRuntimeException e.getMessage() {}",e.getMessage());
                logger.info("StatusRuntimeException e.getStatus() {}",e.getStatus());
            }
        }
        if(!grpcExceptions.isEmpty()){
            handleGrpcException(grpcExceptions);
        }
    }

    public void handleGrpcException(List<StatusRuntimeException> grpcExceptions){
        List<ErrorLogDto> errorLogDtoList = new ArrayList<>();
        grpcExceptions.forEach(e -> {
            ErrorLogDto errorLogDto = new ErrorLogDto(
                    e.getMessage(),
                    "SfGrpcRunner",
                    "",
                    "Salesforce",
                    e.getClass().toString(),
                    e.toString()
            );
            errorLogDtoList.add(errorLogDto);
        });

        errorLogService.logErrorList(errorLogDtoList);
    }

    public void printAllBeans(){
        logger.info("inside printAllBeans");
        Arrays.stream(applicationContext.getBeanDefinitionNames())
                .forEach(b -> {
                    logger.info("beanName {}", b);
                    if("nettyGrpcChannelFactory".equals(b)){
                        logger.info("nettyGrpcChannelFactory found");
                    }
                });
    }
}
