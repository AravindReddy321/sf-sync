package com.dev.sfsync.client;

import com.dev.sfsync.dao.Account;
import com.dev.sfsync.dto.AccountDto;
import com.dev.sfsync.dto.ErrorLogDto;
import com.dev.sfsync.exception.SfSyncException;
import com.dev.sfsync.handler.CdcHandler;
import com.dev.sfsync.service.DlqService;
import com.dev.sfsync.service.ErrorLogService;
import com.dev.sfsync.service.SfSyncService;
import com.dev.sfsync.utility.RetryCdcState;
import com.dev.sfsync.utility.SfGrpcListener;
import com.google.protobuf.ByteString;
import com.salesforce.eventbus.protobuf.*;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


@Component
public class SfGrpcClient {

    private static final Logger logger = LoggerFactory.getLogger("SfSyncClient.class");
    private static final String IS_DELETED = "IsDeleted";
    private static final String CHANGE_TYPE = "changeType";
    private enum ChangeType{ UPDATE, CREATE, DELETE }

    private final SfSyncService sfSyncService;
    private final ErrorLogService errorLogService;
    private final ThreadPoolTaskScheduler threadPoolTaskScheduler;
    private final DlqService dlqService;
    private final ObjectMapper objectMapper;

    private final PubSubGrpc.PubSubStub asyncStubAuthenticated;
    private final PubSubGrpc.PubSubBlockingStub blockingStubAuthenticated;

    private final Map<String, CdcHandler> cdcHandlerMap;

    private static ConcurrentHashMap<String, String> schemaMap = new ConcurrentHashMap<>();
    private static List<ProducerEvent> failedProducerEventList = new ArrayList<>();

    private ConcurrentHashMap<String,StreamObserver<FetchRequest>> fetchRequestStreamObserverMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, RetryCdcState> retryCdcStateMap = new ConcurrentHashMap<>();


    public SfGrpcClient(ThreadPoolTaskScheduler threadPoolTaskScheduler, ErrorLogService errorLogService, SfSyncService sfSyncService, ObjectMapper objectMapper, PubSubGrpc.PubSubStub asyncStubAuthenticated, PubSubGrpc.PubSubBlockingStub blockingStubAuthenticated, Map<String, CdcHandler> cdcHandlerMap, DlqService dlqService){

        this.sfSyncService = sfSyncService;
        this.errorLogService = errorLogService;
        this.threadPoolTaskScheduler = threadPoolTaskScheduler;
        this.dlqService =dlqService;
        this.objectMapper = objectMapper;
        this.cdcHandlerMap =cdcHandlerMap;
        this.asyncStubAuthenticated = asyncStubAuthenticated;
        this.blockingStubAuthenticated =blockingStubAuthenticated;
    }


    public TopicRequest buildTopicRequest(String topicName){
        return TopicRequest.newBuilder()
                .setTopicName(topicName)
                .build();
    }

    @Retryable(
            value = {StatusRuntimeException.class},
            exceptionExpression = "status.code.toString()  == 'UNAVAILABLE' ",
            maxAttempts = 3,
            backoff = @Backoff(delay = 1000)
    )
    public void getTopicInfo(String topicName){
        TopicRequest topicRequest = buildTopicRequest(topicName);
        logger.info("getTopicInfo blockingStubAuthenticated {}",blockingStubAuthenticated);
        TopicInfo topicInfo = blockingStubAuthenticated.getTopic(topicRequest);
        logger.info("topicInfo: {}",topicInfo);
    }

    @Recover
    public void getTopicInfoRecover(Exception e, String topicName) {
        ErrorLogDto errorLogDto = new ErrorLogDto(
                "grpc error: "+e.getMessage(),
                topicName+" getTopicInfoRecover recover",
                "",
                "Salesforce",
                e.getClass().toString(),
                e.toString()
        );
        errorLogService.logError(errorLogDto);
    }

    public FetchRequest buildFetchRequest(String topicName, int numRequested, ByteString replayId){
        try{
            if(replayId == null){ return buildFetchRequest(topicName, numRequested); }
            return FetchRequest.newBuilder()
                    .setTopicName(topicName)
                    .setNumRequested(numRequested)
                    .setReplayPreset(ReplayPreset.CUSTOM)
                    .setReplayId(replayId)
                    .build();
        } catch(Exception e){
            logger.error("error in buildRefillFetchRequest using last replayid {}",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    public FetchRequest buildFetchRequest(String topicName, int numRequested){
        try{
            return FetchRequest.newBuilder()
                    .setTopicName(topicName)
                    .setNumRequested(numRequested)
                    .setReplayPreset(ReplayPreset.LATEST)
                    .build();
        } catch (Exception e) {
            logger.info("error in buildRefillRequest no replayid {}", e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    public void refillFetchRequest(String topicName, int numRequested, ByteString replayId){
        try{
            FetchRequest fetchRefillRequest = buildFetchRequest(topicName, numRequested, replayId);
            if(fetchRequestStreamObserverMap.containsKey(topicName)){
                fetchRequestStreamObserverMap.get(topicName).onNext(fetchRefillRequest);
            } else{
                logger.info("fetchRequestStreamObserverMap does not contain topic {}",topicName);
            }
        } catch(Exception e){
            logger.error("error in refillFetchRequest no replayid {}", e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    public void restartSubsrciption(String topicName, int numRequested, ByteString replayId){
            if(fetchRequestStreamObserverMap.containsKey(topicName)){
                fetchRequestStreamObserverMap.remove(topicName);
            }
            startCdcSubscription(topicName, numRequested, replayId);
    }

    public String getAvroSchemaJson(String schemaId){
        try{
            return schemaMap.computeIfAbsent(schemaId, id->{
                logger.info("fetching schema for id {}", id);
                SchemaRequest schemaRequest = SchemaRequest.newBuilder().setSchemaId(id).build();
                SchemaInfo schemaInfo = blockingStubAuthenticated.getSchema(schemaRequest);
                return schemaInfo.getSchemaJson();
            });
        } catch (Exception e) {
            logger.error("error in getAvroSchemaJson {}",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }



    public void startCdcSubscription(String topicName, int numRequested, ByteString replayId){
        try{
            logger.info("inside startCdcSubscription topic {}", topicName);
            logger.info("asyncStubAuthenticated: {}",asyncStubAuthenticated);
            logger.info("blockingStubAuthenticated: {}",blockingStubAuthenticated);

            StreamObserver<FetchRequest> fetchRequestStreamObserver = asyncStubAuthenticated.subscribe(new SfGrpcListener(threadPoolTaskScheduler, errorLogService,this, cdcHandlerMap, topicName, numRequested,dlqService));
            fetchRequestStreamObserverMap.put(topicName, fetchRequestStreamObserver);
            retryCdcStateMap.computeIfAbsent(topicName, t-> getDefaultRetryCdcState(numRequested));
            fetchRequestStreamObserver.onNext(buildFetchRequest(topicName, numRequested, replayId));
        } catch (Exception e) {
            throw new SfSyncException(e.getMessage());
        }

    }

    public RetryCdcState getDefaultRetryCdcState(int numRequested){
        return RetryCdcState.builder()
                .numRequested(numRequested)
                .retryCount(0)
                .replayId(null)
                .build();
    }

    public RetryCdcState getRetryCdcState(String topicName){
        return retryCdcStateMap.getOrDefault(topicName, null);
    }

    public void resetRetryCdcState(String topicName, int  numRequested){
        retryCdcStateMap.put(topicName, getDefaultRetryCdcState(numRequested));
    }

    public boolean needToResetRetryCdcState(String topicName){
        RetryCdcState retryCdcState = getRetryCdcState(topicName);
        if(retryCdcState == null) return true;
        if(retryCdcState.getRetryCount() > 0) return true;
        return false;
    }

    public boolean retryLimitAvialbale(String topicName){
        RetryCdcState retryCdcState = getRetryCdcState(topicName);
        return retryCdcState == null || retryCdcState.getRetryCount() < 3;
    }

    public void retryCdcStateIncerement(String  topicName, int numRequested){
        RetryCdcState retryCdcState = getRetryCdcState(topicName);
        if(retryCdcState == null)
            resetRetryCdcState(topicName, numRequested);
        else
            retryCdcState.setRetryCount(retryCdcState.getRetryCount() + 1);
    }

}
