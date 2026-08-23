package com.dev.sfsync.client;

import com.dev.sfsync.dao.Account;
import com.dev.sfsync.dto.AccountDto;
import com.dev.sfsync.exception.SfSyncException;
import com.dev.sfsync.handler.CdcHandler;
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
    private final ObjectMapper objectMapper;

    private final PubSubGrpc.PubSubStub asyncStubAuthenticated;
    private final PubSubGrpc.PubSubBlockingStub blockingStubAuthenticated;

    private final Map<String, CdcHandler> cdcHandlerMap;

    private static ConcurrentHashMap<String, String> schemaMap = new ConcurrentHashMap<>();
    private static List<ProducerEvent> failedProducerEventList = new ArrayList<>();

    private ConcurrentHashMap<String,StreamObserver<FetchRequest>> fetchRequestStreamObserverMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, RetryCdcState> retryCdcStateMap = new ConcurrentHashMap<>();


    public SfGrpcClient(ThreadPoolTaskScheduler threadPoolTaskScheduler, ErrorLogService errorLogService, SfSyncService sfSyncService, ObjectMapper objectMapper, PubSubGrpc.PubSubStub asyncStubAuthenticated, PubSubGrpc.PubSubBlockingStub blockingStubAuthenticated, Map<String, CdcHandler> cdcHandlerMap){

        this.sfSyncService = sfSyncService;
        this.errorLogService = errorLogService;
        this.threadPoolTaskScheduler = threadPoolTaskScheduler;
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
    public void getTopicInfo(String topicName) throws StatusRuntimeException{
        TopicRequest topicRequest = buildTopicRequest(topicName);
        logger.info("getTopicInfo blockingStubAuthenticated {}",blockingStubAuthenticated);
        TopicInfo topicInfo = blockingStubAuthenticated.getTopic(topicRequest);
        logger.info("topicInfo: {}",topicInfo);
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

    public List<Map<String,Object>>  getFinalDataMapList(List<ProducerEvent> producerEventList){
        List<Map<String,Object>> dataFinalMapList = new ArrayList<>();
        for(ProducerEvent producerEvent : producerEventList){
            try{
                String schemaId = producerEvent.getSchemaId();
                String schemaJson = schemaMap.containsKey(schemaId) ? schemaMap.get(schemaId) : getAvroSchemaJson(schemaId);
                logger.info("schemaJson {}",schemaJson);
                ByteString protobufByteString = producerEvent.getPayload();
                logger.info("protobuf ByteString {}",protobufByteString);
                byte[] rawPayloadJavaBytes = protobufByteString.toByteArray();
                logger.info("rawPayloadJavaBytes {}",rawPayloadJavaBytes);
                dataFinalMapList.add(decodeAvroPayload(schemaJson, rawPayloadJavaBytes));
            } catch (Exception e) {
                logger.error("error in getFinalDataMapList {}",e.getMessage());
                failedProducerEventList.add(producerEvent);
            }
        }
        logger.info("dataMapList {}", dataFinalMapList);
        return dataFinalMapList;
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

    private Map<String, Object> decodeAvroPayload(String schemaJson, byte[] rawPayloadJavaBytes){
        Map<String, Object> finalDataMap = new HashMap<>();
        try{
            Schema schema = new Schema.Parser().parse(schemaJson);
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);

            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(rawPayloadJavaBytes, null);

            GenericRecord changeEventRecord = reader.read(null, decoder);

            logger.info("record {}",changeEventRecord);

            GenericRecord header = (GenericRecord) changeEventRecord.get("ChangeEventHeader");
            logger.info("header {}", header);

            String changeEvent = header.get(CHANGE_TYPE).toString();
            ChangeType changeType =  ChangeType.valueOf(changeEvent);

            finalDataMap.put(CHANGE_TYPE,changeType);
            logger.info("changeEventName {}", schema.getName());
            finalDataMap.put("changeEventName", schema.getName() );

            Map<String, Object> dataMap = populateDataMapBasedOnChangeEvent(changeType, header, changeEventRecord, schema);

            logger.info("dataMap before id {}", dataMap);
            for(Object recordId : (List<?>) header.get("recordIds")){
                dataMap.put("Id",recordId.toString());
                dataMap.computeIfAbsent(IS_DELETED, key->false);
                logger.info("dataMap {}", dataMap);
                finalDataMap.put("dataMap",dataMap);
            }

        } catch (Exception e) {
            throw new SfSyncException(e.getMessage());
        }
        return finalDataMap;

    }

    private Map<String, Object>  populateDataMapBasedOnChangeEvent(ChangeType changeType, GenericRecord header, GenericRecord changeEventRecord, Schema schema){
        Map<String, Object> dataMap = new HashMap<>();
        List<String> fieldNames = new ArrayList<>();

        if(ChangeType.UPDATE.equals(changeType)){
            logger.info("change type update");
        }
        if(ChangeType.UPDATE.equals(changeType)){
            Set<String> updatedFieldCategories = Set.of("changedFields");
            for(String updatedFieldCategory : updatedFieldCategories){
                fieldNames.addAll(convertBitmapToFieldNames(header, updatedFieldCategory,schema));
            }
        } else if (ChangeType.CREATE.equals(changeType)) {
            logger.info("change type create");
            logger.info("fields from record {}", changeEventRecord.getSchema().getFields());
            for(Schema.Field field : schema.getFields()){
                String fieldName = field.name();
                if("ChangeEventHeader".equals(fieldName)){ continue; }
                fieldNames.add(fieldName);
            }
        } else if (ChangeType.DELETE.equals(changeType)){
            logger.info("change type delete");
            dataMap.put(IS_DELETED, true);
        }
        logger.info("fieldNames {}", fieldNames);
        getDataMapFromAvroPayload(fieldNames, changeEventRecord, dataMap);

        return dataMap;
    }


    public List<String> convertBitmapToFieldNames(GenericRecord header, String fieldHeaderName, Schema schema){
        List<String> fieldNamesList = new ArrayList<>();
        List<Schema.Field> fields = schema.getFields();
        List<?> bitmapList = (List<?>) header.get(fieldHeaderName);
        if(bitmapList == null || bitmapList.isEmpty()) return fieldNamesList;
        logger.info("bitmapList {}",bitmapList);
        for(int i=0; i<bitmapList.size(); i++){
            Object hexStringGenericObject = bitmapList.get(i);
            logger.info("hexStringGenericObject {}",hexStringGenericObject);
            String rawHex = hexStringGenericObject.toString().trim();
            String bitmapHexString = rawHex.startsWith("0x")? rawHex.substring(2):rawHex;
            logger.info("bitmapHexString {}",bitmapHexString);
            Long bitmapLong = Long.parseLong(bitmapHexString,16);
            logger.info("bitmapLong {}",bitmapLong);
            for(int j=0;j<32;j++){
                if((bitmapLong & (1L << j)) != 0 ){
                    logger.info("schemaIndex {}",j);
                    logger.info("fieldName {}", fields.get((i*32)+j).name());
                    fieldNamesList.add(fields.get((i*32)+j).name());
                }
            }
        }
        return fieldNamesList;
    }

    public void getDataMapFromAvroPayload(List<String> fieldNames, GenericRecord avroPayload, Map<String, Object> dataMap){
        for(String fieldName : fieldNames){
            Object value = avroPayload.get(fieldName);
            if(value instanceof CharSequence) { value = value.toString(); }
            dataMap.put(fieldName, value);
        }
        logger.info("dataMap from getDataMapFromAvroPayload {}", dataMap);
    }


    public void populateCreateOrUpdateAccountDtoList(List<Map<String,Object>> finalDataMapList, List<AccountDto> accountDtoCreateList, List<AccountDto> accountDtoUpdateList){
        Map<String, Object> updatedDataMap = new HashMap<>();
        for(Map<String, Object> finalDataMap : finalDataMapList){
            Map<String, Object> dataMap = (Map<String, Object>)finalDataMap.get("dataMap");
            String sfId = (String)dataMap.get("Id");
            logger.info("change type class {}",finalDataMap.get(CHANGE_TYPE).getClass().getName());
            if(ChangeType.CREATE.equals(finalDataMap.get(CHANGE_TYPE))){
                accountDtoCreateList.add(objectMapper.convertValue(dataMap,AccountDto.class));
            } else{
                updatedDataMap.put(sfId, dataMap);
            }
        }
        logger.info("updatedDataMap {}", updatedDataMap);
        if(!updatedDataMap.isEmpty()){
            accountDtoUpdateList.addAll(buildAccountDtoUpdateEvent(updatedDataMap));
        }
    }

    public List<AccountDto> buildAccountDtoUpdateEvent(Map<String, Object> updatedDataMap){
        Set<String> sfIds = updatedDataMap.keySet();
        List<Account> accList = sfSyncService.fetchAccountsBySfIds(sfIds);
        logger.info("accList fetched {}",accList);
        List<AccountDto> updatedAccountDtos = new ArrayList<>();
        for(Account acc : accList){
            logger.info("acc before update {}",acc);
            logger.info("acc getSfId {}",acc.getSfId());
            Map<String, Object> dataMap = (Map<String, Object>) updatedDataMap.get(acc.getSfId());
            AccountDto accountDto = AccountDto.builder()
                    .id(dataMap.containsKey("Id") ? dataMap.get("Id").toString() : acc.sfId)
                    .name(dataMap.containsKey("Name") ? dataMap.get("Name").toString() : acc.name)
                    .description(dataMap.containsKey("Description") ? (String) dataMap.get("Description") : acc.description)
                    .isDeleted(dataMap.containsKey(IS_DELETED) ? (boolean)dataMap.get(IS_DELETED): acc.isDeleted)
                    .build();
            logger.info("dataMap {}",dataMap);
            logger.info("accountDto {}",accountDto);
            updatedAccountDtos.add(accountDto);
        }
        return  updatedAccountDtos;
    }

    public void syncAccounts(List<AccountDto> accountDtoList){
        sfSyncService.syncAccounts(accountDtoList);
    }

    public void processProducerEvents(List<ProducerEvent> producerEventList){
        List<Map<String,Object>> finalDataMapList = new ArrayList<>();
        List<AccountDto> accountDtoCreateList = new ArrayList<>();
        List<AccountDto> accountDtoUpdateList = new ArrayList<>();

        finalDataMapList.addAll(getFinalDataMapList(producerEventList));
        logger.info("dataMapList {}", finalDataMapList);

        populateCreateOrUpdateAccountDtoList(finalDataMapList, accountDtoCreateList, accountDtoUpdateList);

        if(!accountDtoCreateList.isEmpty()){
            logger.info("accountDtoCreateList {}",accountDtoCreateList);
            syncAccounts(accountDtoCreateList);
        }
        if(!accountDtoUpdateList.isEmpty()){
            logger.info("updatedAccountDtoList {}", accountDtoUpdateList);
            syncAccounts(accountDtoUpdateList);
        }
    }



    public void startCdcSubscription(String topicName, int numRequested, ByteString replayId){
        try{
            logger.info("inside startCdcSubscription topic {}", topicName);
            logger.info("asyncStubAuthenticated: {}",asyncStubAuthenticated);
            logger.info("blockingStubAuthenticated: {}",blockingStubAuthenticated);

            StreamObserver<FetchRequest> fetchRequestStreamObserver = asyncStubAuthenticated.subscribe(new SfGrpcListener(threadPoolTaskScheduler, errorLogService,this, cdcHandlerMap, topicName, numRequested));
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
