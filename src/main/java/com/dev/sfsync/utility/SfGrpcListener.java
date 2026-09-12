package com.dev.sfsync.utility;

import com.dev.sfsync.client.SfGrpcClient;
import com.dev.sfsync.dto.ErrorLogDto;
import com.dev.sfsync.exception.SfSyncException;
import com.dev.sfsync.handler.CdcHandler;
import com.dev.sfsync.service.DlqService;
import com.dev.sfsync.service.ErrorLogService;
import com.google.protobuf.ByteString;
import com.salesforce.eventbus.protobuf.*;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.Data;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.hibernate.exception.JDBCConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Data
public class SfGrpcListener implements StreamObserver<FetchResponse> {

    private static final Logger logger = LoggerFactory.getLogger(SfGrpcListener.class);

    private final Map<String, CdcHandler> cdcHandlerMap = new HashMap<>();
    private final SfGrpcClient sfGrpcClient;
    private final ErrorLogService errorLogService;
    private final DlqService dlqService;
    private final ThreadPoolTaskScheduler threadPoolTaskScheduler;
    private enum ChangeType { CREATE, UPDATE, DELETE };

    private final String topicName;
    private final int numRequested;
    private ByteString lastReplayId;

    public SfGrpcListener(ThreadPoolTaskScheduler threadPoolTaskScheduler,ErrorLogService errorLogService, SfGrpcClient sfGrpcClient, Map<String, CdcHandler> cdcHandlerMap, String topicName, int numRequested, DlqService dlqService) {
        this.cdcHandlerMap.putAll(cdcHandlerMap);
        this.sfGrpcClient = sfGrpcClient;
        this.errorLogService = errorLogService;
        this.threadPoolTaskScheduler = threadPoolTaskScheduler;
        this.topicName = topicName;
        this.numRequested = numRequested;
        this.dlqService = dlqService;
    }


    public void onNext(FetchResponse value){
        logger.info("value: {}"+value);

        List<ConsumerEvent> consumerEventList = value.getEventsList();
        if(consumerEventList.isEmpty() ){ return;}

        Map<String,ProducerEvent> producerEventMap = new HashMap<>();
        consumerEventList.forEach(event -> producerEventMap.put(event.getReplayId().toString(),event.getEvent()));
        processProducerEvents(producerEventMap);

        logger.info("value get pending num requested {}", value.getPendingNumRequested());
        lastReplayId = value.getLatestReplayId();
        int pendingNumRequested= value.getPendingNumRequested();
        if(sfGrpcClient.needToResetRetryCdcState(topicName)){
            sfGrpcClient.resetRetryCdcState(topicName, numRequested);
        }
        if(pendingNumRequested < numRequested){
            sfGrpcClient.refillFetchRequest(topicName,numRequested,lastReplayId);
        }
    }

    public void onError(Throwable t){
        logger.error("inside in onError {}", t.getMessage());
        Status status = Status.fromThrowable(t);
        if(status.getCode() == Status.Code.UNAVAILABLE && sfGrpcClient.retryLimitAvialbale(topicName)){
            restartSubsrciptionWithDelay(60L);
            return;
        }
        StatusRuntimeException e = status.asRuntimeException();
        ErrorLogDto errorLogDto = new ErrorLogDto(
                "grpc error: "+e.getMessage(),
                this.topicName+" response observer on error",
                "",
                "Salesforce",
                e.getClass().toString(),
                e.toString()
        );
        errorLogService.logError(errorLogDto);
    }

    public void onCompleted() {
        logger.info("in onCompleted");
        restartSubsrciptionWithDelay(30L);
    }

    public void processProducerEvents(Map<String,ProducerEvent> producerEventMap){
        List<Map<String,Object>> finalDataMapList = new ArrayList<>();
        finalDataMapList.addAll(getFinalDataMapList(producerEventMap));
        logger.info("finalDataMapList from processProducerEvents {}", finalDataMapList);
        if(finalDataMapList.isEmpty()){
            logger.info("finalDataMapList is empty");
            return;
        }
        String changeEventName =(String)finalDataMapList.get(0).get("changeEventName"); // add null check
        if(!cdcHandlerMap.containsKey(changeEventName)){ return; }
        CdcHandler cdcHandler = cdcHandlerMap.get(changeEventName);
        cdcHandler.processEventsData(finalDataMapList);
    }

    public List<Map<String,Object>>  getFinalDataMapList(Map<String,ProducerEvent> producerEventMap){
        List<Map<String,Object>> finalDataMapList = new ArrayList<>();
        Map<String, Exception> failedProducerEventMap = new HashMap<>();
        for(String replayId : producerEventMap.keySet()){
            ProducerEvent producerEvent = producerEventMap.get(replayId);
            String schemaId = producerEvent.getSchemaId();
            String schemaJson = sfGrpcClient.getAvroSchemaJson(schemaId);
            logger.info("schemaJson {}",schemaJson);
            ByteString protobufByteStringPayload = producerEvent.getPayload();
            logger.info("protobuf ByteString {}",protobufByteStringPayload);
            byte[] rawPayloadJavaBytes = protobufByteStringPayload.toByteArray();
            logger.info("rawPayloadJavaBytes {}",rawPayloadJavaBytes);
            Map<String,Object> finalDataMap = new HashMap<>();
            try{
                finalDataMap = decodeAvroPayloadToFinalDataMap(schemaJson, rawPayloadJavaBytes);
                finalDataMapList.add(finalDataMap);
            } catch (IOException e) {
//                logger.info("unable to decode avro payload {}",e.getMessage());
//                dlqService.moveToDlq(rawPayloadJavaBytes);
                String failureRecordKey = replayId+":"+protobufByteStringPayload;
                failedProducerEventMap.put(failureRecordKey,e);
            }
        }
        logger.info("finalDataMapList {}", finalDataMapList);
        if(!finalDataMapList.isEmpty()){
            List<ErrorLogDto> errorLogDtoList = new ArrayList<>();
            for(String failureRecordKey : failedProducerEventMap.keySet()){
                Exception cause = failedProducerEventMap.get(failureRecordKey);
                ErrorLogDto errorLogDto = new ErrorLogDto(
                        "Produce event avro payload decoding failure",
                        "SfGrpcListener",
                        failureRecordKey,
                        "Salesforce",
                        cause.getClass().toString(),
                        Arrays.toString(cause.getStackTrace())
                );
                errorLogDtoList.add(errorLogDto);
            }
            errorLogService.logErrorList(errorLogDtoList);
        }
        return finalDataMapList;
    }

    private Map<String, Object> decodeAvroPayloadToFinalDataMap(String schemaJson, byte[] rawPayloadJavaBytes) throws IOException {
        Map<String, Object> finalDataMap = new HashMap<>();
        Schema schema = new Schema.Parser().parse(schemaJson);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(rawPayloadJavaBytes, null);

        GenericRecord record = reader.read(null, decoder);

        logger.info("record {}",record);

        GenericRecord header = (GenericRecord) record.get("ChangeEventHeader");
        logger.info("header {}", header);

        String changeEvent = header.get("changeType").toString();
        ChangeType changeType =  ChangeType.valueOf(changeEvent);

        finalDataMap.put("changeType",changeEvent);
        logger.info("changeEventName {}", schema.getName());
        finalDataMap.put("changeEventName", schema.getName() );

        Map<String, Object> dataMap = new HashMap<>();
        dataMap = populateDataMapBasedOnChangeEvent(changeType, header, record, schema);

        logger.info("dataMap before id {}", dataMap);
        for(Object recordId : (List<?>) header.get("recordIds")){
            dataMap.put("Id",recordId.toString());
            if(!dataMap.containsKey("IsDeleted")) { dataMap.put("IsDeleted", false); }
            logger.info("dataMap {}", dataMap);
            finalDataMap.put("dataMap",dataMap);
        }
        return finalDataMap;

    }

    private Map<String, Object>  populateDataMapBasedOnChangeEvent(ChangeType changeType, GenericRecord header, GenericRecord record, Schema schema){
        Map<String, Object> dataMap = new HashMap<>();
            List<String> fieldNames = new ArrayList<>();

            if (ChangeType.CREATE.equals(changeType)) {
                logger.info("change type create");
                logger.info("fields from record {}", record.getSchema().getFields());
                for(Schema.Field field : schema.getFields()){
    //                    logger.info("field {}", field);
    //                    logger.info("field.name.toString {}", field.name().toString());
    //                    logger.info("field value {}",record.get(field.name().toString()));
                    String fieldName = field.name().toString();
                    if("ChangeEventHeader".equals(fieldName)){ continue; }
                    fieldNames.add(fieldName);
                }
            } else if (ChangeType.UPDATE.equals(changeType)){
                Set<String> updatedFieldCategories = Set.of("changedFields");//,"nulledFields");
                for(String updatedFieldCategory : updatedFieldCategories){
                    fieldNames.addAll(convertBitmapToFieldNames(header, updatedFieldCategory,schema));
                }
            } else if (ChangeType.DELETE.equals(changeType)){
                logger.info("change type delete");
                dataMap.put("IsDeleted", true);
            }
            logger.info("fieldNames {}", fieldNames);
            dataMap =getDataMapFromAvroPayload(fieldNames, record, dataMap);

            return dataMap;
    }

    public Map<String, Object> getDataMapFromAvroPayload(List<String> fieldNames, GenericRecord avroPayload, Map<String, Object> dataMap){
        for(String fieldName : fieldNames){
            Object value = avroPayload.get(fieldName);
            if(value == null) { continue;}
            if(value instanceof CharSequence) { value = value.toString(); }
            dataMap.put(fieldName, value);
        }
        logger.info("dataMap {}", dataMap);
        return dataMap;
    }

    public List<String> convertBitmapToFieldNames(GenericRecord header, String fieldHeaderName, Schema schema){
        List<String> fieldNamesList = new ArrayList<>();
        List<Schema.Field> fields = schema.getFields();
        List<?> bitmapList = (List<?>) header.get(fieldHeaderName);
        if(bitmapList == null || bitmapList.isEmpty()) return fieldNamesList;
        logger.info("bitmapList {}",bitmapList);
        for(int i=0; i<bitmapList.size(); i++){
            // 0x23
            // 0010 0011
            Object hexStringGenericObject = bitmapList.get(i);
            logger.info("hexStringGenericObject {}",hexStringGenericObject);
            String rawHexWithIndex = hexStringGenericObject.toString().trim();
            int blockIndex = i;
            String rawHex="";
            if(rawHexWithIndex.contains("-")){
                // blockIndex = Integer.parseInt(rawHexWithIndex.substring(0,rawHexWithIndex.indexOf("-")));
                blockIndex = i;
                rawHex=  rawHexWithIndex.substring(rawHexWithIndex.indexOf("-")+1);
            }else{
                rawHex=  rawHexWithIndex;
                blockIndex = i;
            }
            String bitmapHexString = rawHex.startsWith("0x")? rawHex.substring(2):rawHex;
            logger.info("bitmapHexString {}",bitmapHexString);
            Long bitmapLong = Long.parseLong(bitmapHexString,16);
            logger.info("bitmapLong {}",bitmapLong);
            for(int j=0;j<fields.size();j++){
                if((bitmapLong & (1L << j)) != 0 ){
                    logger.info("schemaIndex {} and total index {}",j, (i*32)+j);
                    logger.info("fieldName {}", fields.get((i*32)+j).name());
                    fieldNamesList.add(fields.get((i*32)+j).name());
                }
            }
        }
        return fieldNamesList;
    }

//    public void checkAndRefill(String topicName, int pendingNumRequested,  ByteString replayId, int numRequested){
//        try{
//            if(pendingNumRequested > 0 || fetchRequestStreamObserver == null){ return;}
//            FetchRequest fetchRefillRequest = buildRefillFetchRequest(topicName, numRequested, replayId);
//            fetchRequestStreamObserver.onNext(fetchRefillRequest);
//        } catch(Exception e){
//            logger.error("error in checkAndRefill {}",e.getMessage());
//            throw new SfSyncException(e.getMessage());
//        }
//    }

//    public FetchRequest buildRefillFetchRequest(String topicName, int numRequested, ByteString replayId){
//        try{
//            if(replayId == null){ return buildRefillFetchRequest(topicName, numRequested); }
//            return FetchRequest.newBuilder()
//                    .setTopicName(topicName)
//                    .setNumRequested(numRequested)
//                    .setReplayPreset(ReplayPreset.CUSTOM)
//                    .setReplayId(replayId)
//                    .build();
//        } catch(Exception e){
//            logger.error("error in buildRefillFetchRequest using last replayid {}",e.getMessage());
//            throw new SfSyncException(e.getMessage());
//        }
//    }

//    public FetchRequest buildRefillFetchRequest(String topicName, int numRequested){
//        try{
//            return FetchRequest.newBuilder()
//                    .setTopicName(topicName)
//                    .setNumRequested(numRequested)
//                    .setReplayPreset(ReplayPreset.LATEST)
//                    .build();
//        } catch (Exception e) {
//            logger.info("error in buildRefillRequest no replayid {}", e.getMessage());
//            throw new SfSyncException(e.getMessage());
//        }
//    }

    public void restartSubsrciption(){
        sfGrpcClient.restartSubsrciption(this.topicName, this.numRequested, this.lastReplayId);
    }


    public void restartSubsrciptionWithDelay(Long delay){
        sfGrpcClient.retryCdcStateIncerement(topicName, numRequested);
        delay =delay != null ? delay : 60;
        threadPoolTaskScheduler.schedule( this::restartSubsrciption, Instant.now().plusSeconds(delay));
    }
}
