package com.dev.sfsync.utility;

import com.dev.sfsync.client.SfGrpcClient;
import com.dev.sfsync.exception.SfSyncException;
import com.dev.sfsync.handler.CdcHandler;
import com.google.protobuf.ByteString;
import com.salesforce.eventbus.protobuf.*;
import io.grpc.stub.StreamObserver;
import lombok.Data;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Data
public class SfGrpcListener implements StreamObserver<FetchResponse> {

    private static final Logger logger = LoggerFactory.getLogger(SfGrpcListener.class);

    private final Map<String, CdcHandler> cdcHandlerMap = new HashMap<>();
    private final SfGrpcClient sfGrpcClient;
    private enum ChangeType { CREATE, UPDATE, DELETE };

    private final String topicName;
    private final int numRequested;
    private ByteString lastReplayId;

    public SfGrpcListener(SfGrpcClient sfGrpcClient, Map<String, CdcHandler> cdcHandlerMap, String topicName, int numRequested) {
        this.cdcHandlerMap.putAll(cdcHandlerMap);
        this.sfGrpcClient = sfGrpcClient;
        this.topicName = topicName;
        this.numRequested = numRequested;
    }


    public void onNext(FetchResponse value){
        try{
            logger.info("value: {}"+value);

            List<ConsumerEvent> consumerEventList = new ArrayList<>();
            List<ProducerEvent> producerEventList = new ArrayList<>();

            consumerEventList.addAll(value.getEventsList());
            consumerEventList.forEach(event -> producerEventList.add(event.getEvent()));

            processProducerEvents(producerEventList);

            logger.info("value get pending num requested {}", value.getPendingNumRequested());
            lastReplayId = value.getLatestReplayId();
            int pendingNumRequested= value.getPendingNumRequested();
            if(pendingNumRequested < numRequested){
                sfGrpcClient.refillFetchRequest(topicName,numRequested,lastReplayId);
            }
        } catch (SfSyncException e) {
            logger.error("error in onNext {}", e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    public void onError(Throwable t){
        try{
            logger.info("in onError t {}",t.getClass().getName());
            logger.info("from logger.info t.toString() {}",t.toString());
            logger.error("from logger.error t.toString() {}",t.toString());
            Thread.currentThread().sleep(5000);
            restartSubsrciption();
        } catch (InterruptedException e){
            logger.error("error in onError thread sleep {}",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    public void onCompleted() {
        try{
            logger.info("in onCompleted");
            Thread.currentThread().sleep(5000);
            restartSubsrciption();
        } catch(InterruptedException e){
            logger.error("error in onCompleted thread sleep {}",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    public void processProducerEvents(List<ProducerEvent> producerEventList){
        try{
            List<Map<String,Object>> finalDataMapList = new ArrayList<>();
            finalDataMapList.addAll(getFinalDataMapList(producerEventList));
            logger.info("finalDataMapList from processProducerEvents {}", finalDataMapList);
            String changeEventName =(String)finalDataMapList.get(0).get("changeEventName");
            if(!cdcHandlerMap.containsKey(changeEventName)){ return; }
            CdcHandler cdcHandler = cdcHandlerMap.get(changeEventName);
            cdcHandler.processEventsData(finalDataMapList);
        }  catch (SfSyncException e) {
            logger.error("error in processProducerEvents {}", e.getMessage());
            throw new SfSyncException(e.getMessage());
        }

    }

    public List<Map<String,Object>>  getFinalDataMapList(List<ProducerEvent> producerEventList){
        List<Map<String,Object>> finalDataMapList = new ArrayList<>();
        try{
            for(ProducerEvent producerEvent : producerEventList){
                String schemaId = producerEvent.getSchemaId();
                String schemaJson = sfGrpcClient.getAvroSchemaJson(schemaId);
                logger.info("schemaJson {}",schemaJson);
                ByteString protobufByteString = producerEvent.getPayload();
                logger.info("protobuf ByteString {}",protobufByteString);
                byte[] rawPayloadJavaBytes = protobufByteString.toByteArray();
                logger.info("rawPayloadJavaBytes {}",rawPayloadJavaBytes);
                finalDataMapList.add(decodeAvroPayloadToFinalDataMap(schemaJson, rawPayloadJavaBytes));
            }
        } catch (Exception e) {
            logger.error("error in getFinalDataMapList {}",e.getMessage());
            // failedProducerEventList.add(producerEvent);
             throw new SfSyncException(e.getMessage());
        }
        logger.info("finalDataMapList {}", finalDataMapList);
        return finalDataMapList;
    }

    private Map<String, Object> decodeAvroPayloadToFinalDataMap(String schemaJson, byte[] rawPayloadJavaBytes){
        Map<String, Object> finalDataMap = new HashMap<>();
        try{
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

        } catch (Exception e) {
            logger.error("error in decodeAvroPayloadToFinalDataMap {}",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
        return finalDataMap;

    }

    private Map<String, Object>  populateDataMapBasedOnChangeEvent(ChangeType changeType, GenericRecord header, GenericRecord record, Schema schema){
        Map<String, Object> dataMap = new HashMap<>();
        try{
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
            getDataMapFromAvroPayload(fieldNames, record, dataMap);

            return dataMap;
        } catch (Exception e) {
            logger.error("error in populateDataMapBasedOnChangeEvent {}",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    public void getDataMapFromAvroPayload(List<String> fieldNames, GenericRecord avroPayload, Map<String, Object> dataMap){
        try{
        for(String fieldName : fieldNames){
            Object value = avroPayload.get(fieldName);
//            if(value == null) { continue;}
            if(value != null && value instanceof CharSequence) { value = value.toString(); }
            dataMap.put(fieldName, value);
        }
        logger.info("dataMap {}", dataMap);
//        return dataMap;
       } catch (Exception e) {
            logger.error("error in getDataMapFromAvroPayload {}",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    public List<String> convertBitmapToFieldNames(GenericRecord header, String fieldHeaderName, Schema schema){
        try{
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
                for(int j=0;j<32;j++){
                    if((bitmapLong & (1L << j)) != 0 ){
                        logger.info("schemaIndex {} and total index {}",j, (i*32)+j);
                        logger.info("fieldName {}", fields.get((i*32)+j).name());
                        fieldNamesList.add(fields.get((i*32)+j).name());
                    }
                }
            }
            return fieldNamesList;
        } catch (Exception e) {
            logger.error("error in getDataMapFromAvroPayload {}",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
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

    public FetchRequest buildRefillFetchRequest(String topicName, int numRequested, ByteString replayId){
        try{
            if(replayId == null){ return buildRefillFetchRequest(topicName, numRequested); }
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

    public FetchRequest buildRefillFetchRequest(String topicName, int numRequested){
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

    public void restartSubsrciption(){
        sfGrpcClient.restartSubsrciption(this.topicName, this.numRequested, this.lastReplayId);
    }
}
