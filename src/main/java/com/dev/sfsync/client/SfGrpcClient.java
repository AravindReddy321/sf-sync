package com.dev.sfsync.client;

import com.dev.sfsync.dao.Account;
import com.dev.sfsync.dto.AccountDto;
import com.dev.sfsync.exception.SfSyncException;
import com.dev.sfsync.handler.CdcHandler;
import com.dev.sfsync.service.SfSyncService;
import com.dev.sfsync.utility.SfGrpcListener;
import com.google.protobuf.ByteString;
import com.salesforce.eventbus.protobuf.*;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


@Component
public class SfGrpcClient {

    private static final Logger logger = LoggerFactory.getLogger("SfSyncClient.class");
    private enum ChangeType{ UPDATE, CREATE, DELETE }

//    private final SfSyncClient sfSyncClient;
    private final SfSyncService sfSyncService;
    private final ObjectMapper objectMapper;

    private final PubSubGrpc.PubSubStub asyncStubAuthenticated;
    private final PubSubGrpc.PubSubBlockingStub blockingStubAuthenticated;

    private final Map<String, CdcHandler> cdcHandlerMap;

//    private final String topicName;
    private static ConcurrentHashMap<String, String> schemaMap = new ConcurrentHashMap<>();
    private static List<ProducerEvent> failedProducerEventList = new ArrayList<>();

    private Map<String,StreamObserver<FetchRequest>> fetchRequestStreamObserverMap = new ConcurrentHashMap<>();


    public SfGrpcClient(SfSyncService sfSyncService, ObjectMapper objectMapper, PubSubGrpc.PubSubStub asyncStubAuthenticated, PubSubGrpc.PubSubBlockingStub blockingStubAuthenticated, Map<String, CdcHandler> cdcHandlerMap){

        this.sfSyncService = sfSyncService;
        this.objectMapper = objectMapper;
        this.cdcHandlerMap =cdcHandlerMap;
//        String accessToken = "Bearer "+getAccessToken();

//        this.asyncStub = asyncStub; //unauthenticated
//        this.blockingStub = blockingStub; //unauthenticated
//        Metadata headers = new Metadata();
//
//        Metadata.Key<String> authKey = Metadata.Key.of("accesstoken",Metadata.ASCII_STRING_MARSHALLER);
//        Metadata.Key<String> instanceUrl = Metadata.Key.of("instanceurl",Metadata.ASCII_STRING_MARSHALLER);
//        Metadata.Key<String> tenantid = Metadata.Key.of("tenantid",Metadata.ASCII_STRING_MARSHALLER);
//
//        headers.put(authKey,accessToken);
//        headers.put(instanceUrl,sfUri);
//        headers.put(tenantid,sfOrgId);


//        this.asyncStub = asyncStub.withInterceptors(
//                MetadataUtils.newAttachHeadersInterceptor(headers)
//        );
        this.asyncStubAuthenticated = asyncStubAuthenticated;

//        this.blockingStub = blockingStub.withInterceptors(
//                MetadataUtils.newAttachHeadersInterceptor(headers)
//        );
        this.blockingStubAuthenticated =blockingStubAuthenticated;

//        this.topicName = topicName;

    }

//    private String getAccessToken(){
//        return this.sfSyncClient.getAccessToken();
//    }


    public TopicRequest buildTopicRequest(String topicName){
        return TopicRequest.newBuilder()
                .setTopicName(topicName)
                .build();
    }

    public void getTopicInfo(String topicName){
        TopicRequest topicRequest = buildTopicRequest(topicName);
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
        try{
            if(fetchRequestStreamObserverMap.containsKey(topicName)){
                fetchRequestStreamObserverMap.remove(topicName);
                startCdcSubscription(topicName, numRequested, replayId);
            }
        } catch (Exception e) {
            logger.info("error in restartSubsrciption no replayid {}", e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
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
                failedProducerEventList.add(producerEvent);
                // throw new RuntimeException(e);
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
                String schemaJson = schemaInfo.getSchemaJson();
//                schemaMap.put(id, schemaJson);
                return schemaJson;
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

            GenericRecord record = reader.read(null, decoder);

            logger.info("record {}",record);

            GenericRecord header = (GenericRecord) record.get("ChangeEventHeader");
            logger.info("header {}", header);

            String changeEvent = header.get("changeType").toString();
            ChangeType changeType =  ChangeType.valueOf(changeEvent);

            finalDataMap.put("changeType",changeType);
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
            throw new SfSyncException(e.getMessage());
        }
        return finalDataMap;

    }

    private Map<String, Object>  populateDataMapBasedOnChangeEvent(ChangeType changeType, GenericRecord header, GenericRecord record, Schema schema){
        Map<String, Object> dataMap = new HashMap<>();
        List<String> fieldNames = new ArrayList<>();

        if(ChangeType.UPDATE.equals(changeType)){
            logger.info("change type update");
        }

        // if("UPDATE".equals(header.get("changeType").toString())){
        if(ChangeType.UPDATE.equals(changeType)){
                /*for(String changedFieldName : changedFieldNames){
                    if("LastModifiedDate".equals(changedFieldName)){ continue;}
                    Object value = record.get(changedFieldName);
                    if(value != null){
                        if(value instanceof CharSequence){ value = value.toString(); }
                        dataMap.put(changedFieldName, value);
                    }
                }
//                dataMap.put("Id","001Hs00005uB0A0IAK");
                logger.info("changed record Ids {}", header.get("recordIds"));
                logger.info("changed record Ids class name {}", header.get("recordIds").getClass().getName());

                logger.info("dataMap {}", dataMap);
                ObjectMapper mapper = new ObjectMapper();*/
            Set<String> updatedFieldCategories = Set.of("changedFields");//,"nulledFields");
            for(String updatedFieldCategory : updatedFieldCategories){
                fieldNames.addAll(convertBitmapToFieldNames(header, updatedFieldCategory,schema));
            }
        } else if (ChangeType.CREATE.equals(changeType)) {
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
        } else if (ChangeType.DELETE.equals(changeType)){
            logger.info("change type delete");
            dataMap.put("IsDeleted", true);
        }
        logger.info("fieldNames {}", fieldNames);
        getDataMapFromAvroPayload(fieldNames, record, dataMap);

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
//            if(value == null) { continue;}
            if(value != null && value instanceof CharSequence) { value = value.toString(); }
            dataMap.put(fieldName, value);
        }
        logger.info("dataMap {}", dataMap);
//        return dataMap;
    }


    public void populateCreateOrUpdateAccountDtoList(List<Map<String,Object>> finalDataMapList, List<AccountDto> accountDtoCreateList, List<AccountDto> accountDtoUpdateList){
        Map<String, Object> updatedDataMap = new HashMap<>();
        for(Map<String, Object> finalDataMap : finalDataMapList){
            Map<String, Object> dataMap = (Map<String, Object>)finalDataMap.get("dataMap");
            String sfId = (String)dataMap.get("Id");
            logger.info("change type class {}",finalDataMap.get("changeType").getClass().getName());
            if(ChangeType.CREATE.equals(finalDataMap.get("changeType"))){
                accountDtoCreateList.add(objectMapper.convertValue(dataMap,AccountDto.class));
            } else{
                updatedDataMap.put(sfId, dataMap);
            }
        }
        logger.info("updatedDataMap {}", updatedDataMap);
        if(updatedDataMap != null && !updatedDataMap.isEmpty()){
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
                    .isDeleted(dataMap.containsKey("IsDeleted") ? (boolean)dataMap.get("IsDeleted"): acc.isDeleted)
                    .build();
            logger.info("dataMap {}",dataMap);
            logger.info("accountDto {}",accountDto);
//                                    updatedAccounts.add(objectMapper.readerForUpdating(acc)
//                                                    .readValue(objectMapper.writeValueAsString(dataMap)));
//                                    objectMapper.readerForUpdating(acc)
//                                            .readValue(objectMapper.writeValueAsString(dataMap));
//                                    logger.info("acc after update {}",acc);
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

        if(accountDtoCreateList != null && !accountDtoCreateList.isEmpty()){
            logger.info("accountDtoCreateList {}",accountDtoCreateList);
            syncAccounts(accountDtoCreateList);
        }
        if(accountDtoUpdateList != null && !accountDtoUpdateList.isEmpty()){
            logger.info("updatedAccountDtoList {}", accountDtoUpdateList);
            syncAccounts(accountDtoUpdateList);
        }
    }



    public void startCdcSubscription(String topicName, int numRequested, ByteString replayId){
        try{
            logger.info("asyncStub: {}",asyncStubAuthenticated);
            logger.info("blockingStub: {}",blockingStubAuthenticated);

            /*StreamObserver<FetchRequest>[] fetchRequestStreamObserver = new StreamObserver[1];
            fetchRequestStreamObserver[0]= asyncStub.subscribe(
                    new StreamObserver<FetchResponse>() {
                        @Override
                        public void onNext(FetchResponse value) {
                            logger.info("value: {}"+value);

                            List<ConsumerEvent> consumerEventList = new ArrayList<>();
                            List<ProducerEvent> producerEventList = new ArrayList<>();

                            consumerEventList.addAll(value.getEventsList());
                            consumerEventList.forEach(event -> producerEventList.add(event.getEvent()));

                            processProducerEvents(producerEventList);

                            logger.info("value get pending num requested {}", value.getPendingNumRequested());
                            checkAndRefill(topicName, value.getPendingNumRequested(), value.getLatestReplayId(), numRequested);
                        }

                        @Override
                        public void onError(Throwable t) {

                        }

                        @Override
                        public void onCompleted() {
                            logger.info("onCompleted");

                        }
                    }
            );

            FetchRequest fetchRequest = buildFetchRequest(topicName, numRequested);
            fetchRequestStreamObserver[0].onNext(fetchRequest);*/
            StreamObserver<FetchRequest> fetchRequestStreamObserver = asyncStubAuthenticated.subscribe(new SfGrpcListener(this, cdcHandlerMap, topicName, numRequested));
            fetchRequestStreamObserverMap.put(topicName, fetchRequestStreamObserver);
            fetchRequestStreamObserver.onNext(buildFetchRequest(topicName, numRequested, replayId));
        } catch (Exception e) {
            throw new SfSyncException(e.getMessage());
        }

    }

}
