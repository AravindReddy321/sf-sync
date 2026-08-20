package com.dev.sfsync.handler;

import com.dev.sfsync.dao.Account;
import com.dev.sfsync.dto.AccountDto;
import com.dev.sfsync.exception.SfSyncException;
import com.dev.sfsync.service.SfSyncService;
import tools.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;

@Component("AccountChangeEvent")
public class AccountCdcHandler implements CdcHandler {
    private static final Logger logger = LoggerFactory.getLogger(AccountCdcHandler.class);

    private final SfSyncService sfSyncService;
    private final ObjectMapper objectMapper;

    public AccountCdcHandler(SfSyncService sfSyncService,  ObjectMapper objectMapper) {
        this.sfSyncService = sfSyncService;
        this.objectMapper = objectMapper;
    }

    @Override
    public void processEventsData(List<Map<String,Object>> finalDataMapList){
        try{
            List<AccountDto> accountDtoCreateList = new ArrayList<>();
            List<AccountDto> accountDtoUpdateList = new ArrayList<>();
            Map<String, Object> updatedDataMap = new HashMap<>();
            for(Map<String, Object> finalDataMap : finalDataMapList){
                Map<String, Object> dataMap = (Map<String, Object>)finalDataMap.get("dataMap");
                String sfId = (String)dataMap.get("Id");
                logger.info("change type class {}",finalDataMap.get("changeType").getClass().getName());
                String changeType = (String) finalDataMap.get("changeType");
                logger.info("change type {}",changeType);
                if("CREATE".equals(changeType)){
                    accountDtoCreateList.add(objectMapper.convertValue(dataMap,AccountDto.class));
                } else if("UPDATE".equals(changeType) || "DELETE".equals(changeType)){
                    updatedDataMap.put(sfId, dataMap);
                }
            }
            logger.info("updatedDataMap {}", updatedDataMap);
            if(updatedDataMap != null && !updatedDataMap.isEmpty()){
                accountDtoUpdateList.addAll(buildAccountDtoUpdateEvent(updatedDataMap));
            }

            if(accountDtoCreateList != null && !accountDtoCreateList.isEmpty()){
                logger.info("accountDtoCreateList {}",accountDtoCreateList);
                syncAccounts(accountDtoCreateList);
            }
            if(accountDtoUpdateList != null && !accountDtoUpdateList.isEmpty()){
                logger.info("updatedAccountDtoList {}", accountDtoUpdateList);
                syncAccounts(accountDtoUpdateList);
            }
        } catch (Exception e) {
            logger.error("error in processEventsData {}",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    public List<AccountDto> buildAccountDtoUpdateEvent(Map<String, Object> updatedDataMap){
        try{
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
        } catch (Exception e){
            logger.error("error in buildAccountDtoUpdateEvent {}",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    public void syncAccounts(List<AccountDto> accountDtoList){
        try{
            sfSyncService.syncAccounts(accountDtoList);
        } catch(Exception e){
            logger.error("error in syncAccounts {}",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }
}
