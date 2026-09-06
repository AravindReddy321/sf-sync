package com.dev.sfsync.handler;

import com.dev.sfsync.dao.Account;
import com.dev.sfsync.dto.AccountDto;
import com.dev.sfsync.dto.ErrorLogDto;
import com.dev.sfsync.service.DlqService;
import com.dev.sfsync.service.ErrorLogService;
import com.dev.sfsync.service.SfSyncService;
import org.springframework.dao.DataIntegrityViolationException;
import tools.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

@Component("AccountChangeEvent")
public class AccountCdcHandler implements CdcHandler {
    private static final Logger logger = LoggerFactory.getLogger(AccountCdcHandler.class);
    private static final String CHANGE_TYPE = "changeType";

    private final SfSyncService sfSyncService;
    private final ObjectMapper objectMapper;
    private final ErrorLogService  errorLogService;
    private final DlqService dlqService;

    public AccountCdcHandler(SfSyncService sfSyncService,  ObjectMapper objectMapper, ErrorLogService errorLogService, DlqService dlqService) {
        this.sfSyncService = sfSyncService;
        this.objectMapper = objectMapper;
        this.errorLogService = errorLogService;
        this.dlqService = dlqService;
    }

    public Map<String,Object> getCreateUpdateCombinedMap(List<Map<String,Object>> finalDataMapList){
        Map<String,Object> createUpdateCombinedMap = new HashMap<>();
        Map<String, Object> updatedDataMap = new HashMap<>();
        List<Map<String,Object>> finalDataMapListCorrupted = new ArrayList<>();
        List<AccountDto> accountDtoCreateList = new ArrayList<>();
        for(Map<String, Object> finalDataMap : finalDataMapList){
            Map<String, Object> dataMap = (Map<String, Object>)finalDataMap.get("dataMap");
            if(dataMap == null || dataMap.isEmpty() || !finalDataMap.containsKey(CHANGE_TYPE) || !dataMap.containsKey("Id")){
                finalDataMapListCorrupted.add(finalDataMap);
                continue;
            }
            String sfId = (String)dataMap.get("Id");
            logger.info("change type class {}",finalDataMap.get(CHANGE_TYPE).getClass().getName());
            String changeType = (String) finalDataMap.get(CHANGE_TYPE);
            logger.info("change type {}",changeType);
            if("CREATE".equals(changeType)){
                accountDtoCreateList.add(objectMapper.convertValue(dataMap,AccountDto.class));
            } else if("UPDATE".equals(changeType) || "DELETE".equals(changeType)){
                updatedDataMap.put(sfId, dataMap);
            }
        }
        if(!finalDataMapListCorrupted.isEmpty()){
            finalDataMapListCorrupted.forEach(dlqService::moveToDlq);
        }
        createUpdateCombinedMap.put("createList",accountDtoCreateList);
        createUpdateCombinedMap.put("updatedDataMap",updatedDataMap);
        return createUpdateCombinedMap;
    }

    @Override
    public void processEventsData(List<Map<String,Object>> finalDataMapList){
            List<AccountDto> accountDtoUpdateList = new ArrayList<>();
            List<AccountDto> accountDtoListToSync = new ArrayList<>();
            Map<String, Object> createUpdateCombinedMap = getCreateUpdateCombinedMap(finalDataMapList);
            if(createUpdateCombinedMap == null){ return; }
            List<AccountDto> accountDtoCreateList = (List<AccountDto>) createUpdateCombinedMap.getOrDefault("createList", new ArrayList<>());
            Map<String, Object>updatedDataMap = (Map<String, Object>) createUpdateCombinedMap.getOrDefault("updatedDataMap", new HashMap<>());
            logger.info("updatedDataMap {}", updatedDataMap);
            if(updatedDataMap != null && !updatedDataMap.isEmpty()){
                accountDtoUpdateList.addAll(buildAccountDtoUpdateEvent(updatedDataMap));
            }

            if(accountDtoCreateList != null && !accountDtoCreateList.isEmpty()){
                logger.info("accountDtoCreateList {}",accountDtoCreateList);
                accountDtoListToSync.addAll(accountDtoCreateList);
            }
            if(!accountDtoUpdateList.isEmpty()){
                logger.info("updatedAccountDtoList {}", accountDtoUpdateList);
                accountDtoListToSync.addAll(accountDtoUpdateList);
            }

            try{
                syncAccounts(accountDtoListToSync);
            } catch(DataIntegrityViolationException e){
                List<AccountDto> failedAccountDtoList = new ArrayList<>();
                accountDtoListToSync.forEach( acc -> {
                    if(!syncAccountsIndividually(acc)){
                        failedAccountDtoList.add(acc);
                    }
                });
                handleDataIntegrityFailedAccounts(failedAccountDtoList);
            }

    }

    public List<AccountDto> buildAccountDtoUpdateEvent(Map<String, Object> updatedDataMap){
            Set<String> sfIds = new HashSet<>(updatedDataMap.keySet());
            List<Account> accList = new ArrayList<>();
            try{
                accList = sfSyncService.fetchAccountsBySfIds(sfIds);
            } catch(Exception e){
                dlqService.moveToDlq(updatedDataMap);
                return new ArrayList<>();
            }
            logger.info("accList fetched {}",accList);
            if(sfIds.size() != accList.size()){
                accList.forEach(acc -> {
                    if(sfIds.contains(acc.getSfId())){
                        sfIds.remove(acc.getSfId());
                    }
                });
            }
            logger.info("sfIds remaning {}",sfIds);
            List<AccountDto> updatedAccountDtos = new ArrayList<>();
            sfIds.forEach(id -> updatedAccountDtos.add(objectMapper.convertValue(updatedDataMap.get(id), AccountDto.class)));
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

    public void syncAccounts(List<AccountDto> accountDtoList) {
        sfSyncService.syncAccounts(accountDtoList);
    }

    public boolean syncAccountsIndividually(AccountDto accountDto){
            return sfSyncService.syncAccount(accountDto);
    }

    public void handleDataIntegrityFailedAccounts(List<AccountDto> failedAccountDtoList){
        if(failedAccountDtoList.isEmpty()){ return; }
            List<ErrorLogDto> errorLogDtoList = new ArrayList<>();
            failedAccountDtoList.forEach(failedAccDto -> {
                ErrorLogDto errorLogDto = new ErrorLogDto(
                        "failed to sync account due to integrity",
                        "AccountCdcHandler",
                        failedAccDto.id(),
                        "Salesforce",
                        "DataIntegrityViolationException",
                        ""
                );
                errorLogDtoList.add(errorLogDto);
            });
        try{
            errorLogService.logErrorList(errorLogDtoList);
        } catch(DataIntegrityViolationException e){
            for(ErrorLogDto errorLogDto : errorLogDtoList){
                if(!errorLogService.logError(errorLogDto)){
                    dlqService.moveToDlq(errorLogDto);
                }
            }
        }
    }

}
