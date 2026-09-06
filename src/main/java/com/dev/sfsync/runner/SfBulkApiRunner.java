package com.dev.sfsync.runner;

import com.dev.sfsync.client.SfBulkApiClient;
import com.dev.sfsync.dao.Account;
import com.dev.sfsync.dto.AccountDto;
import com.dev.sfsync.dto.CaseDto;
import com.dev.sfsync.exception.SfSyncException;
import com.dev.sfsync.service.CaseSyncService;
import com.dev.sfsync.service.SfSyncService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;

@Component
public class SfBulkApiRunner implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(SfBulkApiRunner.class);
    private final SfBulkApiClient sfBulkApiClient;
    private final SfSyncService sfSyncService;
    private final CaseSyncService caseSynService;

    public SfBulkApiRunner(SfBulkApiClient sfBulkApiClient, SfSyncService sfSyncService, CaseSyncService caseSyncService) {
        this.sfBulkApiClient = sfBulkApiClient;
        this.sfSyncService = sfSyncService;
        this.caseSynService = caseSyncService;
    }

    public String[] getDtoFieldNames(Class<?> clazz){
        return Arrays.stream(clazz.getRecordComponents())
                .map(RecordComponent::getName)
                .toArray(String[]::new);
    }

    public String buildAccountLastModDateCondition(){
        return "lastModifiedDate >"+sfSyncService.getLastSyncTime();
    }

    public String getAccountSelectStatement(){
        String selectStatementFields = String.join(",",getDtoFieldNames(AccountDto.class));
        return "SELECT %s FROM Account".formatted(selectStatementFields);
    }

    public String getCaseSelectStatement(){
        String selectedFields = String.join(",", getDtoFieldNames(CaseDto.class));
        return "SELECT %s from Case".formatted(selectedFields);
    }

    public String buildRecentCaseClosedDateCondition(){
        return "closedDate >"+caseSynService.getRecentCaseClosedDate();
    }

    @Override
    public void run(String... args) throws Exception {

//        List<String> selectedFieldsList = List.of("Id", "Name", "Description");
//        String selectedFields = String.join(",", getAccountDtoFieldNames());
//        String accountQuery = "SELECT %s FROM Account";
//        accountQuery = accountQuery.formatted(selectedFields);

        String accountQueryWithWhereCond = getAccountSelectStatement() + " WHERE " + buildAccountLastModDateCondition();

//        logger.info("accountQuery {}", accountQuery);
        logger.info("accountQueryWithWhereCond {}", accountQueryWithWhereCond);

        sfBulkApiClient.buildBuilkJobQueryBody(accountQueryWithWhereCond);
        logger.info("Account dto reflection {}",AccountDto.class.getRecordComponents().toString());
        for( RecordComponent recordComponent :AccountDto.class.getRecordComponents()) {
            logger.info("accountdto record component name {}",recordComponent.getName());
        }

        List<String> fieldNamesList = Arrays.stream(AccountDto.class.getRecordComponents()).map( RecordComponent::getName).toList();

        logger.info("fieldNamesList {}", fieldNamesList);

        sfBulkApiClient.submitBulkQuery(accountQueryWithWhereCond,"Account");

        invokeCaseBulkJob();
    }

    public void invokeCaseBulkJob(){
        try{
            logger.info("inside invokeCaseBulkJob");
            String caseQueryWithWhereCond = getCaseSelectStatement()+" WHERE "+ buildRecentCaseClosedDateCondition();
            sfBulkApiClient.submitBulkQuery(caseQueryWithWhereCond, "Case");
        } catch(Exception e){
            logger.error("Exception occurred while invoke caseBulkJob {}",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }
}
