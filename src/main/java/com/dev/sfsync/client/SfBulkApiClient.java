package com.dev.sfsync.client;

import com.dev.sfsync.exception.SfSyncException;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class SfBulkApiClient {

    private static final Logger logger = LoggerFactory.getLogger(SfBulkApiClient.class);
    private static final String JOB_PARAM_MAP_BEAN_NAME="jobParamMapBean";
    private static final String JOB_COMPLETE_STATUS="JobComplete";

    private final String salesforceBaseEndpoint;
    private final String  accessToken;
    private final SfSyncClient sfSyncClient;
    private final ApplicationContext applicationContext;
    private final JobOperator jobOperator;
    private final Job accountsSyncBatchJob;
    private final Job casesSyncBatchJob;

    public SfBulkApiClient(String sfUriBean, String accessTokenBean, SfSyncClient sfSyncClient, ApplicationContext applicationContext, JobOperator jobOperator, Job accountsSyncBatchJob, Job casesSyncBatchJob) {
        this.salesforceBaseEndpoint = sfUriBean;
        this.accessToken = accessTokenBean;
        this.sfSyncClient = sfSyncClient;
        this.applicationContext = applicationContext;
        this.jobOperator = jobOperator;
        this.accountsSyncBatchJob = accountsSyncBatchJob;
        this.casesSyncBatchJob = casesSyncBatchJob;
    }

    public String getSubmitBulkJobUrl(){
        return salesforceBaseEndpoint+"/services/data/v60.0/jobs/query";
    }

    public String getBulkJobCheckStatusUrl(String jobId){
        return getSubmitBulkJobUrl()+"/"+jobId;
    }

    public String getBulkJobResultsUrl(String jobId){
        return getBulkJobCheckStatusUrl(jobId)+"/results";
    }

    public Map<String, String> buildBuilkJobQueryBody(String query){

        Map<String, String> queryBodyMap = new HashMap<>();
        queryBodyMap.put("operation","query");
        queryBodyMap.put("query",query);

        logger.info("queryBodyMap {}", queryBodyMap);

        return queryBodyMap;
    }

    public void submitBulkQuery(String query, String sfObjectName){
        try {

            logger.info("inside submitBulkQuery");
            logger.info("access token {}", accessToken);

            Map<?, ?> bulkQueryResponseMap = sfSyncClient.submitPostApiRequest(getSubmitBulkJobUrl(), buildBuilkJobQueryBody(query));

            logger.info("bulkQueryResponseMap {}", bulkQueryResponseMap);

            String bulkJobId = (String)bulkQueryResponseMap.get("id");
            logger.info("bulkJobId {}", bulkJobId);
            String bulkJobStatus = "";
            while(!JOB_COMPLETE_STATUS.equals(bulkJobStatus)){
                Map<?, ?> bulkJobStatusResponseMap = checkBulkJobStatus(bulkJobId);
                logger.info("bulkJobStatusResponseMap {}", bulkJobStatusResponseMap);
                bulkJobStatus = (String)bulkJobStatusResponseMap.get("state");
                logger.info("bulkJobStatus {}", bulkJobStatus);
                Thread.sleep(2000);
            }

            if(bulkJobId != null && !bulkJobStatus.isBlank()){
                getBulkJobResults(bulkJobId, sfObjectName);
            }

        } catch (Exception e) {
            logger.error("error in submitBulkQuery {}", e.getMessage());
            Thread.currentThread().interrupt();
            throw new SfSyncException(e.getMessage());
        }

    }

    public Map<String, Object> checkBulkJobStatus(String jobId){
        try{
            Map<String, Object> bulkJobStatusResponseMap = sfSyncClient.submitGetApiRequest(getBulkJobCheckStatusUrl(jobId));
            logger.info("bulkJobStatusResponseMap {}", bulkJobStatusResponseMap);
            return bulkJobStatusResponseMap;
        } catch(Exception e){
            logger.error("error in checkBulkJobStatus {}", e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    @Transactional
    public void getBulkJobResults(String jobId, String sfObjectName){
        try{
            Resource resource = sfSyncClient.submitGetApiRequestCsv(getBulkJobResultsUrl(jobId));
            if( applicationContext == null || !applicationContext.containsBean(JOB_PARAM_MAP_BEAN_NAME)){ throw new SfSyncException("applicationContext is empty or jobParamMapBean is not found"); }
            applicationContext.getBean(JOB_PARAM_MAP_BEAN_NAME, Map.class).put(sfObjectName, resource);
            logger.info("bulkJobResultMap {}", applicationContext.getBean(JOB_PARAM_MAP_BEAN_NAME, Map.class));

            logger.info("starting accountsSyncBulkJob");
            switch (sfObjectName){
                case "Account":
                    jobOperator.start(accountsSyncBatchJob, new JobParameters());
                    break;

                case "Case":
                    jobOperator.start(casesSyncBatchJob, new JobParameters());
                    logger.info("case bulk api job should be invoked");
                    break;

                default:
                    return;

            }
        } catch (Exception e){
            logger.error("error in getBulkJobResults {}", e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }


}
