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
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.HashMap;
import java.util.Map;

@Component
public class SfBulkApiClient {

    private static final Logger logger = LoggerFactory.getLogger(SfBulkApiClient.class);

    private final String salesforceBaseEndpoint;
    private final String  accessToken;
    private final SfSyncClient sfSyncClient;
    private final ApplicationContext applicationContext;
    private final JobOperator jobOperator;
    private final Job accountsSyncBatchJob;
    private final Job casesSyncBatchJob;

//    private static final String BEARER = "Bearer";
//    private static final String AUTHORIZATION = "Authorization";

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
//        String queryBody = """
//                {
//                    "operation":"query",
//                    "query":"%s"
//                }
//                """;
//        queryBody = queryBody.formatted(query);
//        logger.info("queryBody {}", queryBody);

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
            String bulkJobStatus = "JobComplete";
            do{
                Map<?, ?> bulkJobStatusResponseMap = checkBulkJobStatus(bulkJobId);
                logger.info("bulkJobStatusResponseMap {}", bulkJobStatusResponseMap);
                bulkJobStatus = (String)bulkJobStatusResponseMap.get("state");
                logger.info("bulkJobStatus {}", bulkJobStatus);
                Thread.sleep(2000);
            } while(!"JobComplete".equals(bulkJobStatus));

            if(bulkJobId != null && "JobComplete".equals(bulkJobStatus)){
                getBulkJobResults(bulkJobId, sfObjectName);
            }

        } catch (Exception e) {
            logger.error("error in submitBulkQuery {}", e.getMessage());
            throw new SfSyncException(e.getMessage());
        }

    }

    public Map<?, ?> checkBulkJobStatus(String jobId){
        try{
            Map<?, ?> bulkJobStatusResponseMap = sfSyncClient.submitGetApiRequest(getBulkJobCheckStatusUrl(jobId));
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
//            Map<?, ?> bulkJobResultMap = sfSyncClient.submitGetApiRequest(getBulkJobResultsUrl(jobId));
            Resource resource = sfSyncClient.submitGetApiRequestCsv(getBulkJobResultsUrl(jobId));
            if( applicationContext == null || !applicationContext.containsBean("jobParamMapBean")){ throw new SfSyncException("applicationContext is empty or jobParamMapBean is not found"); }
            applicationContext.getBean("jobParamMapBean", Map.class).put(sfObjectName, resource);
            logger.info("bulkJobResultMap {}", applicationContext.getBean("jobParamMapBean", Map.class));

            logger.info("starting accountsSyncBulkJob");
            switch (sfObjectName){
                case "Account":
                    jobOperator.start(accountsSyncBatchJob, new JobParameters());

                case "Case":
                    jobOperator.start(casesSyncBatchJob, new JobParameters());
                    logger.info("case bulk api job should be invoked");

            }
        } catch (Exception e){
            logger.error("error in getBulkJobResults {}", e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }


}
