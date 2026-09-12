package com.dev.sfsync.client;

import com.dev.sfsync.exception.SfSyncException;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.TaskExecutorJobOperator;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;

@Component
//reviewed
public class SfBulkApiClient {

    private static final Logger logger = LoggerFactory.getLogger(SfBulkApiClient.class);
    private static final String JOB_PARAM_MAP_BEAN_NAME="jobParamMapBean";
    private static final String JOB_COMPLETE_STATUS="JobComplete";
    private static final String UPLOAD_COMPLETE_STATUS="UploadComplete";
    private static final String IN_PROGRESS_STATUS="InProgress";

    private final String salesforceBaseEndpoint;
    private final String  accessToken;
    private final SfSyncClient sfSyncClient;
    private final ApplicationContext applicationContext;
    private final JobOperator jobOperator;
    private final TaskExecutorJobOperator taskExecutorJobOperator;
    private final Job accountsSyncBatchJob;
    private final Job casesSyncBatchJob;
    private final ThreadPoolTaskScheduler threadPoolTaskScheduler;
    private final TaskExecutor taskExecutor;

    public SfBulkApiClient(String sfUriBean, String accessTokenBean, SfSyncClient sfSyncClient, ApplicationContext applicationContext, JobOperator jobOperator, TaskExecutorJobOperator taskExecutorJobOperator, Job accountsSyncBatchJob, Job casesSyncBatchJob, ThreadPoolTaskScheduler threadPoolTaskScheduler, ThreadPoolTaskExecutor threadPoolTaskExecutor) {
        this.salesforceBaseEndpoint = sfUriBean;
        this.accessToken = accessTokenBean;
        this.sfSyncClient = sfSyncClient;
        this.applicationContext = applicationContext;
        this.jobOperator = jobOperator;
        this.taskExecutorJobOperator = taskExecutorJobOperator;
        this.accountsSyncBatchJob = accountsSyncBatchJob;
        this.casesSyncBatchJob = casesSyncBatchJob;
        this.threadPoolTaskScheduler = threadPoolTaskScheduler;
        this.taskExecutor = threadPoolTaskExecutor;

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

        logger.info("inside submitBulkQuery");
        logger.info("access token {}", accessToken);

        Map<?, ?> bulkQueryResponseMap = sfSyncClient.submitPostApiRequest(getSubmitBulkJobUrl(), buildBuilkJobQueryBody(query));

        logger.info("bulkQueryResponseMap {}", bulkQueryResponseMap);

        String bulkJobId = (String)bulkQueryResponseMap.get("id");
        logger.info("bulkJobId {}", bulkJobId);
//        String bulkJobStatus = "";
//            while(!JOB_COMPLETE_STATUS.equals(bulkJobStatus)){
//                Map<?, ?> bulkJobStatusResponseMap = getBulkJobStatus(bulkJobId);
//                logger.info("bulkJobStatusResponseMap {}", bulkJobStatusResponseMap);
//                bulkJobStatus = (String)bulkJobStatusResponseMap.get("state");
//                logger.info("bulkJobStatus {}", bulkJobStatus);
//                Thread.sleep(2000);
//            }
        processBulkJobResults(bulkJobId,sfObjectName);

    }

    public void processBulkJobResults(String bulkJobId, String sfObjectName){
        String jobStatus = getBulkJobStatus(bulkJobId);
        logger.info("jobStatus {}", jobStatus);
        if(IN_PROGRESS_STATUS.equalsIgnoreCase(jobStatus) || UPLOAD_COMPLETE_STATUS.equalsIgnoreCase(jobStatus)){
            threadPoolTaskScheduler.schedule(()->{
                logger.info("scheduling job {}",bulkJobId);
                processBulkJobResults(bulkJobId, sfObjectName);
            },  Instant.now().plusSeconds(2));
        }else if(JOB_COMPLETE_STATUS.equalsIgnoreCase(jobStatus)){
                getBulkJobResults(bulkJobId, sfObjectName);
        } else{
            return;
        }
    }

    public String getBulkJobStatus(String jobId){
        logger.info("inside getBulkJobStatus");
        Map<String, Object> bulkJobStatusResponseMap = sfSyncClient.submitGetApiRequest(getBulkJobCheckStatusUrl(jobId));
        logger.info("bulkJobStatusResponseMap {}", bulkJobStatusResponseMap);
        return (String)bulkJobStatusResponseMap.get("state");
    }

    @Transactional
    public void getBulkJobResults(String jobId, String sfObjectName){
        try{
            Resource resource = sfSyncClient.submitGetApiRequestCsv(getBulkJobResultsUrl(jobId));
            if( applicationContext == null || !applicationContext.containsBean(JOB_PARAM_MAP_BEAN_NAME)){ throw new SfSyncException("applicationContext is empty or jobParamMapBean is not found"); }
            applicationContext.getBean(JOB_PARAM_MAP_BEAN_NAME, Map.class).put(sfObjectName, resource);
            logger.info("bulkJobResultMap {}", applicationContext.getBean(JOB_PARAM_MAP_BEAN_NAME, Map.class));

            logger.info("starting accountsSyncBulkJob");
            JobParameters jobParameters = new JobParametersBuilder()
                                            .addString("sfObjectName", sfObjectName)
                                            .addString("runTime",Instant.now().toString())
//                                            .addString("runTime", "2026-09-08T13:52:51.802742Z") //case
//                    .addString("runTime", "2026-09-08T13:52:36.478471Z") //account

                                            .toJobParameters();
            switch (sfObjectName){
                case "Account":
//                    jobOperator.start(accountsSyncBatchJob, new JobParameters());
                    taskExecutorJobOperator.start(accountsSyncBatchJob, jobParameters);
                    break;

                case "Case":
//                    jobOperator.start(casesSyncBatchJob, new JobParameters());
                    taskExecutorJobOperator.start(casesSyncBatchJob, jobParameters);
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
