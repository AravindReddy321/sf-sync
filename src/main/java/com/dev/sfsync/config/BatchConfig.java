package com.dev.sfsync.config;

import com.dev.sfsync.exception.SfSyncException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class BatchConfig {
    private static final Logger logger = LoggerFactory.getLogger(BatchConfig.class);

    public String[] getDtoRecordClassFieldNames(Class<?> clazz){
        try{
            return Arrays.stream(clazz.getRecordComponents())
                    .map(RecordComponent::getName)
                    .toArray(String[]::new);
        } catch (Exception e) {
            logger.error("error in getDtoRecordClassFieldNames {} for class {}", e.getMessage(),clazz.getName());
            throw new SfSyncException(e.getMessage());
        }
    }

    @Bean
    public Map<String, Resource> jobParamMapBean(){
        return new HashMap<>();
    }

    @Bean
    public JobExecutionListener jobExecutionListener(){
        return new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                JobExecutionListener.super.beforeJob(jobExecution);
                logger.info("inside before job JobExecutionListener {}",jobExecution);
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
                JobExecutionListener.super.afterJob(jobExecution);
                logger.info("inside after job JobExecutionListener {}", jobExecution);
            }
        };
    }

}
