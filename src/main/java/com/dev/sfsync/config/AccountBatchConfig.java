package com.dev.sfsync.config;

import com.dev.sfsync.dao.Account;
import com.dev.sfsync.dao.AccountMapper;
import com.dev.sfsync.dto.AccountDto;
import com.dev.sfsync.exception.SfSyncException;
import jakarta.persistence.EntityManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.ItemStreamReader;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.batch.infrastructure.item.database.JpaItemWriter;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.LineMapper;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.infrastructure.item.file.mapping.RecordFieldSetMapper;
import org.springframework.batch.infrastructure.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.lang.reflect.RecordComponent;
import java.util.*;

@Configuration
public class AccountBatchConfig {
//    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final Logger logger = LoggerFactory.getLogger(AccountBatchConfig.class);
    private final AccountMapper accountMapper;
    private final BatchConfig batchConfig;

    public AccountBatchConfig(AccountMapper accountMapper, BatchConfig batchConfig) {
        this.accountMapper = accountMapper;
        this.batchConfig = batchConfig;
    }

//    public String[] getRecordClassFieldNames(Class<?> clazz) {
//        return Arrays.stream(clazz.getRecordComponents())
//                .map(RecordComponent::getName)
//                .toArray(String[]::new);
////                .toList();
////                .collect(Collectors.toSet());
//    }

//    @Bean
//    public Map<String, Resource> jobParamMapBean(){
//        return new HashMap<>();
//    }

    @Bean
    @StepScope
    public LineMapper<AccountDto> accountDtoLineMapper(){
        try{
            logger.info("inside accountDtoLineMapper");
            DefaultLineMapper<AccountDto> accountDtoLineMapper = new DefaultLineMapper<>();

            String[] accountDtoFieldNames = batchConfig.getDtoRecordClassFieldNames(AccountDto.class);
            DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
            lineTokenizer.setNames(accountDtoFieldNames);
            lineTokenizer.setStrict(false);
            lineTokenizer.setQuoteCharacter('"');

            RecordFieldSetMapper<AccountDto> recordFieldSetMapper = new RecordFieldSetMapper<>(AccountDto.class);

            accountDtoLineMapper.setLineTokenizer(lineTokenizer);
            accountDtoLineMapper.setFieldSetMapper(recordFieldSetMapper);
            logger.info("accountDtoLineMapper {}",accountDtoLineMapper.toString());
            return accountDtoLineMapper;
        } catch (Exception e){
            logger.error("error occured during line mapper {}",e.getMessage());
            throw new SfSyncException("error occured during line mapper "+e.getMessage());
        }
    }

    @Bean
    @StepScope
    public FlatFileItemReader<AccountDto> accountDtoItemReader(@Value("#{jobParamMapBean['Account']}") Resource accountCsvResource, LineMapper<AccountDto> accountDtoLineMapper){

        try {
            logger.info("inside accountDtoReader accountCsvResource {}",accountCsvResource);
            return new FlatFileItemReaderBuilder<AccountDto>()
                    .lineMapper(accountDtoLineMapper)
                    .resource(accountCsvResource)
                    .saveState(false)
                    .linesToSkip(1)
                    .build();
        } catch (Exception e) {
            logger.info("error occured during accountDtoReader {}",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }


    }

    @Bean
    public ItemProcessor<AccountDto, Account> accountDtoItemProcessor(){
        try {
            logger.info("inside accountDtoItemProcessor");
            return accountMapper::convertAccountDtoToAccount;
        } catch (Exception e) {
            logger.error("error occured during accountDtoItemProcessor",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    //@Bean
    public ItemWriter<Account> accountItemWriter(){
        try {
            logger.info("inside accountItemWriter");
//            return chunk -> { logger.info("chunk toString in accountItemWriter{}",chunk.toString()); };
            return chunk -> { chunk.forEach(item -> logger.info("Account item {}",item.toString()));};
        } catch (Exception e) {
            logger.error("error occured during accountItemWriter",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    //@Bean
    public JpaItemWriter<Account> accountJpaItemWriter(EntityManagerFactory entityManagerFactory){
        try{
            logger.info("inside accountJpaItemWriter");
            return  new JpaItemWriter<>(entityManagerFactory);

        } catch (Exception e) {
            logger.error("error occured during accountJpaItemWriter",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    @Bean
    public JdbcBatchItemWriter<Account> accountJdbcBatchItemWriter(DataSource dataSource){
        try{
            logger.info("inside accountJdbcBatchItemWriter");
            JdbcBatchItemWriter<Account> accountJdbcBatchItemWriter = new JdbcBatchItemWriter<>();
            accountJdbcBatchItemWriter.setDataSource(dataSource);
            accountJdbcBatchItemWriter.setSql("""
                    INSERT INTO accounts(sf_id, name, description, is_deleted, last_sync_time)
                    VALUES(:sfId, :name, :description, :isDeleted, :lastSyncTime)
                    ON DUPLICATE KEY UPDATE sf_id = :sfId,
                    name = :name, description = :description,
                    is_deleted = :isDeleted, last_sync_time = :lastSyncTime
                    """);
            accountJdbcBatchItemWriter.setItemSqlParameterSourceProvider( new BeanPropertyItemSqlParameterSourceProvider<>());
            return accountJdbcBatchItemWriter;
        } catch (Exception e) {
            logger.error("error occured during accountJdbcBatchItemWriter",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    @Bean
    public Step accountBatchStepBuilder(JobRepository jobRepository,
                                        PlatformTransactionManager platformTransactionManager,
                                        ItemStreamReader<AccountDto> accountDtoItemReader,
                                        ItemProcessor<AccountDto, Account> accountDtoItemProcessor,
                                        ItemWriter<Account> accountJdbcBatchItemWriter){
        try{
            logger.info("inside accountBatchStepBuilder");
            return new StepBuilder(jobRepository)
                    .<AccountDto, Account>chunk(5)
                    .transactionManager(platformTransactionManager)
                    .reader(accountDtoItemReader)
                    .processor(accountDtoItemProcessor)
//                    .writer(accountItemWriter)
//                    .writer(accountJpaItemWriter)
                    .writer(accountJdbcBatchItemWriter)
                    .build();
        } catch (Exception e) {
            logger.error("error occured during stepBuilder",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    @Bean
    public Job accountsSyncBatchJob(JobRepository jobRepository, Step accountBatchStepBuilder, JobExecutionListener jobExecutionListener){
        try{
            logger.info("inside accountsSyncBatchJob");
            return new JobBuilder("AccountsSyncBatchJob",jobRepository)
                    .start(accountBatchStepBuilder)
                    .listener(jobExecutionListener)
                    .build();
        } catch (Exception e) {
            logger.error("error occured during accountsSyncBulkJob",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    @Bean
    public JobExecutionListener accountJobExecutionListener(){
        try{
            logger.info("inside jobExecutionListener");
            return new JobExecutionListener() {
                @Override
                public void afterJob(JobExecution jobExecution) {
                    JobExecutionListener.super.afterJob(jobExecution);
                    logger.info("after job completed");
                    jobExecution.getStepExecutions()
                            .forEach(stepExecution -> logger.info("stepExecution {}", stepExecution));
                }
            };
        } catch (Exception e) {
            logger.error("error occured during jobExecutionListener",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

}
