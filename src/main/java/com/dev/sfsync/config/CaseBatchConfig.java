package com.dev.sfsync.config;

import com.dev.sfsync.dao.Case;
import com.dev.sfsync.dao.CaseMapper;
import com.dev.sfsync.dto.CaseDto;
import com.dev.sfsync.exception.SfSyncException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.ItemStreamReader;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.LineMapper;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.infrastructure.item.file.mapping.RecordFieldSetMapper;
import org.springframework.batch.infrastructure.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;


@Configuration
public class CaseBatchConfig {
    private static final Logger logger = LoggerFactory.getLogger(CaseBatchConfig.class);
    private final BatchConfig batchConfig;
    private final CaseMapper caseMapper;
    private final String[] caseDtoRecordClassFieldNames;

    public CaseBatchConfig(BatchConfig batchConfig, CaseMapper caseMapper) {
        this.batchConfig = batchConfig;
        this.caseMapper = caseMapper;
        this.caseDtoRecordClassFieldNames = batchConfig.getDtoRecordClassFieldNames(CaseDto.class);
    }


    @Bean
    public LineMapper<CaseDto> caseLineMapper(){
        try{
            DefaultLineMapper<CaseDto> lineMapper = new DefaultLineMapper<>();

            DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
            lineTokenizer.setNames(caseDtoRecordClassFieldNames);
            lineTokenizer.setQuoteCharacter('"');
            lineTokenizer.setStrict(false);

            RecordFieldSetMapper<CaseDto> fieldSetMapper = new RecordFieldSetMapper<>(CaseDto.class);

            lineMapper.setLineTokenizer(lineTokenizer);
            lineMapper.setFieldSetMapper(fieldSetMapper);
            return lineMapper;
        } catch (Exception e) {
            logger.error("error in caseLineMapper {}",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    @Bean
    @StepScope
    public FlatFileItemReader<CaseDto> caseDtoFlatFileItemReader(@Value("#{jobParamMapBean['Case']}") Resource caseCsvResource, LineMapper<CaseDto> caseDtoLineMapper){
        logger.info("inside caseDtoFlatFileItemReader Resource {}",caseCsvResource);
        try {
            logger.info("caseDtoFlatFileItemReader");
            return new FlatFileItemReaderBuilder<CaseDto>()
                    .lineMapper(caseDtoLineMapper)
                    .resource(caseCsvResource)
                    .saveState(false)
                    .linesToSkip(1)
                    .build();
        } catch (Exception e) {
            logger.error("error in caseDtoFlatFileItemReader {}",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    @Bean
    public ItemProcessor<CaseDto, Case> caseItemProcessor(){
        try{
            logger.info("inside caseItemProcessor");
//            return caseMapper::convertCaseDtoToCase;
            return a->{
                if(a.priority() =="Medium") {
                    return null;
                }else{
                    return caseMapper.convertCaseDtoToCase(a);
                }
            };
        } catch (Exception e) {
            logger.error("error in caseItemProcessor {}",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    @Bean
    public JdbcBatchItemWriter<Case> caseJdbcBatchItemWriter(DataSource dataSource){
        try{
            logger.info("inside JdbcBatchItemWriter");
            JdbcBatchItemWriter<Case> jdbcBatchItemWriter = new JdbcBatchItemWriter<>();
            jdbcBatchItemWriter.setDataSource(dataSource);
            String sql = ("""
                    INSERT INTO cases(sf_id,subject,priority,status,reason,closed_date,created_date,last_modified_date)
                     VALUES(:%s)
                    """).formatted(String.join(",:", caseDtoRecordClassFieldNames));
            logger.info("caseItemWriter {}",sql);
            jdbcBatchItemWriter.setSql(sql);
            jdbcBatchItemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
            return jdbcBatchItemWriter;
        } catch (Exception e) {
            logger.error("error in caseItemWriter {}",e.getMessage());
            throw new SfSyncException(e.getMessage());
        }
    }

    @Bean
    public Step caseBatchStep(JobRepository jobRepository,
                                     PlatformTransactionManager platformTransactionManager,
                                     ItemStreamReader<CaseDto> caseDtoFlatFileItemReader,
                                     ItemProcessor<CaseDto, Case> caseDtoCaseItemProcessor,
                                     ItemWriter<Case> caseJdbcBatchItemWriter){
        logger.info("inside caseBatchStepBuilder");
        return new StepBuilder(jobRepository)
                .<CaseDto, Case>chunk(5)
                .transactionManager(platformTransactionManager)
                .reader(caseDtoFlatFileItemReader)
                .processor(caseDtoCaseItemProcessor)
                .writer(caseJdbcBatchItemWriter)
                .build();
    }

    @Bean
    public Job casesSyncBatchJob(JobRepository jobRepository, Step caseBatchStep){
        logger.info("inside caseBatchJob");
        return new JobBuilder("CasesSyncBulkJob",jobRepository)
                .start(caseBatchStep)
                .build();
    }
}
