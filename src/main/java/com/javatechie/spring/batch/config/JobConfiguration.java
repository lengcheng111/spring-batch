package com.javatechie.spring.batch.config;

import com.javatechie.spring.batch.entity.Customer;
import com.javatechie.spring.batch.mapper.CustomerRowMapper;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;


@Configuration
@EnableBatchProcessing
//@Qualifier("jobConfiguration")
public class JobConfiguration {
    private static final String CUSTOMERS_INFO = "customers_info";
    private static final String TARGET_CUSTOMERS_INFO = "target_" + CUSTOMERS_INFO;
    private static final String CUSTOMER_ID = "customer_id";
    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final DataSource dataSource;

    @Value("${thread.maxPool.size}")
    private Integer threadPoolSize;
    @Value("${thread.corePool.size}")
    private Integer threadCorePoolSize;

    @Value("${thread.corePool.size}")
    private Integer threadQueuePoolSize;

    @Value("${spring.batch.commit.chunk}")
    private Integer chunkSize;

    public JobConfiguration(final JobBuilderFactory jobBuilderFactory, final StepBuilderFactory stepBuilderFactory, final DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.dataSource = dataSource;
    }

    @Bean
    public ColumnRangePartitioner partitioner() {
        ColumnRangePartitioner columnRangePartitioner = new ColumnRangePartitioner();
        columnRangePartitioner.setColumn(CUSTOMER_ID);
        columnRangePartitioner.setDataSource(dataSource);
        columnRangePartitioner.setTable(CUSTOMERS_INFO);
        return columnRangePartitioner;
    }

    @Bean
    @StepScope
    public JdbcPagingItemReader<Customer> pagingItemReader(
            @Value("#{stepExecutionContext['minValue']}") Long minValue,
            @Value("#{stepExecutionContext['maxValue']}") Long maxValue) {
        System.out.println("reading " + minValue + " to " + maxValue);

        Map<String, Order> sortKeys = new HashMap<>();
        sortKeys.put(CUSTOMER_ID, Order.ASCENDING);

        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause(" * ");
        queryProvider.setFromClause("from " + CUSTOMERS_INFO);
        queryProvider.setWhereClause("where " + CUSTOMER_ID + " >= " + minValue + " and " + CUSTOMER_ID + " <= " + maxValue);
        queryProvider.setSortKeys(sortKeys);

        JdbcPagingItemReader<Customer> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(this.dataSource);
        reader.setFetchSize(1000);
        reader.setRowMapper(new CustomerRowMapper());
        reader.setQueryProvider(queryProvider);

        return reader;
    }


    @Bean
    @StepScope
    public JdbcBatchItemWriter<Customer> customerItemWriter() {
        JdbcBatchItemWriter<Customer> itemWriter = new JdbcBatchItemWriter<>();
        itemWriter.setDataSource(dataSource);
        itemWriter.setSql("INSERT INTO  " + TARGET_CUSTOMERS_INFO +
                " VALUES(:id,:contactNo,:country,:dob,:email,:firstName,:gender,:lastName);");

        itemWriter.setItemSqlParameterSourceProvider
                (new BeanPropertyItemSqlParameterSourceProvider<>());
        itemWriter.afterPropertiesSet();

        return itemWriter;
    }

    // Master
    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .partitioner(slaveStep().getName(), partitioner())
                .step(slaveStep())
                .gridSize(threadCorePoolSize)
                .taskExecutor(taskExecutor())
                .build();
    }

    // slave step
    @Bean
    public Step slaveStep() {
        return stepBuilderFactory.get("slaveStep")
                .<Customer, Customer>chunk(chunkSize)
                .reader(pagingItemReader(null, null))
                .writer(customerItemWriter())
                .build();
    }

    @Bean
    public Job jobOfPaginator() {
        return jobBuilderFactory.get("jobOfPaginator")
                .start(step1())
                .build();
    }

    @Bean
    public ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(threadPoolSize);
        taskExecutor.setCorePoolSize(threadCorePoolSize);
        taskExecutor.setQueueCapacity(threadQueuePoolSize);
        taskExecutor.setWaitForTasksToCompleteOnShutdown(true);
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
    }

}