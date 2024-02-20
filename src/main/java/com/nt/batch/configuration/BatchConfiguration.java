package com.nt.batch.configuration;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.nt.batch.entity.UserEntity;
import com.nt.batch.reader.UserReader;
import com.nt.batch.reader.UserRowMapper;
import com.nt.batch.writer.UserWriter;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	private DataSource dataSource;

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private UserWriter genericWriter;
	
	@Autowired
	private UserReader userReader;
	

	@Bean
	public Step step1() {
		return stepBuilderFactory.get("getUsers476").<UserEntity, UserEntity>chunk(1000)
				                 .reader(reader(null))
				                  .writer(writer())
				                 //.writer(genericWriter)
				                 .taskExecutor(taskExecutor())
				                 .build();
	}

	@Bean
	public Job runJob() {
		return jobBuilderFactory.get("process476")
				                .start(step1())
				                .build();
	}
	
/*	@Bean
	@StepScope
	public JdbcCursorItemReader<List<UserEntity>> reader(@Value("#{jobParameters[userId]}") String userId) {
		JdbcCursorItemReader<List<UserEntity>> reader = new JdbcCursorItemReader<>();
		
		    reader.setSql("select id, first_name, last_name, user_id from users");
	        //String sqlQuery = "select id, first_name, last_name, user_id from users where user_id='Ravi@1993'";
		
		    reader.setDataSource(dataSource);
		    //String sqlQuery = "select id, first_name, last_name, user_id from users where user_id="+"'"+userId+"'";
		    //reader.setSql(sqlQuery);
		    reader.setFetchSize(500);
	        reader.setRowMapper(new RowMapper<List<UserEntity>>() {
				List<UserEntity> list = new ArrayList<>();
				@Override
				public List<UserEntity> mapRow(ResultSet rs, int rowNum) throws SQLException {
					
					UserEntity userEntity = new UserEntity();
					userEntity.setId(rs.getLong("id"));
					userEntity.setFirstName(rs.getString("first_name"));
					userEntity.setLastName(rs.getString("last_name"));
					userEntity.setUserId(rs.getString("user_id"));
					list.add(userEntity);
					return list;
				}
			});

		return reader;
	} */
	
    @Bean
	@StepScope
	public JdbcPagingItemReader<UserEntity> reader(@Value("#{jobParameters[userId]}") String userId){
    	
		
		Map<String, Order> sortKeys = new HashMap<>();
        sortKeys.put("id", Order.ASCENDING);
		
        PostgresPagingQueryProvider queryProvider = new PostgresPagingQueryProvider();
        
        queryProvider.setSelectClause("select id, first_name, last_name, user_id");
        queryProvider.setFromClause("from users");
        queryProvider.setSortKeys(sortKeys);

        JdbcPagingItemReader<UserEntity> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(dataSource);
        reader.setPageSize(500);
        reader.setRowMapper(new UserRowMapper());
        reader.setQueryProvider(queryProvider);
        
        return reader;

	}
    
    @Bean
    @StepScope
    public FlatFileItemWriter<UserEntity> writer() {
        FlatFileItemWriter<UserEntity> writer = new FlatFileItemWriter<UserEntity>();
        writer.setResource(new FileSystemResource("D:\\misreports.csv"));
        writer.setHeaderCallback(new FlatFileHeaderCallback() {
            @Override
            public void writeHeader(Writer writer) throws IOException {
                writer.write("ID, FirstName, LastName, UserId");
            }
        });
        DelimitedLineAggregator<UserEntity> lineAggregator = new DelimitedLineAggregator<UserEntity>();
        lineAggregator.setDelimiter(",");
        BeanWrapperFieldExtractor<UserEntity> fieldExtractor = new BeanWrapperFieldExtractor<UserEntity>();
        fieldExtractor.setNames(new String[]{"id", "firstName", "lastName", "userId"});
        lineAggregator.setFieldExtractor(fieldExtractor);
        writer.setLineAggregator(lineAggregator);
        
        return writer;
    } 
    
    
    @Bean
	public TaskExecutor taskExecutor() {
    	ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(10);
        taskExecutor.setMaxPoolSize(20);
        taskExecutor. setQueueCapacity(100);
        taskExecutor.setThreadNamePrefix("user-batch-exec-");
        return taskExecutor;
	} 
}
