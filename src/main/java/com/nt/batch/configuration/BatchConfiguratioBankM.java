package com.nt.batch.configuration;

import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;
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
import org.springframework.batch.item.database.support.OraclePagingQueryProvider;
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
import com.mahabank.reader.GenericRowMapper;


@Configuration
@EnableBatchProcessing
public class BatchConfiguratioBankM{

	@Autowired
	private DataSource dataSource;

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	private GenericWriter genericWriter;

	
	@Bean
	public Step step1(JdbcPagingItemReader<MIS003AggVo> jdbcPagingItemReader) {
		return stepBuilderFactory.get("getUsers476")
				                 .<MIS003AggVo, MIS003AggVo>chunk(50000)
				                 .reader(jdbcPagingItemReader)
				                 .writer(writer())
				                 .taskExecutor(taskExecutor())
				                 .build();
	}

	
	@Bean
	public Job runJob(JdbcPagingItemReader<MIS003AggVo> process) {
		return jobBuilderFactory.get("process476")
				                .start(step1(process))
				                .build();
	}
	
	@Bean
	@StepScope
	public JdbcPagingItemReader<MIS003AggVo> reader(@Value("#{jobParameters[twoFlg]}") String twoFlg,
			@Value("#{jobParameters[byDate]}") Date byDate, @Value("#{jobParameters[addDays]}") Date addDays) {
	
		JdbcPagingItemReader<MIS003AggVo> reader = new JdbcPagingItemReader<MIS003AggVo>();
				
		String byDateMis003 = new SimpleDateFormat("dd-MM-YYY").format(byDate);
		String addDaysMis003 = new SimpleDateFormat("dd-MM-YYY").format(addDays);
		
		/*String nativQuery = "SELECT * FROM MIS003_AGG WHERE REPORT_DATE >='30-07-2023'"
				                + "AND  REPORT_DATE <'31-07-2023'"
				                + "AND two_flg ='WITH TWO'";*/
		
		//String nativQuery = "SELECT * FROM MIS003_AGG WHERE REPORT_DATE >="+byDateMis003+" AND  REPORT_DATE <"+addDaysMis003+" AND two_flg ="+twoFlg;
		
		//String nativQuery = "SELECT * FROM MIS003_AGG WHERE REPORT_DATE >=" + "'" + byDateMis003 + "'"+ " AND  REPORT_DATE <" + "'" + addDaysMis003 + "'" + " AND two_flg =" + "'" + twoFlg + "'";
		
		String whereCaluse = "WHERE REPORT_DATE >=" + "'" + byDateMis003 + "'"+ " AND  REPORT_DATE <" + "'" + addDaysMis003 + "'" + " AND two_flg =" + "'" + twoFlg + "'";
		
		Map<String, Order> sortKeys = new HashMap<>();
        sortKeys.put("id", Order.ASCENDING);
        
        OraclePagingQueryProvider queryProvider = new OraclePagingQueryProvider();
        queryProvider.setSelectClause("SELECT *");
        queryProvider.setFromClause("FROM MIS003_AGG");
        queryProvider.setWhereClause(whereCaluse);
        queryProvider.setSortKeys(sortKeys);
        
		reader.setDataSource(dataSource);
		reader.setPageSize(20000); 
		reader.setRowMapper(new GenericRowMapper());
		reader.setQueryProvider(queryProvider);

		return reader;
	}
	
	@Bean
	@StepScope
	public FlatFileItemWriter<MIS003AggVo> writer() {
		FlatFileItemWriter<MIS003AggVo> writer = new FlatFileItemWriter<MIS003AggVo>();
		writer.setResource(new FileSystemResource("D:\\misreports.csv"));
		writer.setHeaderCallback(new FlatFileHeaderCallback() {
			@Override
			public void writeHeader(Writer writer) throws IOException {
				writer.write("ID, REGION_CODE, REGION_NAME, BRANCH_CODE ,BRANCH_NAME, REPORT_DATE, ACCTS_HOUSING, "
						+ "AMT_HOUSING, ACCTS_HOUSING_NPA, AMT_HOUSING_NPA, NPA_PERCENT_HOUSING, ACCTS_LAP, AMT_LAP, ACCTS_LAP_NPA, "
						+ "AMT_LAP_NPA,NPA_PERCENT_LAP, ACCTS_EDUCATION, AMT_EDUCATION, ACCTS_EDUCATION_NPA, AMT_EDUCATION_NPA, NPA_PERCENT_EDUCATION, "
						+ "ACCTS_OTHER_PERSONAL_LOANS, AMT_OTHER_PERSONAL_LOANS, ACCTS_OTHER_PERSONAL_LOANS_NPA, AMT_OTHER_PERSONAL_LOANS_NPA, "
						+ "NPA_PERCENT_OTHER_PERSONAL_LOANS, ACCTS_2WHEELER_VEHICLE, AMT_2WHEELER_VEHICLE, ACCTS_2WHEELER_VEHICLE_NPA, AMT_2WHEELER_VEHICLE_NPA, "
						+ "NPA_PERCENT_2WHEELER_VEHICLE, ACCTS_4WHEELER_VEHICLE, AMT_4WHEELER_VEHICLE, ACCTS_4WHEELER_VEHICLE_NPA, AMT_4WHEELER_VEHICLE_NPA, "
						+ "NPA_PERCENT_4WHEELER_VEHICLE, ACCTS_ADHAR, AMT_ADHAR, ACCTS_ADHAR_NPA, AMT_ADHAR_NPA, NPA_PERCENT_ADHAR, ACCTS_TOPUP, AMT_TOPUP, "
						+ "ACCTS_TOPUP_NPA, AMT_TOPUP_NPA, NPA_PERCENT_TOPUP, ACCTS_HOUSING_TOPUP, AMT_HOUSING_TOPUP, ACCTS_HOUSING_TOPUP_NPA, "
						+ "AMT_HOUSING_TOPUP_NPA, NPA_PERCENT_HOUSING_TOPUP, ACCTS_AGRI_GOLD, AMT_AGRI_GOLD, ACCTS_AGRI_GOLD_NPA, AMT_AGRI_GOLD_NPA, "
						+ "NPA_PERCENT_AGRI_GOLD, ACCTS_NON_AGRI_GOLD, AMT_NON_AGRI_GOLD,ACCTS_NON_AGRI_GOLD_NPA, AMT_NON_AGRI_GOLD_NPA,NPA_PERCENT_NON_AGRI_GOLD, "
						+ "ACCTS_LOAN_AGNST_PAPER, AMT_LOAN_AGNST_PAPER, ACCTS_LOAN_AGNST_PAPER_NPA, AMT_LOAN_AGNST_PAPER_NPA, NPA_PERCENT_LOAN_AGNST_PAPER, "
						+ "ACCTS_LAD, AMT_LAD, ACCTS_LAD_NPA, AMT_LAD_NPA, NPA_PERCENT_EXCLUDE_LAD, ACCTS_DEBIT_BAL_IN_CC, AMT_DEBIT_BAL_IN_CC, ACCTS_DEBIT_BAL_IN_CC_NPA, "
						+ "AMT_DEBIT_BAL_IN_CC_NPA, NPA_PERCENT_DEBIT_BAL_IN_CC, ACCTS_STAFF, AMT_STAFF, ACCTS_STAFF_NPA, AMT_STAFF_NPA, NPA_PERCENT_STAFF, "
						+ "ACCTS_PERSONAL, AMT_PERSONAL, ACCTS_PERSONAL_NPA , AMT_PERSONAL_NPA , NPA_PERCENT_PERSONAL ,TWO_FLG");
			}
		});
		DelimitedLineAggregator<MIS003AggVo> lineAggregator = new DelimitedLineAggregator<MIS003AggVo>();
		lineAggregator.setDelimiter(",");
		BeanWrapperFieldExtractor<MIS003AggVo> fieldExtractor = new BeanWrapperFieldExtractor<MIS003AggVo>();
		fieldExtractor.setNames(new String[] { "id", "regionCode", "regionName", "branchCode", "branchName","reportDate", 
				"accts_housing","amt_housing","accts_housing_npa","amt_housing_npa", "npa_percent_housing", 
				"accts_lap","amt_lap","accts_lap_npa","amt_lap_npa","npa_percent_lap", "accts_education", "amt_education", "accts_education_npa", 
				"amt_education_npa", "npa_percent_education", "accts_other_personal_loans", "amt_other_personal_loans", "accts_other_personal_loans_npa", 
				"amt_other_personal_loans_npa","npa_percent_other_personal_loans","accts_2wheeler_vehicle","amt_2wheeler_vehicle", "accts_2wheeler_vehicle_npa",
				"amt_2wheeler_vehicle_npa", "npa_percent_2wheeler_vehicle", "accts_4wheeler_vehicle","amt_4wheeler_vehicle", "accts_4wheeler_vehicle_npa",
				"amt_4wheeler_vehicle_npa","npa_percent_4wheeler_vehicle", "accts_adhar","amt_adhar", "accts_adhar_npa","amt_adhar_npa","npa_percent_adhar", 
				"accts_topup","amt_topup", "accts_topup_npa","amt_topup_npa","npa_percent_topup", "accts_housing_topup", "amt_housing_topup", "accts_housing_topup_npa",
				"amt_housing_topup_npa","npa_percent_housing_topup", "accts_agri_gold","amt_agri_gold","accts_agri_gold_npa","amt_agri_gold_npa",
				"npa_percent_agri_gold","accts_non_agri_gold","amt_non_agri_gold","accts_non_agri_gold_npa","amt_non_agri_gold_npa", "npa_percent_non_agri_gold",
				"accts_loan_agnst_paper","amt_loan_agnst_paper","accts_loan_agnst_paper_npa","amt_loan_agnst_paper_npa","npa_percent_loan_agnst_paper", "accts_lad",
				"amt_lad","accts_lad_npa","amt_lad_npa","npa_percent_exclude_lad","accts_debit_bal_in_cc","amt_debit_bal_in_cc","accts_debit_bal_in_cc_npa",
				"amt_debit_bal_in_cc_npa","npa_percent_debit_bal_in_cc","accts_staff","amt_staff","accts_staff_npa","amt_staff_npa","npa_percent_staff",
				"accts_personal","amt_personal","accts_personal_npa","amt_personal_npa","npa_percent_personal","twoFlg"});
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
        taskExecutor.setThreadNamePrefix("generic-batch-exec-");
        return taskExecutor;
	} 
}
