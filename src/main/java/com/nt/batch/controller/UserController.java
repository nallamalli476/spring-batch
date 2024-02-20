package com.nt.batch.controller;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.nt.batch.reposistory.UserReposistory;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@RequiredArgsConstructor
@RequestMapping(value = "/v1/load", consumes = { MediaType.APPLICATION_JSON_VALUE }, produces = {
		MediaType.APPLICATION_JSON_VALUE })
public class UserController {

	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	private Job job;
	
	@Autowired
	private UserReposistory userReposistory;

	@GetMapping("/{id}")
	public void getUserDetail(@PathVariable final String id) throws JobExecutionAlreadyRunningException,
			JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException {

		//List<UserEntity> userEntity= userService.getUserByUserId(userId);
		//Map<String, Object> response = new HashMap<>();
		//response.put("userEntity", userEntity);

	   //JobParameters jobParameters = new JobParametersBuilder().toJobParameters();
		
	 JobParameters params = new JobParametersBuilder()
				.addString("JobID", String.valueOf(System.currentTimeMillis()))
				.addString("userId", id)
				.toJobParameters();
		
		JobExecution execution = jobLauncher.run(job, params); 
		
         System.out.println("Execution :" + execution.getStatus());		
		// return ResponseEntity.ok(response);
  
	}
	
	
	
	
}
