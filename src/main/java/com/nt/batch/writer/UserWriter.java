package com.nt.batch.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.nt.batch.entity.UserEntity;
import com.nt.batch.service.ExcelService;

@Component
public class UserWriter implements ItemWriter<UserEntity>{

	 @Autowired
	 private ExcelService excelService;

	@Override
	public void write(List<? extends UserEntity> items) throws Exception {

        List<UserEntity> userEntities = new ArrayList<>();
		
		System.out.println("MISREPORTS :"+ items.get(0));
		
		UserEntity userEntites =  items.get(0);
		
		System.out.println(userEntites);
		
		//userEntities.add(userEntites);
		
	  //excelService.exportMisReportsToExcel(userEntities);
				
	}
	
}
