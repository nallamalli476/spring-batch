package com.nt.batch.reader;

import java.util.List;

import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.nt.batch.entity.UserEntity;
import com.nt.batch.reposistory.UserReposistory;

@Component
public class UserReader implements ItemReader<List<UserEntity>>{
	
	@Autowired
	private UserReposistory UserReposistory;

	@Override
	public List<UserEntity> read()throws Exception {
		
		
		return UserReposistory.findByUserId("Ravi@1993");
	}

}
