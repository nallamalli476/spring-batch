package com.nt.batch.reader;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.nt.batch.entity.UserEntity;

@Component
public class UserRowMapper implements RowMapper<UserEntity> {

	//List<UserEntity> list = new ArrayList<>();

	@Override
	public UserEntity mapRow(ResultSet rs, int rowNum) throws SQLException {

		UserEntity userEntity = new UserEntity();
		userEntity.setId(rs.getLong("id"));
		userEntity.setFirstName(rs.getString("first_name"));
		userEntity.setLastName(rs.getString("last_name"));
		userEntity.setUserId(rs.getString("user_id"));
		//list.add(userEntity);
		return userEntity;
	}

}
