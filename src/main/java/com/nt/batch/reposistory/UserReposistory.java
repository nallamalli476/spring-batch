package com.nt.batch.reposistory;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.nt.batch.entity.UserEntity;

public interface UserReposistory extends JpaRepository<UserEntity, Long> {

	List<UserEntity> findByUserId(String userId);

}
