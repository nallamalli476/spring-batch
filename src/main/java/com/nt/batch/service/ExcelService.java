package com.nt.batch.service;

import java.util.List;

import com.nt.batch.entity.UserEntity;

public interface ExcelService {

	void exportMisReportsToExcel(List<UserEntity> userEntites);

}
