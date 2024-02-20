package com.nt.batch.service.impl;

import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.List;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Service;
import com.nt.batch.entity.UserEntity;
import com.nt.batch.service.ExcelService;

@Service
public class ExcelServiceIml implements ExcelService {

	private XSSFWorkbook misXSSFWorkBook;

	private XSSFSheet misXSSFSheet;

	private List<UserEntity> userEntites;

	@Override
	public void exportMisReportsToExcel(List<UserEntity> userEntites) {
		this.userEntites = userEntites;
		misXSSFWorkBook = new XSSFWorkbook();
		misXSSFSheet = misXSSFWorkBook.createSheet("misreports");
		writeHeaderRow();
		writeDataRows();
		generateMisReports();
	}

	private void writeHeaderRow() {
		Row row = misXSSFSheet.createRow(0);
		CellStyle style = misXSSFWorkBook.createCellStyle();
		XSSFFont font = misXSSFWorkBook.createFont();
		font.setBold(true);
		font.setFontHeight(14);
		style.setFillBackgroundColor(IndexedColors.GREEN.getIndex());
		style.setFont(font);
		createReportCell(row, style);

	}

	private void createReportCell(Row row, CellStyle style) {

		createCell(row, 0, "ID", style);
		createCell(row, 1, "FIRST NAME", style);
		createCell(row, 2, "LAST NAME", style);
		createCell(row, 3, "USER ID", style);
	}

	private void createCell(Row row, int columnCount, Object value, CellStyle style) {
		misXSSFSheet.autoSizeColumn(columnCount);
		Cell cell = row.createCell(columnCount);
		if (value instanceof Integer) {
			cell.setCellValue((Integer) value);
		} else if (value instanceof Boolean) {
			cell.setCellValue((Boolean) value);
		} else if (value instanceof Double) {
			cell.setCellValue((Double) value);
		} else if (value instanceof Long) {
			cell.setCellValue((Long) value);
		} else if (value instanceof BigInteger) {
			cell.setCellValue((String) value.toString());
		} else if (value instanceof Timestamp) {
			cell.setCellValue((String) value.toString());
		} else {
			cell.setCellValue((String) value);
		}
		cell.setCellStyle(style);
	}

	private void writeDataRows() {

		int rowCount = 1;
		CellStyle style = misXSSFWorkBook.createCellStyle();
		XSSFFont font = misXSSFWorkBook.createFont();
		font.setFontHeight(14);
		style.setFont(font);
		createRowDataForMisCell(rowCount, style);

	}

	private void createRowDataForMisCell(int rowCount, CellStyle style) {
		for (UserEntity userEntity : userEntites) {
			Row row = misXSSFSheet.createRow(rowCount++);
			int columnCount = 0;
			createCell(row, columnCount++, userEntity.getId(), style);
			createCell(row, columnCount++, userEntity.getFirstName(), style);
			createCell(row, columnCount++, userEntity.getLastName(), style);
			createCell(row, columnCount++, userEntity.getUserId(), style);
		}
	}

	private void generateMisReports() {

		String reportName = "D:\\MIS_REPORTS.xlsx";
		FileOutputStream fileOut = null;

		try {
			if (Files.notExists(Paths.get(reportName))) {
				System.out.println("Helllo");
				fileOut = new FileOutputStream(reportName);
				misXSSFWorkBook.write(fileOut);
			} else {
				System.out.println("KING");
				misXSSFWorkBook.write(fileOut);
			}
		} catch (IOException e) {

			System.out.println("Error while generating file excel");
			e.printStackTrace();
		}

	}

}
