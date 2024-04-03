package admin.service;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import java.io.File;
import java.io.FileInputStream;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ExcelDataUploader {

	private static Timestamp parseExcelDate(String excelDateStr) throws Exception {
		String sanitizedDateStr = excelDateStr.replace('\n', ' ').replaceAll(" +", " ").trim();
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
		return new Timestamp(dateFormat.parse(sanitizedDateStr).getTime());
	}

	private static final String URL = "jdbc:postgresql://yuvi.cuytbdxg065v.ap-south-1.rds.amazonaws.com/stride_bms";
	private static final String USER = "postgres";
	private static final String PASSWORD = "0ZTQYTlmFIBH9BKr7bKC";

	public static Connection getConnection() throws SQLException {
		return DriverManager.getConnection(URL, USER, PASSWORD);
	}

	public void uploadData(String excelFilePath) throws SQLException {
		Connection connection = getConnection();
		try (FileInputStream excelFile = new FileInputStream(new File(excelFilePath));
				Workbook workbook = new XSSFWorkbook(excelFile)) {

			Sheet sheet = workbook.getSheetAt(0);
			Map<String, Integer> transmitterIds = new HashMap<>();
			Map<String, Integer> locationIds = new HashMap<>();
			Map<String, Integer> parameterIds = new HashMap<>();
			Map<String, Integer> alarmTypeIds = new HashMap<>();

			connection.setAutoCommit(false);

			for (Row row : sheet) {
				if (row.getRowNum() == 0)
					continue; // Skip header row

				String transmitterName = row.getCell(1).getStringCellValue();
				String areaName = row.getCell(2).getStringCellValue();
				String blockName = row.getCell(3).getStringCellValue();
				String parameterName = row.getCell(4).getStringCellValue();
//				String alarmName = row.getCell(5).getStringCellValue();
				String alarmTypeName = row.getCell(6).getStringCellValue();
				Date alarmStart = parseExcelDate(row.getCell(7).getStringCellValue());
				Date alarmEnd = parseExcelDate(row.getCell(8).getStringCellValue());
				String alarmDuration = row.getCell(9).getCellType() == CellType.NUMERIC
						? String.format("%d:%02d:%02d", (int) (row.getCell(9).getNumericCellValue() * 24),
								(int) ((row.getCell(9).getNumericCellValue() * 24 * 60) % 60),
								(int) ((row.getCell(9).getNumericCellValue() * 24 * 60 * 60) % 60))
						: row.getCell(9).getStringCellValue();
				String alarmStatus = row.getCell(10).getStringCellValue();
				boolean ackStatus = "Acknowledged".equalsIgnoreCase(row.getCell(11).getStringCellValue());
				String acknowledgedBy = row.getCell(12).getStringCellValue();

				Timestamp alarmStartTimestamp = new Timestamp(alarmStart.getTime());
				Timestamp alarmEndTimestamp = new Timestamp(alarmEnd.getTime());

				int locationId = insertLocation(areaName, blockName, connection);
				int transmitterId = insertAndGetId(transmitterName, "transmitters", "transmitter_name",
						"transmitter_id", transmitterIds, connection);
				int parameterId = insertAndGetId(parameterName, "parameters", "name", "parameter_id", parameterIds,
						connection);
				int alarmTypeId = insertAndGetId(alarmTypeName, "alarm_types", "alarm_category", "alarm_type_id",
						alarmTypeIds, connection);

				String alarmSql = "INSERT INTO alarms (transmitter_id, location_id, parameter_id, alarm_type_id, alarm_start_date_and_time, alarm_end_date_and_time, alarm_duration, alarm_status, ack_status) VALUES (?, ?, ?, ?, ?, ?, ?::interval, ?, ?)";
				PreparedStatement alarmStmt = connection.prepareStatement(alarmSql, Statement.RETURN_GENERATED_KEYS);
				alarmStmt.setInt(1, transmitterId);
				alarmStmt.setInt(2, locationId);
				alarmStmt.setInt(3, parameterId);
				alarmStmt.setInt(4, alarmTypeId);
//				alarmStmt.setString(5, alarmName);
				alarmStmt.setTimestamp(5, alarmStartTimestamp);
				alarmStmt.setTimestamp(6, alarmEndTimestamp);
				alarmStmt.setString(7, alarmDuration);
				alarmStmt.setString(8, alarmStatus);
				alarmStmt.setBoolean(9, ackStatus);
				alarmStmt.executeUpdate();

				ResultSet alarmRs = alarmStmt.getGeneratedKeys();
				int alarmId = 0;
				if (alarmRs.next()) {
					alarmId = alarmRs.getInt(1);
				}

				if (alarmId > 0 && !acknowledgedBy.isEmpty()) {
					String acknowledgementSql = "INSERT INTO acknowledgements (alarm_id, acknowledged_by, acknowledged_date_time) VALUES (?, ?, ?)";
					PreparedStatement acknowledgementStmt = connection.prepareStatement(acknowledgementSql);
					acknowledgementStmt.setInt(1, alarmId);
					acknowledgementStmt.setString(2, acknowledgedBy);
					acknowledgementStmt.setTimestamp(3, new Timestamp(new Date().getTime())); // Use actual
																								// acknowledgment time
																								// if available
					acknowledgementStmt.executeUpdate();
				}
			}
			connection.commit();
		} catch (Exception e) {
			e.printStackTrace();
			try {
				if (connection != null) {
					connection.rollback();
				}
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
	}

	private int insertAndGetId(String name, String tableName, String columnName, String idColumnName,
			Map<String, Integer> cache, Connection connection) throws SQLException {
		if (cache.containsKey(name)) {
			return cache.get(name);
		}

		// Attempt to find an existing record first
		String selectSql = String.format("SELECT %s FROM %s WHERE %s = ?", idColumnName, tableName, columnName);
		try (PreparedStatement selectStmt = connection.prepareStatement(selectSql)) {
			selectStmt.setString(1, name);
			ResultSet rs = selectStmt.executeQuery();
			if (rs.next()) {
				int id = rs.getInt(1);
				cache.put(name, id);
				return id;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}

		// If no existing record, insert a new one and get the ID
		String insertSql = String.format("INSERT INTO %s (%s) VALUES (?) RETURNING %s", tableName, columnName,
				idColumnName);
		try (PreparedStatement insertStmt = connection.prepareStatement(insertSql)) {
			insertStmt.setString(1, name);
			ResultSet rs = insertStmt.executeQuery(); // Changed to executeQuery
			if (rs.next()) {
				int id = rs.getInt(1);
				cache.put(name, id);
				return id;
			} else {
				throw new SQLException("Failed to insert new record and retrieve ID.");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}
	}

	private int insertLocation(String areaName, String blockName, Connection connection) throws SQLException {
		// First, attempt to find an existing location with the given area_name and
		// block_name
		String findSql = "SELECT location_id FROM locations WHERE area_name = ? AND block_name = ?";
		try (PreparedStatement findStmt = connection.prepareStatement(findSql)) {
			findStmt.setString(1, areaName);
			findStmt.setString(2, blockName);
			ResultSet rs = findStmt.executeQuery();
			if (rs.next()) {
				return rs.getInt("location_id");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}

		// If not found, proceed with the insert
		String insertSql = "INSERT INTO locations (area_name, block_name) VALUES (?, ?) RETURNING location_id";
		try (PreparedStatement insertStmt = connection.prepareStatement(insertSql)) {
			insertStmt.setString(1, areaName);
			insertStmt.setString(2, blockName);
			ResultSet rs = insertStmt.executeQuery();
			if (rs.next()) {
				return rs.getInt(1);
			} else {
				throw new SQLException("Failed to insert new location and retrieve ID.");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}
	}

	public static void main(String[] args) {
		try {
			ExcelDataUploader uploader = new ExcelDataUploader();
			uploader.uploadData("C:\\Users\\bagal\\Downloads\\testBMS.xlsx");
			
			
			System.out.println("Data Upload Done");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
