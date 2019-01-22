package com.rueggerllc.tests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class JDBCTests {

	private static Logger logger = Logger.getLogger(JDBCTests.class);
	
	@BeforeClass
	public static void setupClass() throws Exception {
	}
	
	@AfterClass
	public static void tearDownClass() throws Exception {
	}

	@Before
	public void setupTest() throws Exception {
	}

	@After
	public void tearDownTest() throws Exception {
	}
	
	@Test
	// @Ignore
	public void testHive() {
		
		String queryString =
			"SELECT description, debit " +
			"FROM capone_txn";
		
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			Properties properties = new Properties();
			properties.setProperty("user", "chris");
			properties.setProperty("password", "");
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			connection = DriverManager.getConnection("jdbc:hive2://captain:10000/db1", properties);
			statement = connection.createStatement();
			resultSet = statement.executeQuery(queryString);
			while (resultSet.next()) {
				String description = resultSet.getString("description");
				Double debit = resultSet.getDouble("debit");
				System.out.println("NEXT ROW= " + description + " " + debit);
			}
			
		} catch (Exception e) {
			logger.error("ERROR", e);
		} finally {
			close(resultSet);
			close(statement);
			close(connection);
		}

	}
	
	
	@Test
	@Ignore
	public void testPosgresDHT22() {
		
		String queryString =
			"SELECT * from dht22_readings " +
			"WHERE sensor_id = 'sensor3' " + 
			"AND reading_time > '2019-01-21 07:00:00' " +
			"LIMIT 20";
		
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			Properties properties = new Properties();
			properties.setProperty("user", "chris");
			properties.setProperty("password", "dakota");
			Class.forName("org.postgresql.Driver");
			connection = DriverManager.getConnection("jdbc:postgresql://captain:5432/rueggerllc", properties);
			statement = connection.createStatement();
			resultSet = statement.executeQuery(queryString);
			while (resultSet.next()) {
				Double temperature = resultSet.getDouble("temperature");
				Double humidity = resultSet.getDouble("humidity");
				// Date time = resultSet.getDate("reading_time");
				Timestamp time = resultSet.getTimestamp("reading_time");
				// String line = String.format(", arg1)
				System.out.println("date=" + time + " temperature=" + temperature + " humidity=" + humidity);
			}
			
		} catch (Exception e) {
			logger.error("ERROR", e);
		} finally {
			close(resultSet);
			close(statement);
			close(connection);
		}

	}
	
	private static java.sql.Date getNow() {
		long now = Calendar.getInstance().getTime().getTime();
		// long uDate = new java.util.Date().getTime();
		java.sql.Date date = new java.sql.Date(now);
		return date;
	}
	
	private void close(Connection connection) {
		try {
			if (connection != null) {
				connection.close();
			}
		} catch (Exception e) {
		}
	}
	private void close(Statement statement) {
		try {
			if (statement != null) {
				statement.close();
			}
		} catch (Exception e) {
		}
	}
	private void close(ResultSet resultSet) {
		try {
			if (resultSet != null) {
				resultSet.close();
			}
		} catch (Exception e) {
		}
	}
	

	
	
}
