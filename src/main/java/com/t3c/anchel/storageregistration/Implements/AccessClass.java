package com.t3c.anchel.storageregistration.Implements;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.t3c.anchel.common.DbConfiguration;

public class AccessClass {
	
	private static final Logger logger = LoggerFactory.getLogger(AccessClass.class);
	
	Properties properties = null;
	String url = null;
	String user = null;
	String password = null;

	public AccessClass() {
		properties = new DbConfiguration().getDbProperties();
		url = properties.getProperty("com.sgs.waarpdb.server");
		user = properties.getProperty("com.sgs.waarpdb.user");
		password = properties.getProperty("com.sgs.waarpdb.pass");

	}

	public void insert(String uuid, long specialKey, String s3url) throws SQLException {

		logger.debug("Updating file details with uuid {}, specialkey {} and s3url {} " ,uuid,specialKey,s3url);
		try {
			Connection conn = (Connection) DriverManager.getConnection(url, user, password);
			String query = " INSERT into S3BUCKETMAPPING (uuid, specialKey, s3fileurl, processedOn, deleted)"
					+ " values (?, ?, ?, ?, ?)";

			PreparedStatement preparedStmt = conn.prepareStatement(query);

			preparedStmt.setString(1, uuid);
			preparedStmt.setLong(2, specialKey);
			preparedStmt.setString(3, s3url);
			preparedStmt.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
			preparedStmt.setBoolean(5, false);
			
			preparedStmt.execute();

			conn.close();
		}

		catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public String TakeSpecialId(String uuid) {
		String splId = null;
		try {
			Connection conn = (Connection) DriverManager.getConnection(url, user, password);
			String query = " SELECT specialKey FROM S3BUCKETMAPPING WHERE uuid =?";

			PreparedStatement preparedStmt = conn.prepareStatement(query);

			preparedStmt.setNString(1, uuid);

			ResultSet rs = preparedStmt.executeQuery();
			rs.next();
			splId = rs.getString(1);

			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return splId;
	}

}
