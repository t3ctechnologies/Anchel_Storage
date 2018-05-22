package com.t3c.anchel.storageregistration.Implements;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
			String query = "UPDATE S3BUCKETMAPPING SET specialKey=?, s3fileurl=?" + " WHERE uuid=?";

			PreparedStatement preparedStmt = conn.prepareStatement(query);

			preparedStmt.setLong(1, specialKey);
			preparedStmt.setString(2, s3url);
			preparedStmt.setString(3, uuid);

			preparedStmt.execute();
			conn.close();
			logger.debug("Updated file details with uuid {}, specialkey {} and s3url {} successfully. " ,uuid,specialKey,s3url);
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
