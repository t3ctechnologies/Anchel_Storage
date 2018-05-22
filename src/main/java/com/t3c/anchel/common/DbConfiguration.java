package com.t3c.anchel.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DbConfiguration {
	public Properties getDbProperties() {
		Properties properties = new Properties();
		try {
			InputStream inputStream = DbConfiguration.class.getClassLoader().getResourceAsStream("./waarpdb.properties");
			properties.load(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return properties;
	}
}
