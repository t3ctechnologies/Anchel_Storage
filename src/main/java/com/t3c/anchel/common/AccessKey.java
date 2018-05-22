package com.t3c.anchel.common;

public class AccessKey {
	private static final String ACCESS_KEY = "";/*put your access key*/
	private static final String SECRET_KEY = "";/*put your secret key*/
	private static final String BUCKET_NAME = "";/*put your bucket name*/

	public static String getSecretKey() {
		return SECRET_KEY;
	}

	public static String getBucketName() {
		return BUCKET_NAME;
	}

	public static String getAccessKey() {
		return ACCESS_KEY;
	}

}
