package com.t3c.anchel.storageregistration.Implements;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.t3c.anchel.common.DbConfiguration;
import com.t3c.anchel.common.SMConstants;

public class StorageAwsImpl extends StorageAdapterClass {

	String accesskey = null;
	String secretkey = null;
	Properties properties = null;
	String region = null;
	String bucketName = null;

	public StorageAwsImpl() {
		properties = new DbConfiguration().getDbProperties();
		this.accesskey = properties.getProperty(SMConstants.ACCESS_KEY);
		this.secretkey = properties.getProperty(SMConstants.SECRET_KEY);
		this.region = properties.getProperty(SMConstants.CLIENT_REGION);
		this.bucketName = properties.getProperty(SMConstants.BUCKET_NAME);
	}

	private static final Logger logger = LoggerFactory.getLogger(StorageAwsImpl.class);

	public void sendFile(String filePath) {
		logger.debug("file : {} is uploading into amazone s3 Bucket.", filePath);

		String createpath1 = filePath.substring(filePath.indexOf('_') + 1);
		String createpath2 = createpath1.substring(createpath1.indexOf('_') + 1);
		File createFile = new File(filePath);
		String createpath3 = createFile.getParent() + File.separator.concat(createpath2);
		try {
			new StorageAwsImpl().add(filePath, createpath2);
			createSuccessFile(createpath3);
		} catch (AmazonServiceException e1) {
			e1.printStackTrace();
			createErrorFile(createpath3);
		} catch (AmazonClientException e) {
			e.printStackTrace();
			createErrorFile(createpath3);
		} catch (InterruptedException e) {
			e.printStackTrace();
			createErrorFile(createpath3);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void createSuccessFile(String filePath) {
		File tFile = new File(filePath.concat("_successFile"));
		String data = "File uploaded successfuly.";
		OutputStream os = null;
		try {
			os = new FileOutputStream(tFile);
			os.write(data.getBytes(), 0, data.length());
			logger.error("Success File is created");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void DeleteFile(String bucketName, String folderName) {

	}

	@Override
	public void GetAll(String bucketName, String folderName) {

	}

	public void GetById(String targetFile) {
		logger.debug("getting object details for bucketname :{}, file :{}", this.bucketName, targetFile);
		credentials = new BasicAWSCredentials(this.accesskey, this.secretkey);
		s3client = AmazonS3ClientBuilder.standard().withCredentials(new AWSCredentialsProvider() {
			public void refresh() {
			}

			public AWSCredentials getCredentials() {
				return credentials;
			}
		}).withRegion(this.region).build();

		try {
			String fileID = new File(targetFile).getName();
			S3Object s3object = s3client.getObject(new GetObjectRequest(this.bucketName, fileID));
			if (s3object != null) {
				InputStream reader = new BufferedInputStream(s3object.getObjectContent());
				File tFile = new File(targetFile);
				OutputStream writer = null;
				try {
					writer = new BufferedOutputStream(new FileOutputStream(tFile));

					int read = -1;
					while ((read = reader.read()) != -1) {
						writer.write(read);
					}
					logger.debug("File writing is processed for fileID :" + fileID);
					writer.flush();
					writer.close();
					reader.close();
				} catch (IOException e) {
					logger.error("Exception while processing: {}", e);
					e.printStackTrace();
					createErrorFile(targetFile);
				}
				logger.debug("file info :{}", s3object.getObjectMetadata().getContentType());
			} else {
				throw new AmazonClientException(targetFile);
			}
		} catch (AmazonClientException exception) {
			logger.error("Specified key doesn't exist");
			exception.printStackTrace();
			createErrorFile(targetFile);
		} catch (IllegalArgumentException e) {
			logger.error("Key can't be null");
			e.printStackTrace();
			createErrorFile(targetFile);
		}

	}

	@SuppressWarnings("resource")
	private void createErrorFile(String targetFile) {
		File tFile = new File(targetFile.concat("_errorFile"));
		String data = "Somthing went wrong, file operation is not completed.";
		OutputStream os = null;
		try {
			os = new FileOutputStream(tFile);
			os.write(data.getBytes(), 0, data.length());
			logger.error("Error File is created");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void sendFile(String filePath, String folderName, String key) throws FileNotFoundException {

	}

}
