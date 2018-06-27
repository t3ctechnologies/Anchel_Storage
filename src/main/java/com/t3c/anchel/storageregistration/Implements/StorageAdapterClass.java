package com.t3c.anchel.storageregistration.Implements;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.BindException;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.t3c.anchel.common.DbConfiguration;
import com.t3c.anchel.common.SMConstants;
import com.t3c.anchel.dto.StorageAccessDTO;

public abstract class StorageAdapterClass {

	private static final Logger logger = LoggerFactory.getLogger(StorageAccessDTO.class);

	String fileName = null;
	// AmazoneDTO amazoneDTO = null;
	static AmazonS3 s3client = null;
	static AWSCredentials credentials = null;

	String accesskey = null;
	String secretkey = null;
	String bucketName = null;
	String region = null;
	Properties properties = null;

	public StorageAdapterClass() {
		properties = new DbConfiguration().getDbProperties();
		this.accesskey = properties.getProperty(SMConstants.ACCESS_KEY);
		this.secretkey = properties.getProperty(SMConstants.SECRET_KEY);
		this.bucketName = properties.getProperty(SMConstants.BUCKET_NAME);
		this.region = properties.getProperty(SMConstants.CLIENT_REGION);
	}

	public StorageAdapterClass create(StorageAdapterClass entity) throws BindException, IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	public StorageAdapterClass load(StorageAdapterClass entity) throws BindException, IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	public StorageAdapterClass update(StorageAdapterClass entity) throws BindException, IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	public List<StorageAdapterClass> findAll() {
		// TODO Auto-generated method stub
		return null;
	}

	public abstract void DeleteFile(String bucketName, String folderName);

	public abstract StorageAccessDTO sendFile(String filePath, String folderName, String key)
			throws FileNotFoundException;

	public abstract void GetAll(String bucketName, String folderName);

	public String add(StorageAccessDTO storageaccess) throws AmazonServiceException, AmazonClientException, InterruptedException {
		logger.debug("Inserting amazoneDTO : {} to aws S3 bucket.", storageaccess);

		credentials = new BasicAWSCredentials(this.accesskey, this.secretkey);
		s3client = AmazonS3ClientBuilder.standard().withCredentials(new AWSCredentialsProvider() {
			public void refresh() {
			}

			public AWSCredentials getCredentials() {
				return credentials;
			}
		}).withRegion(this.region).build();

		File file = new File(storageaccess.getFile());

		logger.debug("File Is Uploading Wait..... :" + storageaccess);
		logger.debug("File Name : " + file);

		TransferManager transferManager = TransferManagerBuilder.standard().withS3Client(s3client)
				.withMultipartUploadThreshold((long) (5 * 1024 * 1025)).build();

		Upload upload = transferManager.upload(this.bucketName, storageaccess.getKey().toString(), file);

		upload.waitForCompletion();

		fileName = ((AmazonS3Client) s3client).getResourceUrl(this.bucketName,
				storageaccess.getFolder() + File.separator + storageaccess.getKey());

		logger.debug("Folder Name :" + storageaccess.getFolder());
		logger.debug("File Name :" + storageaccess.getKey());
		logger.debug("Uploaded successfully..");
		if (upload.isDone())
			transferManager.shutdownNow();
		return fileName;

	}

	public StorageAccessDTO get(String id) {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("unchecked")
	public boolean delete(StorageAccessDTO storageaccess) {
		credentials = new BasicAWSCredentials(this.accesskey, this.secretkey);
		s3client = new AmazonS3Client(credentials);
		java.util.List<S3ObjectSummary> fileList = (List<S3ObjectSummary>) s3client.listObjects(this.bucketName,
				storageaccess.getFolder());
		for (S3ObjectSummary file : fileList) {

			s3client.deleteObject(this.bucketName, file.getKey());
		}
		return false;
	}

}
