package com.t3c.anchel.storageregistration.Implements;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.BindException;
import java.sql.SQLException;
import java.util.ArrayList;
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
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.t3c.anchel.common.DbConfiguration;
import com.t3c.anchel.common.SMConstants;

public abstract class StorageAdapterClass {

	private static final Logger logger = LoggerFactory.getLogger(StorageAdapterClass.class);

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

	public abstract void sendFile(String filePath, String folderName, String key) throws FileNotFoundException;

	public abstract void GetAll(String bucketName, String folderName);

	public void add(String filepath, String fileID) throws AmazonServiceException, AmazonClientException, InterruptedException, SQLException {
		logger.debug("Inserting amazoneDTO : {} to aws S3 bucket.");

		credentials = new BasicAWSCredentials(this.accesskey, this.secretkey);
		s3client = AmazonS3ClientBuilder.standard().withCredentials(new AWSCredentialsProvider() {
			public void refresh() {
			}

			public AWSCredentials getCredentials() {
				return credentials;
			}
		}).withRegion(this.region).build();

		File file = new File(filepath);
		long contentLength = file.length();
		long partSize = 5 * 1024 * 1024;

		logger.debug("File Is Uploading Wait..... :");
		logger.debug("File Name : " + file);

		List<PartETag> partETags = new ArrayList<PartETag>();

		InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(this.bucketName, fileID);
		InitiateMultipartUploadResult initResponse = s3client.initiateMultipartUpload(initRequest);

		long filePosition = 0;
		for (int i = 1; filePosition < contentLength; i++) {
			partSize = Math.min(partSize, (contentLength - filePosition));
			logger.debug("Multipart File Uploading is in Progress with Part " + i);

			UploadPartRequest uploadRequest = new UploadPartRequest().withBucketName(this.bucketName).withKey(fileID)
					.withUploadId(initResponse.getUploadId()).withPartNumber(i).withFileOffset(filePosition)
					.withFile(file).withPartSize(partSize);

			UploadPartResult uploadResult = s3client.uploadPart(uploadRequest);
			partETags.add(uploadResult.getPartETag());

			filePosition += partSize;
		}

		CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(this.bucketName, fileID,
				initResponse.getUploadId(), partETags);
		s3client.completeMultipartUpload(compRequest);
		logger.debug("Multipart upload is completed with file id :" + fileID);
		String url = ((AmazonS3Client) s3client).getResourceUrl(this.bucketName, fileID);
		logger.debug("File uploading into amazons3 is completed :" +url);
		
		AccessClass accessClass = new AccessClass();
		accessClass.insert(fileID, contentLength, url);
	}

}
