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
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.t3c.anchel.common.DbConfiguration;
import com.t3c.anchel.common.SMConstants;
import com.t3c.anchel.dto.StorageAccessDTO;

public class StorageAwsImpl extends StorageAdapterClass {

	String accesskey = null;
	String secretkey = null;
	Properties properties = null;
	String region = null;

	public StorageAwsImpl() {
		properties = new DbConfiguration().getDbProperties();
		this.accesskey = properties.getProperty(SMConstants.ACCESS_KEY);
		this.secretkey = properties.getProperty(SMConstants.SECRET_KEY);
		this.region = properties.getProperty(SMConstants.CLIENT_REGION);
	}

	private static final Logger logger = LoggerFactory.getLogger(StorageAwsImpl.class);

	public StorageAccessDTO sendFile(String filePath, String folderName, Long key) {
		logger.debug("file : {}, folder name :{}, key :{} received to store in amazone s3 Bucket.", filePath,
				folderName, key);
		StorageAccessDTO storagetypeDTO = null;
		storagetypeDTO = new StorageAccessDTO();
		storagetypeDTO.setFile(filePath);
		storagetypeDTO.setFolder(folderName);
		storagetypeDTO.setKey(key);

		String createpath1 = filePath.substring(filePath.indexOf('_') + 1);
		String createpath2 = createpath1.substring(createpath1.indexOf('_') + 1);
		File createFile = new File(filePath);
		String createpath3 = createFile.getParent()+File.separator.concat(createpath2);
		String insertedFileName = null;
		try {
			insertedFileName = new StorageAwsImpl().add(storagetypeDTO);
			
		} catch (AmazonServiceException e1) {
			e1.printStackTrace();
			createErrorFile(createpath3);
		} catch (AmazonClientException e1) {
			e1.printStackTrace();
			createErrorFile(createpath3);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
			createErrorFile(createpath3);
		}
		storagetypeDTO.setUrl(insertedFileName);
		if (!((insertedFileName) == null)) {
			createSuccessFile(createpath3);
			AccessClass accessClass = new AccessClass();
			try {
				String filePath1 = filePath.substring(filePath.indexOf('_') + 1);
				String filePath2 = filePath1.substring(filePath1.indexOf('_') + 1);
				accessClass.insert(filePath2, key, insertedFileName);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		logger.debug("Inserted File is :" + insertedFileName);

		return storagetypeDTO;

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
		credentials = new BasicAWSCredentials(this.accesskey, this.secretkey);
		s3client = AmazonS3ClientBuilder.standard().withCredentials(new AWSCredentialsProvider() {
			public void refresh() {
			}

			public AWSCredentials getCredentials() {
				return credentials;
			}
		}).withRegion(this.region).build();
		java.util.List<S3ObjectSummary> fileList = s3client.listObjects(bucketName, folderName).getObjectSummaries();
		for (S3ObjectSummary file : fileList) {
			s3client.deleteObject(bucketName, file.getKey());
		}
		s3client.deleteObject(bucketName, folderName);
		logger.debug("Deleted Successfully");
	}

	@Override
	public void GetAll(String bucketName, String folderName) {
		credentials = new BasicAWSCredentials(this.accesskey, this.secretkey);
		s3client = new AmazonS3Client(credentials);
		logger.debug("Listing objects");
		ObjectListing objectListing = s3client
				.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix("Some_Folder"));
		List<S3ObjectSummary> objList = null;
		if (objectListing != null) {
			objList = objectListing.getObjectSummaries();
		}
		int i = 1;
		for (S3ObjectSummary objectSummary : objList) {
			logger.debug(
					"id: {}, bucketName :{}, key :{}, eTag :{}, size :{}, lastModified :{}, storageClass :{}, owner :{}",
					i, objectSummary.getBucketName(), objectSummary.getKey(), objectSummary.getETag(),
					objectSummary.getSize(), objectSummary.getLastModified(), objectSummary.getStorageClass(),
					objectSummary.getOwner());
			i++;
		}

	}

	/*
	 * public static void GetFile(String strFileName, String strFolderName){
	 * GetById(G_DEFAULT_BUCKET_NAME, G_DEFAULT_FOLDER_NAME, strFileName,
	 * strFolderName + "//" + strFileName); }
	 */

	public void GetById(String bucketName, String folderName, String key, String targetFile) {
		logger.debug("getting object details for folderName :{}, key :{}", folderName, key);
		credentials = new BasicAWSCredentials(this.accesskey, this.secretkey);
		s3client = AmazonS3ClientBuilder.standard().withCredentials(new AWSCredentialsProvider() {
			public void refresh() {
			}

			public AWSCredentials getCredentials() {
				return credentials;
			}
		}).withRegion(this.region).build();

		try {

			S3Object s3object = s3client.getObject(new GetObjectRequest(bucketName, key));
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
	public StorageAccessDTO sendFile(String filePath, String folderName, String key) throws FileNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}
}
