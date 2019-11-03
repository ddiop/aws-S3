package com.example.bucket.service;

import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.WritableResource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

@Service
public class AwsS3Service {

    private static final Logger logger = LoggerFactory.getLogger(AwsS3Service.class);


    AmazonS3 amazonS3;

    @Autowired
    ResourceLoader resourceLoader;

    @Autowired
    ResourcePatternResolver resourcePatternResolver;

    public void downloadS3Object(String s3Url) throws IOException {
        Resource resource = resourceLoader.getResource(s3Url);
        File downloadedS3Object = new File(resource.getFilename());
        try (InputStream inputStream = resource.getInputStream()) {
            Files.copy(inputStream, downloadedS3Object.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    public void uploadFileToS3(File file, String s3Url) throws IOException {
        WritableResource resource = (WritableResource) resourceLoader.getResource(s3Url);
        try (OutputStream outputStream = resource.getOutputStream()) {
            Files.copy(file.toPath(), outputStream);
        }
    }

    public void downloadMultipleS3Objects(String s3UrlPattern) throws IOException {
        Resource[] allFileMatchingPatten = this.resourcePatternResolver.getResources(s3UrlPattern);
        for (Resource resource : allFileMatchingPatten) {
            String fileName = resource.getFilename();
            fileName = fileName.substring(0, fileName.lastIndexOf("/") + 1);
            File downloadedS3Object = new File(fileName);
            try (InputStream inputStream = resource.getInputStream()) {
                Files.copy(inputStream, downloadedS3Object.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    public void createBucket(String bucketName) {
        logger.debug("Creating S3 bucket: {}", bucketName);
        amazonS3.createBucket(bucketName);
        logger.info("{} bucket created successfully", bucketName);
    }

    public void downloadObject(String bucketName, String objectName) {
        String s3Url = "s3://" + bucketName + "/" + objectName;
        try {
            downloadS3Object(s3Url);
            logger.info("{} file download result: {}", objectName, new File(objectName).exists());
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void uploadObject(String bucketName, String objectName) {
        String s3Url = "s3://" + bucketName + "/" + objectName;
        File file = new File(objectName);
        try {
            uploadFileToS3(file, s3Url);
            logger.info("{} file uploaded to S3", objectName);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }




    public void deleteBucket(String bucketName) {
        logger.trace("Deleting S3 objects under {} bucket...", bucketName);
        ListObjectsV2Result listObjectsV2Result = amazonS3.listObjectsV2(bucketName);
        for (S3ObjectSummary objectSummary : listObjectsV2Result.getObjectSummaries()) {
            logger.info("Deleting S3 object: {}", objectSummary.getKey());
            amazonS3.deleteObject(bucketName, objectSummary.getKey());
        }
        logger.info("Deleting S3 bucket: {}", bucketName);
        amazonS3.deleteBucket(bucketName);
    }

}

