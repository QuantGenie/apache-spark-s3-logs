package org.utils;


import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class S3MultiPartAppender extends AppenderSkeleton implements Runnable{

    private String bucketName;
    private String keyPrefix;
    private String region;

    AmazonS3 s3Client;
    private static String accessKey = "";
    private static String secretKey = "";
    private int partSize = 5 * 1024 *1024; // 5MB (adjust as needed but minimum 5MB is mandatory for each part except for last part )
    private StringBuilder buffer = new StringBuilder();

    private S3MultipartUploader uploader;

    private String key;
    private String uploadID;

    // Setters for configuration properties
    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    @Override
    protected void append(LoggingEvent event) {

        buffer.append(layout.format(event));

        if (buffer.length() >= partSize) {
            // If the buffer size exceeds the partSize, initiate multipart upload
            uploadBufferToS3(false);
            buffer.setLength(0); // Clear the buffer
        }
    }

    private void ConfigureMultipartUpload(String bucketName, String key)
    {
        InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(bucketName, key);
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initiateRequest);
        uploadID = initResponse.getUploadId();
    }


    private void uploadBufferToS3(boolean isLastPartFile) {
        byte[] content = buffer.toString().getBytes(StandardCharsets.UTF_8);
        uploader.initiateMultipartUpload(s3Client, bucketName, key, uploadID, content, isLastPartFile);
    }

    @Override
    public void activateOptions() {

        if (layout == null) {
            errorHandler.error("No layout set for the appender named [" + name + "].");
            return;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(this));

        BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(region)
                .build();

        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
        String timestamp = now.format(formatter);

        key = keyPrefix+"_"+timestamp+".log";
        uploader = new S3MultipartUploader();
        ConfigureMultipartUpload(bucketName,key);
    }
    @Override
    public void close() {
        // Upload any remaining content in the buffer when closing the appender
        uploadBufferToS3(true);
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }


    @Override
    public void run() {
        this.close();
    }
}
