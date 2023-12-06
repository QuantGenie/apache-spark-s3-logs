package org.utils;


import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import java.nio.charset.StandardCharsets;

public class S3MultiPartAppender extends AppenderSkeleton implements Runnable{

    private String bucketName;
    private String keyPrefix;
    private String region;

    AmazonS3 s3Client;
    private static String accessKey = "";
    private static String secretKey = "";
    private int partSize = 5 * 1024 * 1024; // 5MB part size (adjust as needed)
    private StringBuilder buffer = new StringBuilder();

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
        if (layout == null) {
            errorHandler.error("No layout set for the appender named [" + name + "].");
            return;
        }

        String logMessage = layout.format(event);
        buffer.append(logMessage);

        if (buffer.length() >= partSize) {
            // If the buffer size exceeds the partSize, initiate multipart upload
            uploadBufferToS3();
            buffer.setLength(0); // Clear the buffer
        }
    }


    private void uploadBufferToS3() {

        String key = keyPrefix + "appdev_multipart" + ".log";
        byte[] content = buffer.toString().getBytes(StandardCharsets.UTF_8);

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(content.length);

        S3MultipartUploader uploader = new S3MultipartUploader();
        System.out.println("S3 client while uploading: "+s3Client);
        uploader.initiateMultipartUpload(s3Client, bucketName, key, content);
    }

    // The initiateMultipartUpload method is unchanged from the previous example

    @Override
    public void activateOptions() {
        System.out.println("Initializing the S3 Appender");
        Runtime.getRuntime().addShutdownHook(new Thread(this));
        BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

        s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(region)
                .build();
        System.out.println("S3 client is created:"+s3Client);
    }
    @Override
    public void close() {
        // Upload any remaining content in the buffer when closing the appender
        System.out.println("close is called");

        if (buffer.length() > 0) {
            System.out.println("Content is there while closing..flushing!");
            uploadBufferToS3();
        }
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

/*
class ShutdownSignalHandler implements Runnable {

    private final S3MultiPartAppender appender;

    public ShutdownSignalHandler(S3MultiPartAppender appender) {
        this.appender = appender;
    }

    @Override
    public void run() {
        appender.close();
    }
}*/
