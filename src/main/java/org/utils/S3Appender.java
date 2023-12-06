package org.utils;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

public class S3Appender extends AppenderSkeleton {

    private String bucketName;
    private String keyPrefix;
    private static String region;
    private static AmazonS3 s3Client = null;

    private StringBuilder buffer = new StringBuilder();

    private static String accessKey = "";
    private static String secretKey = "";

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

    private void getS3Client()
    {
        BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        System.out.println(region);

        s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(region)
                .build();
    }


    @Override
    protected void append(LoggingEvent event) {
        if (layout == null) {
            errorHandler.error("No layout set for the appender named [" + name + "].");
            return;
        }

        if (s3Client == null)
        {
            getS3Client();
        }

        String logMessage = layout.format(event);

        buffer.append(logMessage);

        try {

            byte[] content = buffer.toString().getBytes(StandardCharsets.UTF_8);

            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(content.length);
            System.out.println("Key is:" + keyPrefix+"appdev" + ".log");
            PutObjectResult result = s3Client.putObject(new PutObjectRequest(bucketName, keyPrefix+"appdev" + ".log", new ByteArrayInputStream(content), metadata));
            System.out.println("Uploaded object ETag: " + result.getETag());

        } catch (Exception e) {
            errorHandler.error("Error uploading log message to S3", e, 0);
        }
    }

    @Override
    public void close() {
        // Cleanup code, if any
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }
}
