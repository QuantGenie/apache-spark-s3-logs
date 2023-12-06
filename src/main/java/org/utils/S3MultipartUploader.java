package org.utils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

public class S3MultipartUploader {

    private int partSize = 5 * 1024 * 1024; // 5MB part size (adjust as needed)



    protected void initiateMultipartUpload(AmazonS3 s3Client, String bucketName, String key, byte[] content) {
        InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(bucketName, key);
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initiateRequest);
        String uploadId = initResponse.getUploadId();

        try {
            List<PartETag> partETags = new ArrayList<>();
            int offset = 0;
            int partNumber = 1;

            while (offset < content.length) {
                int partSize = Math.min(this.partSize, content.length - offset);
                byte[] partBytes = new byte[partSize];
                System.arraycopy(content, offset, partBytes, 0, partSize);

                UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(bucketName)
                        .withKey(key)
                        .withUploadId(uploadId)
                        .withPartNumber(partNumber)
                        .withInputStream(new ByteArrayInputStream(partBytes))
                        .withPartSize(partSize);

                UploadPartResult uploadPartResponse = s3Client.uploadPart(uploadRequest);
                partETags.add(uploadPartResponse.getPartETag());

                offset += partSize;
                partNumber++;
            }

            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
                    bucketName, key, uploadId, partETags);
            s3Client.completeMultipartUpload(completeRequest);
        } catch (Exception e) {
            //errorHandler.error("Error during multipart upload to S3", e, 0);
            s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId));
        }
    }
}
