package org.utils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

public class S3MultipartUploader {

    private int partNumber = 1;
    private List<PartETag> partETags = new ArrayList<>();



    protected void initiateMultipartUpload(AmazonS3 s3Client, String bucketName, String key, String uploadId, byte[] content, boolean isLastPartFile)
    {
        try {
                UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(bucketName)
                        .withKey(key)
                        .withUploadId(uploadId)
                        .withPartNumber(partNumber)
                        .withInputStream(new ByteArrayInputStream(content))
                        .withPartSize(content.length);
                UploadPartResult uploadPartResponse = s3Client.uploadPart(uploadRequest);
                partETags.add(uploadPartResponse.getPartETag());
                partNumber++;
            if(isLastPartFile) {
                CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
                        bucketName, key, uploadId, partETags);
                s3Client.completeMultipartUpload(completeRequest);
            }
        } catch (Exception e) {
            s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId));
        }
    }
}
