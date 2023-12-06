package org.utils;

public class CustomSparkS3Writer
{}/*{


    private final AmazonS3 s3Client;
    private final String bucketName;
    private final String key;
    private final boolean createVersion;
    private final int partSize;

    public CustomSparkS3Writer(AmazonS3 s3Client, String bucketName, String key, boolean createVersion, int partSize) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.key = key;
        this.createVersion = createVersion;
        this.partSize = partSize; // in bytes
    }

    public void append(Dataset<Row> data) {
        // Check if file exists
        GetObjectMetadata metadata = null;
        try {
            metadata = s3Client.getObjectMetadata(new GetObjectRequest(bucketName, key));
        } catch (NoSuchKeyException e) {
            // File doesn't exist, create a new one
        }

        // Determine upload strategy: single part or multipart
        boolean multipart = metadata != null && metadata.getContentLength() + data.count() * rowSize() > partSize;

        // Single part upload
        if (!multipart) {
            byte[] encodedData = encodeData(data); // convert data to desired format (e.g., CSV)
            PutObjectRequest putRequest = new PutObjectRequest(bucketName, key, encodedData);
            s3Client.putObject(putRequest);
            return;
        }

        // Multipart upload
        List<PartETag> partETags = new ArrayList<>();
        long offset = 0;
        while (offset < data.count()) {
            long partLength = Math.min(partSize, data.count() * rowSize() - offset);
            byte[] partData = encodePart(data, offset, partLength); // encode specific data part
            UploadPartRequest uploadPartRequest = new UploadPartRequest(bucketName, key, partETags.size(), partData);
            UploadPartResult partResult = s3Client.uploadPart(uploadPartRequest);
            partETags.add(partResult.getPartETag());
            offset += partLength;
        }
        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(bucketName, key, partETags);
        s3Client.completeMultipartUpload(completeRequest);
    }

    private byte[] encodeData(Dataset<Row> data) {
        // Implement logic to encode the entire DataFrame into desired format (e.g., CSV)
    }

    private byte[] encodePart(Dataset<Row> data, long offset, long length) {
        // Implement logic to encode a specific data part of the DataFrame
    }

    private long rowSize() {
        // Estimate the average size of a single row in bytes for calculation
    }

    // Optional: Add methods to handle object versions, concurrency control, etc.
}
*/
