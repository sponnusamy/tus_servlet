package org.tus.servlet.upload.s3;

import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tus.servlet.upload.Config;
import org.tus.servlet.upload.Datastore;
import org.tus.servlet.upload.FileInfo;
import org.tus.servlet.upload.Locker;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.UploadPartRequest;

public class S3Store implements Datastore {
    protected static final Logger log = LoggerFactory.getLogger(S3Store.class.getName());
    protected static String extensions = "creation,termination";
    private AmazonS3 s3Client;
    private String s3Bucket;
    private Map<String, FileInfo> uploadIdByFileId = new HashMap<>();
    private static final long MULTI_PART_SIZE = 5 * 1024 * 1024; // 5 MB

    private static final String AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID";
    private static final String AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY";
    private static final String AWS_S3_BUCKET = "AWS_S3_BUCKET";
    private static final String AWS_DEFAULT_REGION = "AWS_DEFAULT_REGION";

    @Override
    public void init(Config config, Locker locker) throws Exception {
        String awsAccessKeyId = System.getenv(AWS_ACCESS_KEY_ID) != null ? System.getenv(AWS_ACCESS_KEY_ID)
                : config.getStringValue(AWS_ACCESS_KEY_ID);
        String awsSecretAccessKey = System.getenv(AWS_SECRET_ACCESS_KEY) != null ? System.getenv(AWS_SECRET_ACCESS_KEY)
                : config.getStringValue(AWS_SECRET_ACCESS_KEY);

        String awsRegion = System.getenv(AWS_DEFAULT_REGION) != null ? System.getenv(AWS_DEFAULT_REGION)
                : config.getStringValue(AWS_DEFAULT_REGION);
        String awsS3Bucket = System.getenv(AWS_S3_BUCKET) != null ? System.getenv(AWS_S3_BUCKET)
                : config.getStringValue(AWS_S3_BUCKET);

        if (awsAccessKeyId == null || awsSecretAccessKey == null || awsRegion == null || awsS3Bucket == null)
            throw new Exception(String.format("Either %s or %s or %s or %s are missing", AWS_ACCESS_KEY_ID,
                    AWS_SECRET_ACCESS_KEY, AWS_S3_BUCKET, AWS_DEFAULT_REGION));

        AWSStaticCredentialsProvider creds = new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey));

        this.s3Client = AmazonS3ClientBuilder.standard().withCredentials(creds).withRegion(awsRegion).build();
        this.s3Bucket = awsS3Bucket;

    }

    @Override
    public String getExtensions() {
        return extensions;
    }

    @Override
    public void create(FileInfo fi) throws Exception {
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(this.s3Bucket, fi.id);
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
        fi.decodedMetadata.put("s3UploadId", initResponse.getUploadId());
        saveFileInfo(fi);
    }

    @Override
    public long write(HttpServletRequest request, String id, long offset, long max) throws Exception {
        FileInfo fi = getFileInfo(id);
        if (fi == null)
            throw new Exception("File not found");

        String uploadId = getS3uploadId(id);
        if (uploadId == null)
            throw new Exception("S3 Upload Id is not found");

        int currentPart = Integer.parseInt(fi.decodedMetadata.get("currentPart"));
        // Upload the file parts.
        long filePosition = fi.offset;
        long contentLength = fi.entityLength;
        long transferred = 0;

        try (ReadableByteChannel rbc = Channels.newChannel(request.getInputStream());
                InputStream is = Channels.newInputStream(rbc)) {
            for (int i = currentPart + 1; filePosition < contentLength; i++) {
                long partSize = Math.min(MULTI_PART_SIZE, (fi.entityLength - filePosition));

                log.info(String.format("Uploading part %s, at file position %s", i, filePosition));
                UploadPartRequest uploadRequest = new UploadPartRequest().withBucketName(s3Bucket).withKey(fi.id)
                        .withUploadId(uploadId).withPartNumber(i).withInputStream(is).withPartSize(partSize);
                uploadRequest.getRequestClientOptions().setReadLimit((int) MULTI_PART_SIZE);
                s3Client.uploadPart(uploadRequest);
                filePosition += partSize;
                transferred += partSize;
            }
        } catch (Exception e) {
            log.error(String.format("File Upload Interrupted for key %s", fi.id));
        }
        return transferred;
    }

    @Override
    public FileInfo getFileInfo(String id) throws Exception {
        FileInfo fi = uploadIdByFileId.get(id);
        if (fi == null)
            return fi;
        String uploadId = getS3uploadId(id);

        // Calculate offset
        ListPartsRequest listPartsRequest = new ListPartsRequest(s3Bucket, id, uploadId);
        PartListing result = s3Client.listParts(listPartsRequest);
        int currentPart = Math.max(result.getNextPartNumberMarker(), 0);
        AtomicLong offset = new AtomicLong();
        result.getParts().forEach(p -> offset.addAndGet(p.getSize()));

        fi.offset = offset.get();
        fi.decodedMetadata.put("currentPart", String.valueOf(currentPart));

        return uploadIdByFileId.get(id);
    }

    @Override
    public void saveFileInfo(FileInfo fileInfo) throws Exception {
        uploadIdByFileId.put(fileInfo.id, fileInfo);
    }

    @Override
    public void terminate(String id) throws Exception {
        AbortMultipartUploadRequest abortReq = new AbortMultipartUploadRequest(s3Bucket, id, getS3uploadId(id));
        s3Client.abortMultipartUpload(abortReq);

    }

    @Override
    public void finish(String id) throws Exception {
        ListPartsRequest request = new ListPartsRequest(s3Bucket, id, getS3uploadId(id));

        PartListing result = s3Client.listParts(request);
        List<PartETag> partETags = result.getParts().stream().map(p -> new PartETag(p.getPartNumber(), p.getETag()))
                .collect(Collectors.toList());

        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(s3Bucket, id, getS3uploadId(id),
                partETags);
        s3Client.completeMultipartUpload(compRequest);
    }

    @Override
    public void destroy() throws Exception {

    }

    private String getS3uploadId(String id) {
        FileInfo fi = uploadIdByFileId.get(id);
        return fi.decodedMetadata.get("s3UploadId");
    }

}
