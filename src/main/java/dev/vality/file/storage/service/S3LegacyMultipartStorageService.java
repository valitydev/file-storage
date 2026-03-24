package dev.vality.file.storage.service;

import dev.vality.file.storage.*;
import dev.vality.file.storage.CompleteMultipartUploadRequest;
import dev.vality.file.storage.configuration.properties.S3SdkV2Properties;
import dev.vality.msgpack.Value;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.util.List;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class S3LegacyMultipartStorageService {

    private final S3SdkV2Properties s3SdkV2Properties;
    private final S3Client s3SdkV2Client;
    private final S3PresignedMultipartStorageService presignedMultipartStorageService;

    public CreateMultipartUploadResult createMultipartUpload(Map<String, Value> metadata) {
        var multipartUpload = presignedMultipartStorageService.createMultipartUpload(metadata);
        return new CreateMultipartUploadResult()
                .setFileDataId(multipartUpload.getFileDataId())
                .setMultipartUploadId(multipartUpload.getMultipartUploadId());
    }

    public UploadMultipartResult uploadMultipart(UploadMultipartRequestData requestData) {
        String fileId = requestData.getFileDataId();
        String multipartUploadId = requestData.getMultipartUploadId();
        try {
            var uploadPartRequest = UploadPartRequest.builder()
                    .bucket(s3SdkV2Properties.getBucketName())
                    .key(fileId)
                    .uploadId(multipartUploadId)
                    .partNumber(requestData.getSequencePart())
                    .contentLength((long) requestData.getContentLength())
                    .build();
            RequestBody requestBody = RequestBody.fromBytes(requestData.getContent());
            UploadPartResponse uploadPartResponse = s3SdkV2Client.uploadPart(uploadPartRequest, requestBody);
            var response = uploadPartResponse.sdkHttpResponse();
            log.info("Check file part upload result {}:{}",
                    response.statusCode(), response.statusText());
            if (response.isSuccessful()) {
                log.info("File part was uploaded, fileId={}, bucketName={}, uploadId={}, partId={}",
                        fileId, s3SdkV2Properties.getBucketName(), multipartUploadId, uploadPartResponse.eTag());
            } else {
                throw new IllegalStateException(String.format(
                        "Failed to upload file part, fileId=%s, bucketName=%s, uploadId=%s",
                        fileId, s3SdkV2Properties.getBucketName(), multipartUploadId));
            }
            return new UploadMultipartResult()
                    .setPartId(uploadPartResponse.eTag())
                    .setSequencePart(requestData.getSequencePart());
        } catch (SdkException ex) {
            throw new IllegalStateException(
                    String.format("Failed to upload file part, fileId=%s, bucketName=%s, uploadId=%s",
                            fileId, s3SdkV2Properties.getBucketName(), multipartUploadId),
                    ex);
        }
    }

    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request) {
        String fileId = request.getFileDataId();
        String multipartUploadId = request.getMultipartUploadId();
        try {
            var completeRequest = buildRequest(request, fileId, multipartUploadId);
            CompleteMultipartUploadResponse completeResponse = s3SdkV2Client.completeMultipartUpload(completeRequest);
            var response = completeResponse.sdkHttpResponse();
            log.info("Check complete multipart upload result {}:{}",
                    response.statusCode(), response.statusText());
            if (response.isSuccessful()) {
                log.info("Multipart upload was completed, fileId={}, bucketName={}, uploadId={}",
                        fileId, s3SdkV2Properties.getBucketName(), multipartUploadId);
            } else {
                throw new IllegalStateException(String.format(
                        "Failed to complete multipart upload, fileId=%s, bucketName=%s, uploadId=%s",
                        fileId, s3SdkV2Properties.getBucketName(), multipartUploadId));
            }
            String objectUrl = s3SdkV2Client.utilities().getUrl(GetUrlRequest.builder()
                            .bucket(s3SdkV2Properties.getBucketName())
                            .key(fileId)
                            .build())
                    .toExternalForm();
            log.info("Create url for multipart uploaded file, url={}, fileId={}, bucketName={}, uploadId={}",
                    objectUrl, fileId, s3SdkV2Properties.getBucketName(), multipartUploadId);
            return new CompleteMultipartUploadResult()
                    .setUploadUrl(objectUrl);
        } catch (SdkException ex) {
            throw new IllegalStateException(
                    String.format("Failed to complete multipart upload, fileId=%s, bucketName=%s, uploadId=%s",
                            fileId, s3SdkV2Properties.getBucketName(), multipartUploadId),
                    ex);
        }
    }

    private software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest buildRequest(
            CompleteMultipartUploadRequest request,
            String fileId,
            String multipartUploadId) {
        List<CompletedPart> completedParts = request.getCompletedParts().stream()
                .map(this::buildCompletedPart)
                .toList();
        CompletedMultipartUpload completedUpload = CompletedMultipartUpload.builder()
                .parts(completedParts)
                .build();
        return software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest.builder()
                .bucket(s3SdkV2Properties.getBucketName())
                .key(fileId)
                .uploadId(multipartUploadId)
                .multipartUpload(completedUpload)
                .build();
    }

    private CompletedPart buildCompletedPart(CompletedMultipart completedMultipart) {
        return CompletedPart.builder()
                .eTag(completedMultipart.getPartId())
                .partNumber(completedMultipart.getSequencePart())
                .build();
    }

}
