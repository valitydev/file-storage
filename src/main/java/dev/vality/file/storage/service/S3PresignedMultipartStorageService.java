package dev.vality.file.storage.service;

import dev.vality.file.storage.*;
import dev.vality.file.storage.AbortMultipartUploadRequest;
import dev.vality.file.storage.configuration.properties.S3SdkV2Properties;
import dev.vality.msgpack.Value;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.UploadPartPresignRequest;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

@Service
@Slf4j
@RequiredArgsConstructor
public class S3PresignedMultipartStorageService {

    private static final String FILENAME_METADATA = "filename";
    private static final String CONTENT_MD5_HEADER = "Content-MD5";
    private static final String CHECKSUM_SHA256_HEADER = "x-amz-checksum-sha256";

    private final S3SdkV2Properties s3SdkV2Properties;
    private final S3Client s3SdkV2Client;
    private final S3Presigner s3Presigner;
    private final S3FileMetadataService s3FileMetadataService;

    public PresignedMultipartUpload createMultipartUpload(Map<String, Value> metadata) {
        String fileName = extractFileName(metadata);
        String fileId = UUID.randomUUID().toString();
        CreateMultipartUploadResponse createResponse = createS3MultipartUpload(fileId, fileName);
        String multipartUploadId = createResponse.uploadId();
        try {
            S3FileMetadataService.StoredFileMetadata storedMetadata = s3FileMetadataService.uploadMultipartFileMetadata(
                    metadata,
                    fileId,
                    multipartUploadId,
                    FileUploadStatus.pending_upload);
            return buildMultipartUpload(storedMetadata);
        } catch (RuntimeException ex) {
            abortQuietly(fileId, multipartUploadId);
            throw ex;
        }
    }

    public PresignedMultipartUpload getMultipartUpload(String fileId) {
        S3FileMetadataService.StoredFileMetadata storedMetadata = s3FileMetadataService.getLatestFileMetadata(fileId)
                .orElseThrow(() -> new NoSuchElementException(
                        String.format("Multipart upload not found, fileId=%s, bucketName=%s",
                                fileId, s3SdkV2Properties.getBucketName())));
        return buildMultipartUpload(storedMetadata);
    }

    public PresignMultipartUploadPartResult presignMultipartUploadPart(PresignMultipartUploadPartRequest request) {
        String fileId = request.getFileDataId();
        String multipartUploadId = request.getMultipartUploadId();
        S3FileMetadataService.StoredFileMetadata storedMetadata =
                requirePendingMultipartUpload(fileId, multipartUploadId);
        Instant expiresAt = Instant.now().plus(s3SdkV2Properties.getMultipartUrlTtl());
        try {
            UploadPartRequest uploadPartRequest = buildUploadPartRequest(request, fileId, multipartUploadId);
            var presignRequest = UploadPartPresignRequest.builder()
                    .signatureDuration(s3SdkV2Properties.getMultipartUrlTtl())
                    .uploadPartRequest(uploadPartRequest)
                    .build();
            var presignedRequest = s3Presigner.presignUploadPart(presignRequest);
            log.info("Multipart upload part url was presigned, fileId={}, bucketName={}, uploadId={}, sequencePart={}",
                    fileId, s3SdkV2Properties.getBucketName(), multipartUploadId, request.getSequencePart());
            log.debug("Presigned multipart http request={}", presignedRequest.httpRequest());
            return new PresignMultipartUploadPartResult()
                    .setSequencePart(request.getSequencePart())
                    .setUploadUrl(presignedRequest.url().toString())
                    .setExpiresAt(expiresAt.toString())
                    .setRequiredHeaders(buildRequiredHeaders(request));
        } catch (SdkException ex) {
            throw new IllegalStateException(
                    String.format("Failed to presign multipart upload part, fileId=%s, bucketName=%s, uploadId=%s, " +
                                    "sequencePart=%s, uploadStatus=%s",
                            fileId,
                            s3SdkV2Properties.getBucketName(),
                            multipartUploadId,
                            request.getSequencePart(),
                            storedMetadata.uploadStatus()),
                    ex);
        }
    }

    public CompletePresignedMultipartUploadResult completeMultipartUpload(
            CompletePresignedMultipartUploadRequest request) {
        String fileId = request.getFileDataId();
        String multipartUploadId = request.getMultipartUploadId();
        requirePendingMultipartUpload(fileId, multipartUploadId);
        validateCompletedParts(request.getCompletedParts(), fileId, multipartUploadId);
        try {
            var completeRequest = buildRequest(request, fileId, multipartUploadId);
            var completeResponse = s3SdkV2Client.completeMultipartUpload(completeRequest);
            var response = completeResponse.sdkHttpResponse();
            log.info("Check complete presigned multipart upload result {}:{}",
                    response.statusCode(), response.statusText());
            if (!response.isSuccessful()) {
                throw new IllegalStateException(String.format(
                        "Failed to complete presigned multipart upload, fileId=%s, bucketName=%s, uploadId=%s",
                        fileId, s3SdkV2Properties.getBucketName(), multipartUploadId));
            }
            S3FileMetadataService.StoredFileMetadata storedMetadata = s3FileMetadataService.updateMultipartFileMetadata(
                    fileId,
                    multipartUploadId,
                    FileUploadStatus.uploaded);
            log.info("Presigned multipart upload was completed, fileId={}, bucketName={}, uploadId={}",
                    fileId, s3SdkV2Properties.getBucketName(), multipartUploadId);
            return new CompletePresignedMultipartUploadResult()
                    .setFileDataId(fileId)
                    .setMultipartUploadId(multipartUploadId)
                    .setFileUrl(buildObjectUrl(fileId))
                    .setUploadStatus(storedMetadata.uploadStatus());
        } catch (SdkException ex) {
            throw new IllegalStateException(
                    String.format(
                            "Failed to complete presigned multipart upload, fileId=%s, bucketName=%s, uploadId=%s",
                            fileId, s3SdkV2Properties.getBucketName(), multipartUploadId),
                    ex);
        }
    }

    public void abortMultipartUpload(AbortMultipartUploadRequest request) {
        String fileId = request.getFileDataId();
        String multipartUploadId = request.getMultipartUploadId();
        requirePendingMultipartUpload(fileId, multipartUploadId);
        try {
            var abortRequest = software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest.builder()
                    .bucket(s3SdkV2Properties.getBucketName())
                    .key(fileId)
                    .uploadId(multipartUploadId)
                    .build();
            AbortMultipartUploadResponse abortResponse = s3SdkV2Client.abortMultipartUpload(abortRequest);
            var response = abortResponse.sdkHttpResponse();
            log.info("Check abort presigned multipart upload result {}:{}",
                    response.statusCode(), response.statusText());
            if (!response.isSuccessful()) {
                throw new IllegalStateException(String.format(
                        "Failed to abort presigned multipart upload, fileId=%s, bucketName=%s, uploadId=%s",
                        fileId, s3SdkV2Properties.getBucketName(), multipartUploadId));
            }
            s3FileMetadataService.updateMultipartFileMetadata(fileId, multipartUploadId, FileUploadStatus.aborted);
        } catch (SdkException ex) {
            throw new IllegalStateException(
                    String.format("Failed to abort presigned multipart upload, fileId=%s, bucketName=%s, uploadId=%s",
                            fileId, s3SdkV2Properties.getBucketName(), multipartUploadId),
                    ex);
        }
    }

    private CreateMultipartUploadResponse createS3MultipartUpload(String fileId, String fileName) {
        try {
            var createRequest = CreateMultipartUploadRequest.builder()
                    .bucket(s3SdkV2Properties.getBucketName())
                    .key(fileId)
                    .contentDisposition("attachment;filename=" + URLEncoder.encode(fileName, StandardCharsets.UTF_8))
                    .build();
            CreateMultipartUploadResponse createResponse = s3SdkV2Client.createMultipartUpload(createRequest);
            var response = createResponse.sdkHttpResponse();
            log.info("Check create presigned multipart upload result {}:{}",
                    response.statusCode(), response.statusText());
            if (!response.isSuccessful()) {
                throw new IllegalStateException(String.format(
                        "Failed to create presigned multipart upload, fileId=%s, bucketName=%s",
                        fileId, s3SdkV2Properties.getBucketName()));
            }
            log.info("Presigned multipart upload was created, fileId={}, bucketName={}, uploadId={}",
                    fileId, s3SdkV2Properties.getBucketName(), createResponse.uploadId());
            return createResponse;
        } catch (SdkException ex) {
            throw new IllegalStateException(
                    String.format("Failed to create presigned multipart upload, fileId=%s, bucketName=%s",
                            fileId, s3SdkV2Properties.getBucketName()),
                    ex);
        }
    }

    private UploadPartRequest buildUploadPartRequest(PresignMultipartUploadPartRequest request,
                                                     String fileId,
                                                     String multipartUploadId) {
        var builder = UploadPartRequest.builder()
                .bucket(s3SdkV2Properties.getBucketName())
                .key(fileId)
                .uploadId(multipartUploadId)
                .partNumber(request.getSequencePart());
        if (request.isSetContentLength()) {
            builder.contentLength(request.getContentLength());
        }
        if (request.isSetContentMd5()) {
            builder.contentMD5(request.getContentMd5());
        }
        if (request.isSetChecksumSha256()) {
            builder.checksumSHA256(request.getChecksumSha256());
        }
        return builder.build();
    }

    private Map<String, String> buildRequiredHeaders(PresignMultipartUploadPartRequest request) {
        Map<String, String> headers = new LinkedHashMap<>();
        if (request.isSetContentMd5()) {
            headers.put(CONTENT_MD5_HEADER, request.getContentMd5());
        }
        if (request.isSetChecksumSha256()) {
            headers.put(CHECKSUM_SHA256_HEADER, request.getChecksumSha256());
        }
        return headers;
    }

    private software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest buildRequest(
            CompletePresignedMultipartUploadRequest request,
            String fileId,
            String multipartUploadId) {
        List<CompletedPart> completedParts = request.getCompletedParts().stream()
                .sorted(Comparator.comparingInt(CompletedPresignedMultipart::getSequencePart))
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

    private CompletedPart buildCompletedPart(CompletedPresignedMultipart completedMultipart) {
        return CompletedPart.builder()
                .eTag(completedMultipart.getEtag())
                .partNumber(completedMultipart.getSequencePart())
                .build();
    }

    private void validateCompletedParts(List<CompletedPresignedMultipart> completedParts,
                                        String fileId,
                                        String multipartUploadId) {
        if (completedParts == null || completedParts.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                    "Completed parts must not be empty, fileId=%s, uploadId=%s",
                    fileId, multipartUploadId));
        }
        var distinctPartNumbers = completedParts.stream()
                .map(CompletedPresignedMultipart::getSequencePart)
                .distinct()
                .count();
        if (distinctPartNumbers != completedParts.size()) {
            throw new IllegalArgumentException(String.format(
                    "Completed parts must contain unique sequencePart values, fileId=%s, uploadId=%s",
                    fileId, multipartUploadId));
        }
        completedParts.forEach(part -> {
            if (part.getSequencePart() <= 0) {
                throw new IllegalArgumentException(String.format(
                        "Completed part sequencePart must be positive, fileId=%s, uploadId=%s",
                        fileId, multipartUploadId));
            }
            if (part.getEtag() == null || part.getEtag().isBlank()) {
                throw new IllegalArgumentException(String.format(
                        "Completed part etag must not be blank, fileId=%s, uploadId=%s, sequencePart=%s",
                        fileId, multipartUploadId, part.getSequencePart()));
            }
        });
    }

    private S3FileMetadataService.StoredFileMetadata requirePendingMultipartUpload(String fileId,
                                                                                   String multipartUploadId) {
        S3FileMetadataService.StoredFileMetadata storedMetadata = s3FileMetadataService.getLatestFileMetadata(fileId)
                .orElseThrow(() -> new NoSuchElementException(
                        String.format("Multipart upload not found, fileId=%s, bucketName=%s",
                                fileId, s3SdkV2Properties.getBucketName())));
        if (!multipartUploadId.equals(storedMetadata.multipartUploadId())) {
            throw new IllegalStateException(String.format(
                    "Multipart upload id does not match stored state, fileId=%s, bucketName=%s, actualUploadId=%s, " +
                            "expectedUploadId=%s",
                    fileId,
                    s3SdkV2Properties.getBucketName(),
                    multipartUploadId,
                    storedMetadata.multipartUploadId()));
        }
        if (storedMetadata.uploadStatus() != FileUploadStatus.pending_upload) {
            throw new IllegalStateException(String.format(
                    "Multipart upload is not pending, fileId=%s, bucketName=%s, uploadId=%s, uploadStatus=%s",
                    fileId,
                    s3SdkV2Properties.getBucketName(),
                    multipartUploadId,
                    storedMetadata.uploadStatus()));
        }
        return storedMetadata;
    }

    private PresignedMultipartUpload buildMultipartUpload(S3FileMetadataService.StoredFileMetadata storedMetadata) {
        PresignedMultipartUpload multipartUpload = new PresignedMultipartUpload()
                .setFileDataId(storedMetadata.fileId())
                .setMultipartUploadId(storedMetadata.multipartUploadId())
                .setCreatedAt(storedMetadata.createdAt())
                .setMetadata(storedMetadata.metadata())
                .setUploadStatus(storedMetadata.uploadStatus());
        Value fileName = storedMetadata.metadata().get(FILENAME_METADATA);
        if (fileName != null && fileName.isSetStr()) {
            multipartUpload.setFileName(fileName.getStr());
        }
        if (storedMetadata.uploadStatus() == FileUploadStatus.uploaded) {
            multipartUpload.setFileUrl(buildObjectUrl(storedMetadata.fileId()));
        }
        return multipartUpload;
    }

    private String buildObjectUrl(String fileId) {
        String objectUrl = s3SdkV2Client.utilities().getUrl(GetUrlRequest.builder()
                        .bucket(s3SdkV2Properties.getBucketName())
                        .key(fileId)
                        .build())
                .toExternalForm();
        log.info("Create object url for multipart uploaded file, url={}, fileId={}, bucketName={}",
                objectUrl, fileId, s3SdkV2Properties.getBucketName());
        return objectUrl;
    }

    private String extractFileName(Map<String, Value> metadata) {
        Value fileName = metadata.get(FILENAME_METADATA);
        if (fileName == null || !fileName.isSetStr() || fileName.getStr().isBlank()) {
            throw new IllegalArgumentException("Can't create multipart upload object without fileName");
        }
        return fileName.getStr();
    }

    private void abortQuietly(String fileId, String multipartUploadId) {
        try {
            s3SdkV2Client.abortMultipartUpload(software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest
                    .builder()
                    .bucket(s3SdkV2Properties.getBucketName())
                    .key(fileId)
                    .uploadId(multipartUploadId)
                    .build());
        } catch (Exception ex) {
            log.warn("Failed to abort leaked multipart upload after metadata write failure, fileId={}, uploadId={}",
                    fileId, multipartUploadId, ex);
        }
    }
}
