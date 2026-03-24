package dev.vality.file.storage.service;

import dev.vality.file.storage.FileUploadStatus;
import dev.vality.file.storage.configuration.properties.S3SdkV2Properties;
import dev.vality.file.storage.util.DamselUtil;
import dev.vality.msgpack.Value;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class S3FileMetadataService {

    private static final String FILE_ID = "x-vality-file-id";
    private static final String CREATED_AT = "x-vality-created-at";
    private static final String METADATA = "x-vality-metadata-";
    private static final String MULTIPART_UPLOAD_ID = "x-vality-multipart-upload-id";
    private static final String UPLOAD_STATUS = "x-vality-upload-status";

    private final S3SdkV2Properties s3SdkV2Properties;
    private final S3Client s3SdkV2Client;

    public void uploadFileMetadata(Map<String, Value> metadata, String fileId) {
        putObjectMetadata(fileId, buildS3Metadata(metadata, fileId, null, null, Instant.now().toString()));
    }

    public StoredFileMetadata uploadMultipartFileMetadata(Map<String, Value> metadata,
                                                          String fileId,
                                                          String multipartUploadId,
                                                          FileUploadStatus uploadStatus) {
        String createdAt = Instant.now().toString();
        putObjectMetadata(
                fileId,
                buildS3Metadata(metadata, fileId, multipartUploadId, uploadStatus, createdAt));
        return new StoredFileMetadata(fileId, createdAt, metadata, multipartUploadId, uploadStatus);
    }

    public StoredFileMetadata updateMultipartFileMetadata(String fileId,
                                                          String multipartUploadId,
                                                          FileUploadStatus uploadStatus) {
        StoredFileMetadata existing = getLatestFileMetadata(fileId)
                .orElseThrow(() -> new NoSuchElementException(
                        String.format("File metadata not found, fileId=%s, bucketName=%s",
                                fileId, s3SdkV2Properties.getBucketName())));
        String actualMultipartUploadId = multipartUploadId != null ? multipartUploadId : existing.multipartUploadId();
        putObjectMetadata(
                fileId,
                buildS3Metadata(
                        existing.metadata(),
                        fileId,
                        actualMultipartUploadId,
                        uploadStatus,
                        existing.createdAt()));
        return new StoredFileMetadata(
                fileId,
                existing.createdAt(),
                existing.metadata(),
                actualMultipartUploadId,
                uploadStatus);
    }

    public Optional<StoredFileMetadata> getLatestFileMetadata(String fileId) {
        try {
            List<ObjectVersion> versions = s3SdkV2Client.listObjectVersions(ListObjectVersionsRequest.builder()
                            .bucket(s3SdkV2Properties.getBucketName())
                            .prefix(fileId)
                            .build())
                    .versions();
            return versions.stream()
                    .sorted(Comparator.comparing(ObjectVersion::lastModified).reversed())
                    .map(ObjectVersion::versionId)
                    .map(versionId -> loadObjectMetadata(fileId, versionId))
                    .flatMap(Optional::stream)
                    .findFirst();
        } catch (S3Exception ex) {
            throw new IllegalStateException(
                    String.format("Failed to get latest file metadata, fileId=%s, bucketName=%s",
                            fileId, s3SdkV2Properties.getBucketName()),
                    ex);
        }
    }

    private void putObjectMetadata(String fileId, Map<String, String> s3Metadata) {
        try {
            var request = PutObjectRequest.builder()
                    .bucket(s3SdkV2Properties.getBucketName())
                    .key(fileId)
                    .metadata(s3Metadata)
                    .build();
            var putObjectResponse = s3SdkV2Client.putObject(request, RequestBody.empty());
            var response = putObjectResponse.sdkHttpResponse();
            log.info("Check upload object version with file metadata result {}:{}",
                    response.statusCode(), response.statusText());
            if (response.isSuccessful()) {
                log.info("Object version with file metadata was uploaded, fileId={}, bucketName={}",
                        fileId, s3SdkV2Properties.getBucketName());
            } else {
                throw new IllegalStateException(String.format(
                        "Failed to upload object version with file metadata, fileId=%s, bucketName=%s",
                        fileId, s3SdkV2Properties.getBucketName()));
            }
        } catch (S3Exception ex) {
            throw new IllegalStateException(
                    String.format("Failed to upload object version with file metadata, fileId=%s, bucketName=%s",
                            fileId, s3SdkV2Properties.getBucketName()),
                    ex);
        }
    }

    private HashMap<String, String> buildS3Metadata(Map<String, Value> metadata,
                                                    String fileId,
                                                    String multipartUploadId,
                                                    FileUploadStatus uploadStatus,
                                                    String createdAt) {
        var s3Metadata = new HashMap<String, String>();
        s3Metadata.put(FILE_ID, fileId);
        s3Metadata.put(CREATED_AT, createdAt);
        if (multipartUploadId != null) {
            s3Metadata.put(MULTIPART_UPLOAD_ID, multipartUploadId);
        }
        if (uploadStatus != null) {
            s3Metadata.put(UPLOAD_STATUS, Integer.toString(uploadStatus.getValue()));
        }
        metadata.forEach((key, value) -> s3Metadata.put(METADATA + key, DamselUtil.toJsonString(value)));
        return s3Metadata;
    }

    private Optional<StoredFileMetadata> loadObjectMetadata(String fileId, String versionId) {
        GetObjectResponse objectResponse = s3SdkV2Client.getObject(
                GetObjectRequest.builder()
                        .bucket(s3SdkV2Properties.getBucketName())
                        .key(fileId)
                        .versionId(versionId)
                        .build(),
                (response, inputStream) -> response);
        if (!objectResponse.hasMetadata() || !objectResponse.metadata().containsKey(CREATED_AT)) {
            return Optional.empty();
        }
        Map<String, String> s3Metadata = objectResponse.metadata();
        Map<String, Value> metadata = s3Metadata.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(METADATA) && entry.getValue() != null)
                .collect(Collectors.toMap(
                        entry -> entry.getKey().substring(METADATA.length()),
                        entry -> DamselUtil.fromJson(entry.getValue(), Value.class)));
        FileUploadStatus uploadStatus = Optional.ofNullable(s3Metadata.get(UPLOAD_STATUS))
                .map(Integer::parseInt)
                .map(FileUploadStatus::findByValue)
                .orElse(null);
        return Optional.of(new StoredFileMetadata(
                fileId,
                s3Metadata.get(CREATED_AT),
                metadata,
                s3Metadata.get(MULTIPART_UPLOAD_ID),
                uploadStatus));
    }

    public record StoredFileMetadata(String fileId,
                                     String createdAt,
                                     Map<String, Value> metadata,
                                     String multipartUploadId,
                                     FileUploadStatus uploadStatus) {
    }
}
