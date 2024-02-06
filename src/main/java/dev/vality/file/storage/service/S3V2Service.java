package dev.vality.file.storage.service;

import dev.vality.file.storage.*;
import dev.vality.file.storage.CompleteMultipartUploadRequest;
import dev.vality.file.storage.configuration.properties.S3SdkV2Properties;
import dev.vality.file.storage.service.exception.FileNotFoundException;
import dev.vality.file.storage.service.exception.StorageException;
import dev.vality.file.storage.util.DamselUtil;
import dev.vality.msgpack.Value;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import javax.annotation.PostConstruct;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class S3V2Service implements StorageService {

    private static final String FILE_ID = "x-vality-file-id";
    private static final String CREATED_AT = "x-vality-created-at";
    private static final String METADATA = "x-vality-metadata-";
    private static final String FILENAME_METADATA = "filename";
    private static final String FILENAME_PARAM = "filename=";

    private final S3SdkV2Properties s3SdkV2Properties;
    private final S3Client s3SdkV2Client;
    private final S3Presigner s3Presigner;

    @PostConstruct
    public void init() {
        if (!doesBucketExist()) {
            createBucket();
            enableBucketVersioning();
        }
    }

    @Override
    public NewFileResult createNewFile(Map<String, Value> metadata, Instant expirationTime) {
        var fileId = UUID.randomUUID().toString();
        uploadFileMetadata(metadata, fileId);
        var url = presignUploadUrl(expirationTime, fileId);
        return new NewFileResult(fileId, url.toString());
    }

    @Override
    public URL generateDownloadUrl(String fileId, Instant expirationTime) {
        var versions = getObjectVersions(fileId);
        checkFileExist(fileId, versions);
        var fileVersionId = getFileVersionId(fileId, versions);
        PresignedGetObjectRequest presignedRequest = getPresignedRequest(fileId, expirationTime, fileVersionId);
        return presignedRequest.url();
    }

    @Override
    public FileData getFileData(String fileId) {
        var versions = getObjectVersions(fileId);
        checkFileExist(fileId, versions);
        var fileMetadataVersionId = getFileMetadataVersionId(fileId, versions);
        var metadataObjectResponse = getObject(fileId, fileMetadataVersionId);
        var fileMetadata = getFileMetadata(fileId, fileMetadataVersionId, metadataObjectResponse);
        var fileVersionId = getFileVersionId(fileId, versions);
        var fileObjectResponse = getObject(fileId, fileVersionId);
        String fileName = getFileName(fileId, fileVersionId, fileObjectResponse);
        return new FileData(
                fileMetadata.getFileId(),
                fileName,
                fileMetadata.getCreatedAt(),
                fileMetadata.getMetadata());
    }

    @Override
    public FileData getMultipartFileData(String fileId) {
        var versions = getObjectVersions(fileId);
        if (CollectionUtils.isEmpty(versions)) {
            throw new FileNotFoundException(String.format(
                    "Failed to check object version with file on exist, fileId=%s, bucketName=%s ",
                    fileId,
                    s3SdkV2Properties.getBucketName()));
        }
        var fileVersionId = getFileVersionId(fileId, versions);
        var objectResponse = getObject(fileId, fileVersionId);
        var fileMetadata = getFileMetadata(fileId, fileVersionId, objectResponse);
        Map<String, Value> metadata = fileMetadata.getMetadata();
        String fileName = metadata.get(FILENAME_METADATA).getStr();
        return new FileData(
                fileId,
                fileName,
                fileMetadata.getCreatedAt(),
                metadata);
    }

    // единственный доступный вариант проверки существования бакета на данный момент через catch
    // в репе сдк висит таска https://github.com/aws/aws-sdk-java-v2/issues/392#issuecomment-880224831
    // в первой версии сдк тоже через catch проверка на существование
    // разница только в том, что проверка идет через метод S3Client#getBucketAcl
    // во второй версии тоже есть этот метод, не уверен в чем разница с выбранным вариантом,
    // но везде советуют его
    private boolean doesBucketExist() {
        try {
            var request = HeadBucketRequest.builder()
                    .bucket(s3SdkV2Properties.getBucketName())
                    .build();
            var headBucketResponse = s3SdkV2Client.headBucket(request);
            var response = headBucketResponse.sdkHttpResponse();
            log.info(String.format("Check exist bucket result %d:%s",
                    response.statusCode(), response.statusText()));
            if (response.isSuccessful()) {
                log.info("Bucket is exist, bucketName={}", s3SdkV2Properties.getBucketName());
            } else {
                throw new StorageException(String.format(
                        "Failed to check bucket on exist, bucketName=%s", s3SdkV2Properties.getBucketName()));
            }
            return true;
        } catch (NoSuchBucketException ex) {
            log.info("Bucket does not exist, bucketName={}", s3SdkV2Properties.getBucketName());
            return false;
        } catch (S3Exception ex) {
            throw new StorageException(
                    String.format("Failed to check bucket on exist, bucketName=%s", s3SdkV2Properties.getBucketName()),
                    ex);
        }
    }

    private void createBucket() {
        try {
            var s3Waiter = s3SdkV2Client.waiter();
            var createBucketRequest = CreateBucketRequest.builder()
                    .bucket(s3SdkV2Properties.getBucketName())
                    .build();
            s3SdkV2Client.createBucket(createBucketRequest);
            var headBucketRequest = HeadBucketRequest.builder()
                    .bucket(s3SdkV2Properties.getBucketName())
                    .build();
            // Wait until the bucket is created and print out the response.
            s3Waiter.waitUntilBucketExists(headBucketRequest)
                    .matched()
                    .response()
                    .ifPresent(headBucketResponse -> {
                        var response = headBucketResponse.sdkHttpResponse();
                        log.info(String.format("Check created bucket result %d:%s",
                                response.statusCode(), response.statusText()));
                        if (response.isSuccessful()) {
                            log.info("Bucket has been created, bucketName={}", s3SdkV2Properties.getBucketName());
                        } else {
                            throw new StorageException(String.format(
                                    "Failed to create bucket, bucketName=%s", s3SdkV2Properties.getBucketName()));
                        }
                    });
        } catch (S3Exception ex) {
            throw new StorageException(
                    String.format("Failed to create bucket, bucketName=%s", s3SdkV2Properties.getBucketName()),
                    ex);
        }
    }

    private void enableBucketVersioning() {
        try {
            var request = PutBucketVersioningRequest.builder()
                    .bucket(s3SdkV2Properties.getBucketName())
                    .versioningConfiguration(VersioningConfiguration.builder()
                            .status(BucketVersioningStatus.ENABLED)
                            .build())
                    .build();
            var putBucketVersioningResponse = s3SdkV2Client.putBucketVersioning(request);
            var response = putBucketVersioningResponse.sdkHttpResponse();
            log.info(String.format("Check enable versioning bucket result %d:%s",
                    response.statusCode(), response.statusText()));
            if (response.isSuccessful()) {
                log.info("Versioning bucket has been enabled, bucketName={}", s3SdkV2Properties.getBucketName());
            } else {
                throw new StorageException(String.format(
                        "Failed to enable bucket versioning, bucketName=%s", s3SdkV2Properties.getBucketName()));
            }
        } catch (S3Exception ex) {
            throw new StorageException(
                    String.format("Failed to enable bucket versioning, " +
                            "bucketName=%s", s3SdkV2Properties.getBucketName()),
                    ex);
        }
    }

    private void uploadFileMetadata(Map<String, Value> metadata, String fileId) {
        try {
            HashMap<String, String> s3Metadata = buildS3Metadata(metadata, fileId);
            var request = PutObjectRequest.builder()
                    .bucket(s3SdkV2Properties.getBucketName())
                    .key(fileId)
                    .metadata(s3Metadata)
                    .build();
            var putObjectResponse = s3SdkV2Client.putObject(request, RequestBody.empty());
            var response = putObjectResponse.sdkHttpResponse();
            log.info(String.format("Check upload object version with file metadata result %d:%s",
                    response.statusCode(), response.statusText()));
            if (response.isSuccessful()) {
                log.info("Object version with file metadata was uploaded, fileId={}, bucketName={}",
                        fileId, s3SdkV2Properties.getBucketName());
            } else {
                throw new StorageException(String.format(
                        "Failed to upload object version with file metadata, fileId=%s, bucketName=%s",
                        fileId, s3SdkV2Properties.getBucketName()));
            }
        } catch (S3Exception ex) {
            throw new StorageException(
                    String.format("Failed to upload object version with file metadata, fileId=%s, bucketName=%s",
                            fileId, s3SdkV2Properties.getBucketName()),
                    ex);
        }
    }

    private URL presignUploadUrl(Instant expirationTime, String fileId) {
        var presignRequest = PutObjectPresignRequest.builder()
                .signatureDuration(Duration.between(Instant.now(), expirationTime))
                .putObjectRequest(PutObjectRequest.builder()
                        .bucket(s3SdkV2Properties.getBucketName())
                        .key(fileId)
                        .build())
                .build();
        var presignedRequest = s3Presigner.presignPutObject(presignRequest);
        log.info("Upload url was presigned, fileId={}, bucketName={}", fileId, s3SdkV2Properties.getBucketName());
        log.debug("Presigned http request={}", presignedRequest.httpRequest().toString());
        return presignedRequest.url();
    }

    private List<ObjectVersion> getObjectVersions(String fileId) {
        try {
            var request = ListObjectVersionsRequest.builder()
                    .bucket(s3SdkV2Properties.getBucketName())
                    .prefix(fileId)
                    .build();
            var listObjectVersionsResponse = s3SdkV2Client.listObjectVersions(request);
            var response = listObjectVersionsResponse.sdkHttpResponse();
            log.info(String.format("Check list object versions result %d:%s",
                    response.statusCode(), response.statusText()));
            if (response.isSuccessful()) {
                var versions = listObjectVersionsResponse.versions();
                log.info("List object versions has been got, fileId={}, bucketName={}, objectVersions={}, ",
                        fileId,
                        s3SdkV2Properties.getBucketName(),
                        versions.stream().map(ObjectVersion::toString).collect(Collectors.joining(",")));
                return versions;
            } else {
                throw new StorageException(String.format(
                        "Failed to get list object versions, fileId=%s, bucketName=%s",
                        fileId, s3SdkV2Properties.getBucketName()));
            }
        } catch (S3Exception ex) {
            throw new StorageException(
                    String.format(
                            "Failed to get list object versions, fileId=%s, bucketName=%s",
                            fileId, s3SdkV2Properties.getBucketName()),
                    ex);
        }
    }

    private void checkFileExist(String fileId, List<ObjectVersion> versions) {
        if (!doesFileExist(versions)) {
            throw new FileNotFoundException(String.format(
                    "Failed to check object version with file on exist, fileId=%s, bucketName=%s, objectVersions=%s, ",
                    fileId,
                    s3SdkV2Properties.getBucketName(),
                    versions.stream().map(ObjectVersion::toString).collect(Collectors.joining(","))));
        }
    }

    private Boolean doesFileExist(List<ObjectVersion> versions) {
        // должно быть 2 ревизии — 1я это метаданные, 2ая это сам загруженный файл
        return versions.size() == 2;
//                && versions.stream()
//                .filter(v -> v.size() > 0)
//                .map(v -> true)
//                .findFirst()
//                .orElse(false);
    }

    private String getFileMetadataVersionId(String fileId, List<ObjectVersion> versions) {
        return versions.stream()
                .filter(Predicate.not(ObjectVersion::isLatest))
                .findFirst()
                .orElseThrow(() -> new StorageException(String.format(
                        "Object version with file metadata not found, fileId=%s, bucketName=%s, objectVersions=%s, ",
                        fileId,
                        s3SdkV2Properties.getBucketName(),
                        versions.stream().map(ObjectVersion::toString).collect(Collectors.joining(",")))))
                .versionId();
    }

    private FileMetadata getFileMetadata(String fileId,
                                         String versionId,
                                         GetObjectResponse objectResponse) {
        if (objectResponse.hasMetadata() && !objectResponse.metadata().isEmpty()) {
            var s3Metadata = objectResponse.metadata();
            var metadata = s3Metadata.entrySet().stream()
                    .filter(entry -> entry.getKey().startsWith(METADATA)
                            && entry.getValue() != null)
                    .collect(Collectors.toMap(
                            o -> o.getKey().substring(METADATA.length()),
                            o -> DamselUtil.fromJson(o.getValue(), Value.class)));
            return new FileMetadata(fileId, s3Metadata.get(CREATED_AT), metadata);
        } else {
            throw new StorageException(String.format(
                    "Object version with file metadata is empty, " +
                            "fileId=%s, fileMetadataVersionId=%s, bucketName=%s",
                    fileId, versionId, s3SdkV2Properties.getBucketName()));
        }
    }

    private String getFileVersionId(String fileId, List<ObjectVersion> versions) {
        return versions.stream()
                .filter(ObjectVersion::isLatest)
                .findFirst()
                .orElseThrow(() -> new StorageException(String.format(
                        "Object version with file not found, fileId=%s, bucketName=%s, objectVersions=%s, ",
                        fileId,
                        s3SdkV2Properties.getBucketName(),
                        versions.stream().map(ObjectVersion::toString).collect(Collectors.joining(",")))))
                .versionId();
    }

    private GetObjectResponse getObject(String fileId, String versionId) {
        try {
            var request = GetObjectRequest.builder()
                    .bucket(s3SdkV2Properties.getBucketName())
                    .key(fileId)
                    .versionId(versionId)
                    .build();
            GetObjectResponse objectResponse = s3SdkV2Client.getObject(
                    request,
                    (getObjectResponse, inputStream) -> getObjectResponse);
            SdkHttpResponse response = objectResponse.sdkHttpResponse();
            log.info(String.format("Check get object result %d:%s",
                    response.statusCode(), response.statusText()));
            if (response.isSuccessful()) {
                log.info("Object version with file has been got, " +
                                "fileId={}, fileVersionId={}, bucketName={}",
                        fileId, versionId, s3SdkV2Properties.getBucketName());
                return objectResponse;
            } else {
                throw new StorageException(String.format(
                        "Failed to get object version with file, " +
                                "fileId=%s, fileVersionId=%s, bucketName=%s",
                        fileId, versionId, s3SdkV2Properties.getBucketName()));
            }
        } catch (S3Exception ex) {
            throw new StorageException(
                    String.format(
                            "Failed to get object version with file, " +
                                    "fileId=%s, fileVersionId=%s, bucketName=%s",
                            fileId, versionId, s3SdkV2Properties.getBucketName()),
                    ex);
        }
    }

    private String getFileName(String fileId,
                               String fileVersionId,
                               GetObjectResponse getObjectResponse) {
        var response = getObjectResponse.sdkHttpResponse();
        return Optional.ofNullable(getObjectResponse.contentDisposition())
                .map(this::extractFileName)
                .or(() -> response.firstMatchingHeader("Content-Disposition")
                        .map(this::extractFileName))
                .orElseThrow(() -> new StorageException(String.format(
                        "Header 'Content-Disposition' in object version with file is empty, " +
                                "fileId=%s, fileVersionId=%s, bucketName=%s",
                        fileId, fileVersionId, s3SdkV2Properties.getBucketName())));
    }

    private String extractFileName(String contentDisposition) {
        int fileNameIndex = contentDisposition.lastIndexOf(FILENAME_PARAM) + FILENAME_PARAM.length();
        return contentDisposition.substring(fileNameIndex).replaceAll("\"", "");
    }


    @Override
    public CreateMultipartUploadResult createMultipartUpload(Map<String, Value> metadata) {
        if (!metadata.containsKey(FILENAME_METADATA)) {
            throw new StorageException("Can't create multipart upload object without fileName");
        }
        var fileId = UUID.randomUUID().toString();
        uploadFileMetadata(metadata, fileId);
        try {
            HashMap<String, String> s3Metadata = buildS3Metadata(metadata, fileId);
            String filename = URLEncoder.encode(metadata.get(FILENAME_METADATA).getStr(), StandardCharsets.UTF_8);
            var createRequest = CreateMultipartUploadRequest.builder()
                    .bucket(s3SdkV2Properties.getBucketName())
                    .key(fileId)
                    .metadata(s3Metadata)
                    .contentDisposition("attachment;filename=" + filename)
                    .build();
            CreateMultipartUploadResponse createResponse = s3SdkV2Client.createMultipartUpload(createRequest);
            var response = createResponse.sdkHttpResponse();
            log.info("Check create multipart upload object with file metadata result {}:{}",
                    response.statusCode(), response.statusText());
            if (response.isSuccessful()) {
                log.info("Multipart upload was created, fileId={}, bucketName={}, uploadId={}",
                        fileId, s3SdkV2Properties.getBucketName(), createResponse.uploadId());
            } else {
                throw new StorageException(String.format(
                        "Failed to create multipart upload, fileId=%s, bucketName=%s",
                        fileId, s3SdkV2Properties.getBucketName()));
            }
            return new CreateMultipartUploadResult()
                    .setFileDataId(fileId)
                    .setMultipartUploadId(createResponse.uploadId());
        } catch (SdkException ex) {
            throw new StorageException(
                    String.format("Failed to create multipart upload, fileId=%s, bucketName=%s",
                            fileId, s3SdkV2Properties.getBucketName()),
                    ex);
        }
    }

    private static HashMap<String, String> buildS3Metadata(Map<String, Value> metadata, String fileId) {
        var s3Metadata = new HashMap<String, String>();
        s3Metadata.put(FILE_ID, fileId);
        s3Metadata.put(CREATED_AT, Instant.now().toString());
        metadata.forEach((key, value) -> s3Metadata.put(METADATA + key, DamselUtil.toJsonString(value)));
        return s3Metadata;
    }

    @Override
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
                throw new StorageException(String.format(
                        "Failed to upload file part, fileId=%s, bucketName=%s, uploadId=%s",
                        fileId, s3SdkV2Properties.getBucketName(), multipartUploadId));
            }
            return new UploadMultipartResult()
                    .setPartId(uploadPartResponse.eTag())
                    .setSequencePart(requestData.getSequencePart());
        } catch (SdkException ex) {
            throw new StorageException(
                    String.format("Failed to upload file part, fileId=%s, bucketName=%s, uploadId=%s",
                            fileId, s3SdkV2Properties.getBucketName(), multipartUploadId),
                    ex);
        }
    }

    @Override
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
                throw new StorageException(String.format(
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
            throw new StorageException(
                    String.format("Failed to complete multipart upload, fileId=%s, bucketName=%s, uploadId=%s",
                            fileId, s3SdkV2Properties.getBucketName(), multipartUploadId),
                    ex);
        }
    }

    @Override
    public URL generateMultipartDownloadUrl(String fileId, Instant expirationTime) {
        var versions = getObjectVersions(fileId);
        if (CollectionUtils.isEmpty(versions)) {
            throw new FileNotFoundException(String.format(
                    "Failed to check object version with file on exist, fileId=%s, bucketName=%s ",
                    fileId,
                    s3SdkV2Properties.getBucketName()));
        }
        var fileVersionId = getFileVersionId(fileId, versions);
        PresignedGetObjectRequest presignedRequest = getPresignedRequest(fileId, expirationTime, fileVersionId);
        return presignedRequest.url();
    }

    private PresignedGetObjectRequest getPresignedRequest(String fileId, Instant expirationTime, String fileVersionId) {
        var presignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(Duration.between(Instant.now(), expirationTime))
                .getObjectRequest(GetObjectRequest.builder()
                        .bucket(s3SdkV2Properties.getBucketName())
                        .key(fileId)
                        .versionId(fileVersionId)
                        .build())
                .build();
        var presignedRequest = s3Presigner.presignGetObject(presignRequest);
        log.info("Download url was presigned, fileId={}, bucketName={}, isBrowserExecutable={}",
                fileId, s3SdkV2Properties.getBucketName(), presignedRequest.isBrowserExecutable());
        log.debug("Presigned http request={}", presignedRequest.httpRequest().toString());
        return presignedRequest;
    }

    private software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest buildRequest(
            CompleteMultipartUploadRequest request,
            String fileId,
            String multipartUploadId) {
        List<CompletedPart> completedParts = request.getCompletedParts().stream()
                .map(completedMultipart -> CompletedPart.builder()
                        .eTag(completedMultipart.getPartId())
                        .partNumber(completedMultipart.getSequencePart())
                        .build())
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

    @RequiredArgsConstructor
    @Getter
    @ToString
    private static class FileMetadata {

        private final String fileId;
        private final String createdAt;
        private final Map<String, Value> metadata;

    }
}
