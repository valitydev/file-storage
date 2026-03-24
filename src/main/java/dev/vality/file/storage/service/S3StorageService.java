package dev.vality.file.storage.service;

import dev.vality.file.storage.FileData;
import dev.vality.file.storage.NewFileResult;
import dev.vality.file.storage.configuration.properties.S3SdkV2Properties;
import dev.vality.file.storage.util.DamselUtil;
import dev.vality.msgpack.Value;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class S3StorageService {

    private static final String CREATED_AT = "x-vality-created-at";
    private static final String METADATA = "x-vality-metadata-";
    private static final String FILENAME_PARAM = "filename=";
    private static final Duration MIN_PRESIGN_DURATION = Duration.ofSeconds(1);

    private final S3SdkV2Properties s3SdkV2Properties;
    private final S3Client s3SdkV2Client;
    private final S3Presigner s3Presigner;
    private final S3FileMetadataService s3FileMetadataService;

    @PostConstruct
    public void init() {
        if (!doesBucketExist()) {
            createBucket();
            enableBucketVersioning();
        }
    }

    public NewFileResult createNewFile(Map<String, Value> metadata, Instant expirationTime) {
        var fileId = UUID.randomUUID().toString();
        s3FileMetadataService.uploadFileMetadata(metadata, fileId);
        var url = presignUploadUrl(expirationTime, fileId);
        return new NewFileResult(fileId, url.toString());
    }

    public URL generateDownloadUrl(String fileId, Instant expirationTime) {
        var versions = getObjectVersions(fileId);
        var responseCache = new HashMap<String, GetObjectResponse>();
        var fileVersionId = getFileVersionId(fileId, versions, responseCache);
        PresignedGetObjectRequest presignedRequest = getPresignedRequest(fileId, expirationTime, fileVersionId);
        return presignedRequest.url();
    }

    public FileData getFileData(String fileId) {
        var versions = getObjectVersions(fileId);
        var responseCache = new HashMap<String, GetObjectResponse>();
        var fileMetadataVersionId = getFileMetadataVersionId(fileId, versions, responseCache);
        var metadataObjectResponse = getObject(fileId, fileMetadataVersionId, responseCache);
        var fileMetadata = getFileMetadata(fileId, fileMetadataVersionId, metadataObjectResponse);
        var fileVersionId = getFileVersionId(fileId, versions, responseCache);
        var fileObjectResponse = getObject(fileId, fileVersionId, responseCache);
        String fileName = getFileName(fileId, fileVersionId, fileObjectResponse);
        return new FileData(
                fileMetadata.fileId(),
                fileName,
                fileMetadata.createdAt(),
                fileMetadata.metadata());
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
                throw new IllegalStateException(String.format(
                        "Failed to check bucket on exist, bucketName=%s", s3SdkV2Properties.getBucketName()));
            }
            return true;
        } catch (NoSuchBucketException ex) {
            log.info("Bucket does not exist, bucketName={}", s3SdkV2Properties.getBucketName());
            return false;
        } catch (S3Exception ex) {
            throw new IllegalStateException(
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
                        log.info("Check created bucket result {}:{}", response.statusCode(), response.statusText());
                        if (response.isSuccessful()) {
                            log.info("Bucket has been created, bucketName={}", s3SdkV2Properties.getBucketName());
                        } else {
                            throw new IllegalStateException(String.format(
                                    "Failed to create bucket, bucketName=%s", s3SdkV2Properties.getBucketName()));
                        }
                    });
        } catch (S3Exception ex) {
            throw new IllegalStateException(
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
            log.info("Check enable versioning bucket result {}:{}", response.statusCode(), response.statusText());
            if (response.isSuccessful()) {
                log.info("Versioning bucket has been enabled, bucketName={}", s3SdkV2Properties.getBucketName());
            } else {
                throw new IllegalStateException(String.format(
                        "Failed to enable bucket versioning, bucketName=%s", s3SdkV2Properties.getBucketName()));
            }
        } catch (S3Exception ex) {
            throw new IllegalStateException(
                    String.format("Failed to enable bucket versioning, " +
                            "bucketName=%s", s3SdkV2Properties.getBucketName()),
                    ex);
        }
    }

    private URL presignUploadUrl(Instant expirationTime, String fileId) {
        var presignRequest = PutObjectPresignRequest.builder()
                .signatureDuration(resolvePresignDuration(expirationTime))
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
            log.info("Check list object versions result {}:{}", response.statusCode(), response.statusText());
            if (response.isSuccessful()) {
                var versions = listObjectVersionsResponse.versions();
                log.info("List object versions has been got, fileId={}, bucketName={}, objectVersions={}, ",
                        fileId,
                        s3SdkV2Properties.getBucketName(),
                        versions.stream().map(ObjectVersion::toString).collect(Collectors.joining(",")));
                return versions;
            } else {
                throw new IllegalStateException(String.format(
                        "Failed to get list object versions, fileId=%s, bucketName=%s",
                        fileId, s3SdkV2Properties.getBucketName()));
            }
        } catch (S3Exception ex) {
            throw new IllegalStateException(
                    String.format(
                            "Failed to get list object versions, fileId=%s, bucketName=%s",
                            fileId, s3SdkV2Properties.getBucketName()),
                    ex);
        }
    }

    private String getFileMetadataVersionId(String fileId,
                                            List<ObjectVersion> versions,
                                            Map<String, GetObjectResponse> responseCache) {
        return findVersionId(
                fileId,
                versions,
                responseCache,
                this::isMetadataVersion,
                "file metadata");
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
            throw new IllegalStateException(String.format(
                    "Object version with file metadata is empty, " +
                            "fileId=%s, fileMetadataVersionId=%s, bucketName=%s",
                    fileId, versionId, s3SdkV2Properties.getBucketName()));
        }
    }

    private String getFileVersionId(String fileId,
                                    List<ObjectVersion> versions,
                                    Map<String, GetObjectResponse> responseCache) {
        return findVersionId(
                fileId,
                versions,
                responseCache,
                this::isUploadedFileVersion,
                "uploaded file");
    }

    private String findVersionId(String fileId,
                                 List<ObjectVersion> versions,
                                 Map<String, GetObjectResponse> responseCache,
                                 Predicate<GetObjectResponse> responsePredicate,
                                 String objectKind) {
        return versions.stream()
                .sorted(Comparator.comparing(ObjectVersion::lastModified).reversed())
                .map(ObjectVersion::versionId)
                .filter(versionId -> responsePredicate.test(getObject(fileId, versionId, responseCache)))
                .findFirst()
                .orElseThrow(() -> new NoSuchElementException(String.format(
                        "Object version with %s not found, fileId=%s, bucketName=%s, objectVersions=%s",
                        objectKind,
                        fileId,
                        s3SdkV2Properties.getBucketName(),
                        versions.stream().map(ObjectVersion::toString).collect(Collectors.joining(",")))));
    }

    private boolean isMetadataVersion(GetObjectResponse objectResponse) {
        return objectResponse.hasMetadata() && objectResponse.metadata().containsKey(CREATED_AT);
    }

    private boolean isUploadedFileVersion(GetObjectResponse objectResponse) {
        return Optional.ofNullable(objectResponse.contentDisposition())
                .or(() -> objectResponse.sdkHttpResponse().firstMatchingHeader("Content-Disposition"))
                .isPresent();
    }

    private GetObjectResponse getObject(String fileId,
                                        String versionId,
                                        Map<String, GetObjectResponse> responseCache) {
        return responseCache.computeIfAbsent(versionId, id -> loadObject(fileId, id));
    }

    private GetObjectResponse loadObject(String fileId, String versionId) {
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
                throw new IllegalStateException(String.format(
                        "Failed to get object version with file, " +
                                "fileId=%s, fileVersionId=%s, bucketName=%s",
                        fileId, versionId, s3SdkV2Properties.getBucketName()));
            }
        } catch (S3Exception ex) {
            throw new IllegalStateException(
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
                .orElseThrow(() -> new IllegalStateException(String.format(
                        "Header 'Content-Disposition' in object version with file is empty, " +
                                "fileId=%s, fileVersionId=%s, bucketName=%s",
                        fileId, fileVersionId, s3SdkV2Properties.getBucketName())));
    }

    private String extractFileName(String contentDisposition) {
        int fileNameIndex = contentDisposition.lastIndexOf(FILENAME_PARAM) + FILENAME_PARAM.length();
        return contentDisposition.substring(fileNameIndex).replaceAll("\"", "");
    }

    private PresignedGetObjectRequest getPresignedRequest(String fileId, Instant expirationTime, String fileVersionId) {
        var presignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(resolvePresignDuration(expirationTime))
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

    private Duration resolvePresignDuration(Instant expirationTime) {
        var signatureDuration = Duration.between(Instant.now(), expirationTime);
        if (signatureDuration.isNegative() || signatureDuration.isZero()) {
            throw new IllegalArgumentException(String.format(
                    "Expiration time must be in the future, expirationTime=%s", expirationTime));
        }
        if (signatureDuration.compareTo(MIN_PRESIGN_DURATION) < 0) {
            return MIN_PRESIGN_DURATION;
        }
        return signatureDuration;
    }

    private record FileMetadata(String fileId, String createdAt, Map<String, Value> metadata) {

    }
}
