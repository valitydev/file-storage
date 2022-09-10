package dev.vality.file.storage.service;

import com.amazonaws.HttpMethod;
import com.amazonaws.SdkBaseException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import dev.vality.file.storage.FileData;
import dev.vality.file.storage.NewFileResult;
import dev.vality.file.storage.configuration.properties.S3Properties;
import dev.vality.file.storage.service.exception.ExtractMetadataException;
import dev.vality.file.storage.service.exception.FileNotFoundException;
import dev.vality.file.storage.service.exception.StorageException;
import dev.vality.file.storage.service.exception.WaitingUploadException;
import dev.vality.file.storage.util.DamselUtil;
import dev.vality.msgpack.Value;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.String.format;

@Service
@ConditionalOnProperty(value = "s3-sdk-v2.enabled", havingValue = "false")
@Slf4j
@RequiredArgsConstructor
public class S3Service implements StorageService {

    private static final String FILE_DATA_ID = "x-rbkmoney-file-data-id";
    private static final String FILE_ID = "x-rbkmoney-file-id";
    private static final String CREATED_AT = "x-rbkmoney-created-at";
    private static final String METADATA = "x-rbkmoney-metadata-";
    private static final String FILENAME_PARAM = "filename=";

    private final TransferManager transferManager;
    private final AmazonS3 s3Client;
    private final S3Properties s3Properties;
    private String bucketName;

    @PostConstruct
    public void init() {
        this.bucketName = s3Properties.getBucketName();
        bucketInit();
    }

    @Override
    public NewFileResult createNewFile(Map<String, Value> metadata, Instant expirationTime) {
        String fileDataId = id();
        String fileId = id();

        log.info("Trying to create NewFileResult, fileDataId='{}', bucketId='{}'", fileDataId, bucketName);

        FileDto fileDto = fileDto(fileDataId, fileId, metadata);

        // записывается неизменяемый фейковый файл с метаданными, в котором находится ссылка на реальный файл
        uploadEmptyFileWithMetadata(fileDataId, fileDto);

        // генерируется ссылка на выгрузку файла в хранилище напрямую в цеф по ключу fileId
        log.info("Generate Upload Url, fileDataId='{}', bucketId='{}'", fileDataId, bucketName);
        URL uploadUrl = generatePresignedUrl(fileDataId, fileId, expirationTime, HttpMethod.PUT);

        log.info("NewFileResult has been successfully created, fileDataId='{}', bucketId='{}'", fileDataId, bucketName);

        return new NewFileResult(fileDataId, uploadUrl.toString());
    }

    @Override
    public URL generateDownloadUrl(String fileDataId, Instant expirationTime) {
        log.info("Trying to generate Download Url, fileDataId='{}', bucketId='{}'", fileDataId, bucketName);

        // достается неизменяемый фейковый файл с метаданными
        log.info("Extract file, fileDataId='{}', bucketId='{}'", fileDataId, bucketName);
        FileDto fileDto = getFileDto(fileDataId);

        // генерируем ссылку на загрузку файла из хранилища напрямую в цеф по ключу fileId
        log.info("Generate Download Url, fileDataId='{}', bucketId='{}'", fileDataId, bucketName);
        URL generatePresignedUrl = generatePresignedUrl(
                fileDto.getFileDataId(),
                fileDto.getFileId(),
                expirationTime,
                HttpMethod.GET);

        log.info("Download Url has been successfully generate, fileDataId='{}', bucketId='{}'", fileDataId, bucketName);

        return generatePresignedUrl;
    }

    @Override
    public FileData getFileData(String fileDataId) {
        log.info("Trying to get FileData, fileDataId='{}', bucketId='{}'", fileDataId, bucketName);

        // достается неизменяемый фейковый файл с метаданными
        log.info("Extract file, fileDataId='{}', bucketId='{}'", fileDataId, bucketName);
        FileDto fileDto = getFileDto(fileDataId);

        // достается реальный файл формата s3
        String fileName = getFileName(fileDataId, fileDto);

        log.info("FileData has been successfully got, fileDataId='{}', bucketId='{}'", fileDataId, bucketName);

        return new FileData(fileDto.getFileDataId(), fileName, fileDto.getCreatedAt(), fileDto.getMetadata());
    }

    @PreDestroy
    public void terminate() {
        transferManager.shutdownNow(true);
    }

    private void bucketInit() {
        try {
            if (!s3Client.doesBucketExistV2(bucketName)) {
                s3Client.createBucket(bucketName);
            }
        } catch (SdkBaseException ex) {
            throw new StorageException(
                    format("Failed to create bucket, bucketName=%s", bucketName),
                    ex
            );
        }
    }

    private void uploadEmptyFileWithMetadata(String fileDataId, FileDto fileDto) {
        try {
            PutObjectRequest putObjectRequest = putObjectRequest(fileDataId, fileDto, byteArrayInputStream());

            Upload upload = transferManager.upload(putObjectRequest);

            upload.waitForUploadResult();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new WaitingUploadException(
                    format(
                            "Thread is interrupted while waiting for the file upload to complete, " +
                                    "fileDataId=%s, bucketId=%s",
                            fileDataId, bucketName
                    )
            );
        } catch (SdkBaseException ex) {
            throw new StorageException(
                    format("Failed to upload file, fileDataId=%s, bucketId=%s", fileDataId, bucketName),
                    ex
            );
        }
    }

    private URL generatePresignedUrl(String fileDataId, String fileId, Instant expirationTime, HttpMethod httpMethod) {
        try {
            GeneratePresignedUrlRequest request = generatePresignedUrlRequest(fileId, expirationTime, httpMethod);

            URL url = s3Client.generatePresignedUrl(request);

            checkNotNull("PresignedUrl", fileDataId, url);

            return url;
        } catch (SdkBaseException ex) {
            throw new StorageException(
                    format("Failed to generate PresignedUrl, fileDataId=%s, bucketId=%s", fileDataId, bucketName),
                    ex
            );
        }
    }

    private FileDto getFileDto(String fileDataId) {
        S3Object s3Object = getS3Object(fileDataId, fileDataId);

        checkRealFileStatus(fileDataId, s3Object);

        return getFileDto(fileDataId, s3Object.getObjectMetadata());
    }

    private FileDto getFileDto(String fileDataId, ObjectMetadata objectMetadata) {
        String id = getUserMetadataParameter(fileDataId, objectMetadata, FILE_DATA_ID);
        String fileId = getFileIdFromObjectMetadata(fileDataId, objectMetadata);
        String createdAt = getUserMetadataParameter(fileDataId, objectMetadata, CREATED_AT);
        Map<String, Value> metadata = objectMetadata.getUserMetadata().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(METADATA) && entry.getValue() != null)
                .collect(
                        Collectors.toMap(
                                o -> o.getKey().substring(METADATA.length()),
                                o -> DamselUtil.fromJson(o.getValue(), Value.class)
                        )
                );
        return new FileDto(id, fileId, createdAt, metadata);
    }

    private String getFileName(String fileDataId, FileDto fileDto) {
        S3Object s3Object = getS3Object(fileDataId, fileDto.getFileId());

        return extractFileName(s3Object);
    }

    private S3Object getS3Object(String fileDataId, String id) {
        try (S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, id))) {

            checkNotNull("S3Object", fileDataId, object);

            return object;
        } catch (SdkBaseException ex) {
            throw new StorageException(
                    format("Failed to get S3Object, fileDataId=%s, bucketId=%s", fileDataId, bucketName),
                    ex
            );
        } catch (IOException ex) {
            throw new StorageException(
                    format("Unable to close S3Object, fileDataId=%s, bucketId=%s", fileDataId, bucketName),
                    ex);
        }
    }

    private void checkRealFileStatus(String fileDataId, S3Object s3Object) {
        try {
            ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

            String fileId = getFileIdFromObjectMetadata(fileDataId, objectMetadata);
            if (s3Client.doesObjectExist(bucketName, fileId)) {
                return;
            }
        } catch (SdkBaseException ex) {
            throw new StorageException(
                    format("Failed to check on exist the file, fileDataId=%s, bucketId=%s", fileDataId, bucketName),
                    ex
            );
        }

        // если файл не соотвествует условиям, блокируем доступ к нему
        throw new FileNotFoundException(format("S3Object is null, fileDataId=%s, bucketId=%s", fileDataId, bucketName));
    }

    private String getFileIdFromObjectMetadata(String fileDataId, ObjectMetadata objectMetadata) {
        return getUserMetadataParameter(fileDataId, objectMetadata, FILE_ID);
    }

    private String getUserMetadataParameter(String fileDataId, ObjectMetadata objectMetadata, String key) {
        return Optional.ofNullable(objectMetadata.getUserMetaDataOf(key))
                .orElseThrow(
                        () -> new ExtractMetadataException(
                                format(
                                        "Failed to extract metadata parameter, fileDataId=%s, bucketId=%s, key=%s",
                                        fileDataId, bucketName, key
                                )
                        )
                );
    }

    private String extractFileName(S3Object s3Object) {
        String contentDisposition = s3Object.getObjectMetadata().getContentDisposition();
        int fileNameIndex = contentDisposition.lastIndexOf(FILENAME_PARAM) + FILENAME_PARAM.length();
        return contentDisposition.substring(fileNameIndex);
    }

    private void checkNotNull(String objectType, String fileDataId, Object object) {
        if (Objects.isNull(object)) {
            throw new FileNotFoundException(
                    format("%s is null, fileDataId=%s, bucketId=%s", objectType, fileDataId, bucketName));
        }
    }

    private String id() {
        return UUID.randomUUID().toString();
    }

    private ByteArrayInputStream byteArrayInputStream() {
        return new ByteArrayInputStream(new byte[0]);
    }

    private FileDto fileDto(String fileDataId, String fileId, Map<String, Value> metadata) {
        return new FileDto(fileDataId, fileId, Instant.now().toString(), metadata);
    }

    private PutObjectRequest putObjectRequest(String fileDataId, FileDto fileDto, InputStream inputStream) {
        return new PutObjectRequest(
                bucketName,
                fileDataId,
                inputStream,
                objectMetadata(fileDto)
        );
    }

    private ObjectMetadata objectMetadata(FileDto fileDto) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.addUserMetadata(FILE_DATA_ID, fileDto.getFileDataId());
        objectMetadata.addUserMetadata(FILE_ID, fileDto.getFileId());
        objectMetadata.addUserMetadata(CREATED_AT, fileDto.getCreatedAt());
        fileDto.getMetadata().forEach(
                (key, value) -> objectMetadata.addUserMetadata(METADATA + key, DamselUtil.toJsonString(value))
        );
        return objectMetadata;
    }

    private GeneratePresignedUrlRequest generatePresignedUrlRequest(
            String fileId,
            Instant expirationTime,
            HttpMethod httpMethod) {
        return new GeneratePresignedUrlRequest(bucketName, fileId)
                .withMethod(httpMethod)
                .withExpiration(Date.from(expirationTime));
    }

    @RequiredArgsConstructor
    @Getter
    @ToString
    private class FileDto {

        private final String fileDataId;
        private final String fileId;
        private final String createdAt;
        private final Map<String, Value> metadata;

    }
}
