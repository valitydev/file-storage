package dev.vality.file.storage.handler;

import dev.vality.file.storage.*;
import dev.vality.file.storage.service.StorageService;
import dev.vality.file.storage.service.exception.FileNotFoundException;
import dev.vality.file.storage.util.CheckerUtil;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.msgpack.Value;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Service;

import java.net.URL;
import java.time.Instant;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class FileStorageHandler implements FileStorageSrv.Iface {

    private final StorageService storageService;

    @Override
    public NewFileResult createNewFile(Map<String, Value> metadata, String expiresAt) throws TException {
        var instant = TypeUtil.stringToInstant(expiresAt);
        return storageService.createNewFile(metadata, instant);
    }

    @Override
    public String generateDownloadUrl(String fileDataId, String expiresAt) throws TException {
        try {
            CheckerUtil.checkString(fileDataId, "Bad request parameter, fileDataId required and not empty arg");
            CheckerUtil.checkString(expiresAt, "Bad request parameter, expiresAt required and not empty arg");
            Instant instant = TypeUtil.stringToInstant(expiresAt);
            URL url = storageService.generateDownloadUrl(fileDataId, instant);
            return url.toString();
        } catch (FileNotFoundException e) {
            throw fileNotFound(e);
        }
    }

    @Override
    public FileData getFileData(String fileDataId) throws TException {
        try {
            CheckerUtil.checkString(fileDataId, "Bad request parameter, fileDataId required and not empty arg");
            return storageService.getFileData(fileDataId);
        } catch (FileNotFoundException e) {
            throw fileNotFound(e);
        }
    }

    @Override
    public CreateMultipartUploadResult createMultipartUpload(Map<String, Value> metadata) {
        log.info("Receive request for create multipart upload with metadata={}", metadata);
        CreateMultipartUploadResult result = storageService.createMultipartUpload(metadata);
        log.info("Successfully create multipart upload, fileId={}, uploadId={}",
                result.getFileDataId(), result.getMultipartUploadId());
        return result;
    }

    @Override
    public UploadMultipartResult uploadMultipart(UploadMultipartRequestData request) {
        log.debug("Receive request for upload file part, fileId={}, uploadId={}, sequencePart={}",
                request.getFileDataId(), request.getMultipartUploadId(), request.getSequencePart());
        UploadMultipartResult result = storageService.uploadMultipart(request);
        log.debug("Successfully upload file part, fileId={}, uploadId={}, partId={}",
                request.getFileDataId(), request.getMultipartUploadId(), result.getPartId());
        return result;
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request) {
        log.info("Receive request for complete multipart upload, fileId={}, uploadId={}",
                request.getFileDataId(), request.getMultipartUploadId());
        CompleteMultipartUploadResult result = storageService.completeMultipartUpload(request);
        log.info("Successfully complete multipart upload, fileId={}, url={}",
                request.getFileDataId(), result.getUploadUrl());
        return result;
    }

    private FileNotFound fileNotFound(FileNotFoundException e) {
        log.warn("File not found", e);
        return new FileNotFound();
    }
}
