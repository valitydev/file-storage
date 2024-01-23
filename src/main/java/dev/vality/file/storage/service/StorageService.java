package dev.vality.file.storage.service;

import dev.vality.file.storage.*;
import dev.vality.msgpack.Value;

import java.net.URL;
import java.time.Instant;
import java.util.Map;

public interface StorageService {

    NewFileResult createNewFile(Map<String, Value> metadata, Instant expirationTime);

    URL generateDownloadUrl(String fileDataId, Instant expirationTime);

    FileData getFileData(String fileDataId);

    FileData getMultipartFileData(String fileId);

    CreateMultipartUploadResult createMultipartUpload(Map<String, Value> metadata);

    UploadMultipartResult uploadMultipart(UploadMultipartRequestData requestData);

    CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request);

}
