package dev.vality.file.storage.service;

import dev.vality.damsel.msgpack.Value;
import dev.vality.file.storage.FileData;
import dev.vality.file.storage.NewFileResult;

import java.net.URL;
import java.time.Instant;
import java.util.Map;

public interface StorageService {

    NewFileResult createNewFile(Map<String, Value> metadata, Instant expirationTime);

    URL generateDownloadUrl(String fileDataId, Instant expirationTime);

    FileData getFileData(String fileDataId);

}
