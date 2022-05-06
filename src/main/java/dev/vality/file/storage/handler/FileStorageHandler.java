package dev.vality.file.storage.handler;

import dev.vality.damsel.msgpack.Value;
import dev.vality.file.storage.FileData;
import dev.vality.file.storage.FileNotFound;
import dev.vality.file.storage.FileStorageSrv;
import dev.vality.file.storage.NewFileResult;
import dev.vality.file.storage.service.StorageService;
import dev.vality.file.storage.service.exception.FileNotFoundException;
import dev.vality.file.storage.service.exception.StorageException;
import dev.vality.file.storage.util.CheckerUtil;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.woody.api.flow.error.WUnavailableResultException;
import dev.vality.woody.api.flow.error.WUndefinedResultException;
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
        try {
            Instant instant = TypeUtil.stringToInstant(expiresAt);
            return storageService.createNewFile(metadata, instant);
        } catch (StorageException e) {
            throw unavailableResultException(e);
        } catch (Exception e) {
            throw undefinedResultException("Error when \"createNewFile\"", e);
        }
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
        } catch (StorageException e) {
            throw unavailableResultException(e);
        } catch (Exception e) {
            throw undefinedResultException("Error when \"generateDownloadUrl\"", e);
        }
    }

    @Override
    public FileData getFileData(String fileDataId) throws TException {
        try {
            CheckerUtil.checkString(fileDataId, "Bad request parameter, fileDataId required and not empty arg");
            return storageService.getFileData(fileDataId);
        } catch (FileNotFoundException e) {
            throw fileNotFound(e);
        } catch (StorageException e) {
            throw unavailableResultException(e);
        } catch (Exception e) {
            throw undefinedResultException("Error when \"getFileData\"", e);
        }
    }

    private FileNotFound fileNotFound(FileNotFoundException e) {
        log.warn("File not found", e);
        return new FileNotFound();
    }

    private WUnavailableResultException unavailableResultException(StorageException e) {
        log.error("Error with storage", e);
        return new WUnavailableResultException("Error with storage", e);
    }

    private WUndefinedResultException undefinedResultException(String msg, Exception e) {
        log.error(msg, e);
        return new WUndefinedResultException(msg, e);
    }
}
