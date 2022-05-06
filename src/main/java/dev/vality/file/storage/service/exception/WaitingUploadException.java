package dev.vality.file.storage.service.exception;

public class WaitingUploadException extends StorageException {

    public WaitingUploadException(String message) {
        super(message);
    }

    public WaitingUploadException(String message, Throwable cause) {
        super(message, cause);
    }
}
