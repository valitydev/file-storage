package dev.vality.file.storage.handler;

import dev.vality.file.storage.*;
import dev.vality.file.storage.service.S3PresignedMultipartStorageService;
import dev.vality.msgpack.Value;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.NoSuchElementException;

@Service
@Slf4j
@RequiredArgsConstructor
public class FileStoragePresignedMultipartHandler implements FileStoragePresignedMultipartSrv.Iface {

    private final S3PresignedMultipartStorageService presignedMultipartStorageService;

    @Override
    public PresignedMultipartUpload createMultipartUpload(Map<String, Value> metadata) {
        log.info("Receive request for create presigned multipart upload with metadata={}", metadata);
        PresignedMultipartUpload result = presignedMultipartStorageService.createMultipartUpload(metadata);
        log.info("Successfully create presigned multipart upload, fileId={}, uploadId={}",
                result.getFileDataId(), result.getMultipartUploadId());
        return result;
    }

    @Override
    public PresignedMultipartUpload getMultipartUpload(String fileDataId) throws FileNotFound {
        log.debug("Receive request for get presigned multipart upload, fileId={}", fileDataId);
        try {
            PresignedMultipartUpload result = presignedMultipartStorageService.getMultipartUpload(fileDataId);
            log.debug("Successfully got presigned multipart upload, fileId={}, uploadId={}, uploadStatus={}",
                    result.getFileDataId(), result.getMultipartUploadId(), result.getUploadStatus());
            return result;
        } catch (NoSuchElementException ex) {
            throw new FileNotFound();
        }
    }

    @Override
    public PresignMultipartUploadPartResult presignMultipartUploadPart(PresignMultipartUploadPartRequest request) {
        log.debug("Receive request for presign file part upload, fileId={}, uploadId={}, sequencePart={}",
                request.getFileDataId(), request.getMultipartUploadId(), request.getSequencePart());
        PresignMultipartUploadPartResult result = presignedMultipartStorageService.presignMultipartUploadPart(request);
        log.debug("Successfully presigned file part upload, fileId={}, uploadId={}, sequencePart={}",
                request.getFileDataId(), request.getMultipartUploadId(), result.getSequencePart());
        return result;
    }

    @Override
    public CompletePresignedMultipartUploadResult completeMultipartUpload(
            CompletePresignedMultipartUploadRequest request) {
        log.info("Receive request for complete presigned multipart upload, fileId={}, uploadId={}",
                request.getFileDataId(), request.getMultipartUploadId());
        CompletePresignedMultipartUploadResult result =
                presignedMultipartStorageService.completeMultipartUpload(request);
        log.info("Successfully complete presigned multipart upload, fileId={}, fileUrl={}, uploadStatus={}",
                request.getFileDataId(), result.getFileUrl(), result.getUploadStatus());
        return result;
    }

    @Override
    public void abortMultipartUpload(AbortMultipartUploadRequest request) {
        log.info("Receive request for abort presigned multipart upload, fileId={}, uploadId={}",
                request.getFileDataId(), request.getMultipartUploadId());
        presignedMultipartStorageService.abortMultipartUpload(request);
        log.info("Successfully aborted presigned multipart upload, fileId={}, uploadId={}",
                request.getFileDataId(), request.getMultipartUploadId());
    }
}
