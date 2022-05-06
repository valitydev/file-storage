package dev.vality.file.storage.s3signer;

import dev.vality.file.storage.FileStorageTest;
import dev.vality.testcontainers.annotations.minio.MinioTestcontainerSingleton;

@MinioTestcontainerSingleton(bucketName = "s3signer")
public class WithMinio extends FileStorageTest {
}
