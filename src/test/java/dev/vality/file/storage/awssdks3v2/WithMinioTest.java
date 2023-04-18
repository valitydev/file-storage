package dev.vality.file.storage.awssdks3v2;

import dev.vality.file.storage.FileStorageTest;
import dev.vality.testcontainers.annotations.minio.MinioTestcontainerSingleton;

@MinioTestcontainerSingleton(bucketName = "awssdks3v2")
public class WithMinioTest extends FileStorageTest {
}
