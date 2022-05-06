package dev.vality.file.storage.awssdks3v2;

import dev.vality.file.storage.FileStorageTest;
import dev.vality.testcontainers.annotations.minio.MinioTestcontainerSingleton;

@MinioTestcontainerSingleton(
        properties = "s3-sdk-v2.enabled=true",
        bucketName = "awssdks3v2")
public class WithMinioTest extends FileStorageTest {
}
