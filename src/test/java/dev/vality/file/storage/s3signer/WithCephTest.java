package dev.vality.file.storage.s3signer;

import dev.vality.file.storage.FileStorageTest;
import dev.vality.testcontainers.annotations.minio.MinioTestcontainerSingleton;
import org.junit.jupiter.api.Disabled;

@Disabled
@MinioTestcontainerSingleton(bucketName = "s3signer")
public class WithCephTest extends FileStorageTest {
}
