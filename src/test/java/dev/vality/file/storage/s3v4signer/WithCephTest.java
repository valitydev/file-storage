package dev.vality.file.storage.s3v4signer;

import dev.vality.file.storage.FileStorageTest;
import dev.vality.testcontainers.annotations.minio.MinioTestcontainerSingleton;
import org.junit.jupiter.api.Disabled;

@Disabled
@MinioTestcontainerSingleton(
        properties = "s3.signer-override=AWSS3V4SignerType",
        bucketName = "s3v4signer")
public class WithCephTest extends FileStorageTest {
}
