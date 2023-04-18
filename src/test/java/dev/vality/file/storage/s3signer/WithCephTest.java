package dev.vality.file.storage.s3signer;

import dev.vality.file.storage.FileStorageTest;
import dev.vality.testcontainers.annotations.ceph.CephTestcontainerSingleton;
import org.junit.jupiter.api.Disabled;

@Disabled
@CephTestcontainerSingleton(bucketName = "s3signer")
public class WithCephTest extends FileStorageTest {
}
