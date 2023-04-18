package dev.vality.file.storage.awssdks3v2;

import dev.vality.file.storage.FileStorageTest;
import dev.vality.testcontainers.annotations.ceph.CephTestcontainerSingleton;
import org.junit.jupiter.api.Disabled;

@Disabled
@CephTestcontainerSingleton(
        properties = {"s3-sdk-v2.enabled=true", "s3-sdk-v2.region=us-east-1"},
        bucketName = "awssdks3v2")
public class WithCephTest extends FileStorageTest {
}
