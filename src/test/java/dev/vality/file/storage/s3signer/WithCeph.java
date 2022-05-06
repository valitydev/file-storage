package dev.vality.file.storage.s3signer;

import dev.vality.file.storage.FileStorageTest;
import dev.vality.testcontainers.annotations.ceph.CephTestcontainerSingleton;

@CephTestcontainerSingleton(bucketName = "s3signer")
public class WithCeph extends FileStorageTest {
}
