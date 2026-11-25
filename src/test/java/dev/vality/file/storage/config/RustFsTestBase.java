package dev.vality.file.storage.config;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

@Testcontainers
public abstract class RustFsTestBase {

    private static final int S3_PORT = 9000;
    private static final String REGION = "us-east-1";
    private static final String ACCESS_KEY = "file-storage-test";
    private static final String SECRET_KEY = "file-storage-test-secret";

    @Container
    private static final GenericContainer<?> RUSTFS =
            new GenericContainer<>(DockerImageName.parse("rustfs/rustfs:1.0.5"))
                    .withExposedPorts(S3_PORT)
                    .withEnv("RUSTFS_ACCESS_KEY", ACCESS_KEY)
                    .withEnv("RUSTFS_SECRET_KEY", SECRET_KEY)
                    .withEnv("RUSTFS_REGION", REGION)
                    .withEnv("RUSTFS_CONSOLE_ENABLE", "false")
                    .withEnv("RUSTFS_CHECK_UPDATE", "false")
                    .waitingFor(Wait.forHttp("/health/ready")
                            .forPort(S3_PORT)
                            .forStatusCode(200)
                            .withStartupTimeout(Duration.ofMinutes(2)));

    @DynamicPropertySource
    static void s3Properties(DynamicPropertyRegistry registry) {
        registry.add("s3-sdk-v2.endpoint", () ->
                "http://%s:%d".formatted(RUSTFS.getHost(), RUSTFS.getMappedPort(S3_PORT)));
        registry.add("s3-sdk-v2.bucket-name", () -> "file-storage-test");
        registry.add("s3-sdk-v2.region", () -> REGION);
        registry.add("s3-sdk-v2.access-key", () -> ACCESS_KEY);
        registry.add("s3-sdk-v2.secret-key", () -> SECRET_KEY);
    }
}
