package dev.vality.file.storage.awssdks3v2;

import dev.vality.file.storage.FileStorageTest;
import dev.vality.file.storage.config.EmbeddedS3Initializer;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(initializers = EmbeddedS3Initializer.class)
public class WithLocalS3Test extends FileStorageTest {
}
