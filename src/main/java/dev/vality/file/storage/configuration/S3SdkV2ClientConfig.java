package dev.vality.file.storage.configuration;

import dev.vality.file.storage.configuration.properties.S3SdkV2Properties;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

import java.net.URI;

@Configuration
@RequiredArgsConstructor
@EnableConfigurationProperties(S3SdkV2Properties.class)
public class S3SdkV2ClientConfig {

    private final S3SdkV2Properties s3SdkV2Properties;

    @Bean(destroyMethod = "close")
    public S3Presigner s3Presigner() {
        return S3Presigner.builder()
                .region(Region.of(s3SdkV2Properties.getRegion()))
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(
                                        s3SdkV2Properties.getAccessKey(),
                                        s3SdkV2Properties.getSecretKey())))
                .endpointOverride(URI.create(s3SdkV2Properties.getEndpoint()))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .checksumValidationEnabled(false)
                        .build())
                .build();
    }

    @Bean(destroyMethod = "close")
    public S3Client s3SdkV2Client() {
        return S3Client.builder()
                .region(Region.of(s3SdkV2Properties.getRegion()))
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(
                                        s3SdkV2Properties.getAccessKey(),
                                        s3SdkV2Properties.getSecretKey())))
                .endpointOverride(URI.create(s3SdkV2Properties.getEndpoint()))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .build();
    }
}
