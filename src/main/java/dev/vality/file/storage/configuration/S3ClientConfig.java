package dev.vality.file.storage.configuration;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import dev.vality.file.storage.configuration.properties.S3Properties;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
@EnableConfigurationProperties(S3Properties.class)
public class S3ClientConfig {

    private final S3Properties s3Properties;

    @Bean
    public TransferManager transferManager(AmazonS3 s3Client) {
        return TransferManagerBuilder.standard()
                .withS3Client(s3Client)
                .build();
    }

    @Bean
    public AmazonS3 s3Client() {
        return AmazonS3ClientBuilder.standard()
                .withCredentials(
                        new AWSCredentialsProviderChain(
                                new EnvironmentVariableCredentialsProvider(),
                                new AWSStaticCredentialsProvider(
                                        new BasicAWSCredentials(
                                                s3Properties.getAccessKey(),
                                                s3Properties.getSecretKey()))))
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(
                                s3Properties.getEndpoint(),
                                s3Properties.getSigningRegion()))
                .withClientConfiguration(
                        new ClientConfiguration()
                                .withProtocol(s3Properties.getClientProtocol())
                                .withSignerOverride(s3Properties.getSignerOverride())
                                .withMaxErrorRetry(s3Properties.getClientMaxErrorRetry()))
                .build();
    }
}
