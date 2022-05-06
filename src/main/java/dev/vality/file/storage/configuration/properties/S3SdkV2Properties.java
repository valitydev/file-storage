package dev.vality.file.storage.configuration.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties("s3-sdk-v2")
public class S3SdkV2Properties {

    private String endpoint;
    private String bucketName;
    private String region;
    private String accessKey;
    private String secretKey;

}
