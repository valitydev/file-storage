package dev.vality.file.storage.configuration.properties;

import com.amazonaws.Protocol;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties("s3")
public class S3Properties {

    private String endpoint;
    private String bucketName;
    private String signingRegion;
    private Protocol clientProtocol;
    private Integer clientMaxErrorRetry;
    private String signerOverride;
    private String accessKey;
    private String secretKey;

}
