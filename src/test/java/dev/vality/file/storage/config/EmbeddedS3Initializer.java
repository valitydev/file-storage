package dev.vality.file.storage.config;

import com.robothy.s3.rest.LocalS3;
import org.springframework.beans.factory.support.DefaultSingletonBeanRegistry;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

public class EmbeddedS3Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

    @Override
    public void initialize(ConfigurableApplicationContext context) {
        LocalS3 localS3 = LocalS3.builder()
                .port(-1)
                .build();
        localS3.start();
        LocalS3Proxy localS3Proxy = new LocalS3Proxy(localS3.getPort());
        localS3Proxy.start();

        DefaultSingletonBeanRegistry beanRegistry = (DefaultSingletonBeanRegistry) context.getBeanFactory();
        beanRegistry.registerSingleton("embeddedS3", localS3);
        beanRegistry.registerDisposableBean("embeddedS3", localS3::shutdown);
        beanRegistry.registerSingleton("embeddedS3Proxy", localS3Proxy);
        beanRegistry.registerDisposableBean("embeddedS3Proxy", localS3Proxy::close);

        TestPropertyValues.of(
                        "s3-sdk-v2.endpoint=http://localhost:" + localS3Proxy.getPort(),
                        "s3-sdk-v2.bucket-name=awssdks3v2")
                .applyTo(context);
    }
}
