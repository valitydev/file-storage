package dev.vality.file.storage;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.TransferManager;
import dev.vality.woody.api.flow.error.WRuntimeException;
import dev.vality.woody.thrift.impl.http.THSpawnClientBuilder;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.web.server.LocalServerPort;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;

@Disabled
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class CatchExceptionsTest {

    @LocalServerPort
    protected int port;

    @MockBean
    private TransferManager transferManager;

    @MockBean
    private AmazonS3 s3Client;

    @Test
    public void shouldThrowException() throws URISyntaxException, TException {
        FileStorageSrv.Iface fileStorageCli = new THSpawnClientBuilder()
                .withAddress(new URI("http://localhost:" + port + "/file_storage/v2"))
                .withNetworkTimeout(555000)
                .build(FileStorageSrv.Iface.class);

        Mockito.when(s3Client.getObject(Mockito.any())).thenThrow(SdkClientException.class);

        assertThrows(
                WRuntimeException.class,
                () -> fileStorageCli.getFileData(UUID.randomUUID().toString()));
    }
}
