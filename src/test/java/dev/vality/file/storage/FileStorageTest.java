package dev.vality.file.storage;

import dev.vality.woody.api.flow.error.WRuntimeException;
import dev.vality.woody.thrift.impl.http.THSpawnClientBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.AbstractResponseHandler;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.thrift.TException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static dev.vality.msgpack.Value.*;
import static dev.vality.testcontainers.annotations.util.ValuesGenerator.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource("classpath:application.yml")
@DirtiesContext
public abstract class FileStorageTest {

    private static final int TIMEOUT = 555000;
    private static final String FILE_DATA = "test";
    private static final String FILE_NAME = "asd123.asd";

    protected FileStorageSrv.Iface fileStorageClient;

    @Value("${local.server.port}")
    private int port;

    @BeforeEach
    public void setUp() throws Exception {
        fileStorageClient = new THSpawnClientBuilder()
                .withAddress(new URI("http://localhost:" + port + "/file_storage/v2"))
                .withNetworkTimeout(TIMEOUT)
                .build(FileStorageSrv.Iface.class);
    }

    @Test
    public void fileUploadWithHttpClientBuilderTest() throws IOException, URISyntaxException, TException {
        var expirationTime = generateCurrentTimePlusDay().toString();
        var metadata = new HashMap<String, dev.vality.msgpack.Value>();
        metadata.put("author", dev.vality.msgpack.Value.str("Mary Doe"));
        metadata.put("version", dev.vality.msgpack.Value.str("1.0.0.0"));
        var fileResult = fileStorageClient.createNewFile(metadata, expirationTime);
        var path = getFileFromResources("respect");
        uploadTestData(fileResult, FILE_NAME, path);
        // генерация url с доступом только для загрузки
        var downloadUrl = fileStorageClient.generateDownloadUrl(fileResult.getFileDataId(), expirationTime);
        downloadTestData(downloadUrl, path);
    }

    @Test
    public void uploadAndDownloadFileFromStorageTest() throws IOException, TException {
        Path testFile = Files.createTempFile("", "test_file");

        Path testActualFile = Files.createTempFile("", "test_actual_file");

        try {
            // создание нового файла
            String expirationTime = generateCurrentTimePlusDay().toString();
            Map<String, dev.vality.msgpack.Value> metadata = new HashMap<>();
            metadata.put("author", dev.vality.msgpack.Value.str("Mary Doe"));
            metadata.put("version", dev.vality.msgpack.Value.str("1.0.0.0"));
            NewFileResult fileResult = fileStorageClient.createNewFile(metadata, expirationTime);
            uploadTestData(fileResult, FILE_NAME);

            // генерация url с доступом только для загрузки
            URL downloadUrl = new URL(
                    fileStorageClient.generateDownloadUrl(fileResult.getFileDataId(), expirationTime));

            HttpURLConnection downloadUrlConnection = getHttpURLConnection(downloadUrl, "GET", false);
            InputStream inputStream = downloadUrlConnection.getInputStream();

            // чтение записанного файла из хранилища
            Files.copy(inputStream, testActualFile, StandardCopyOption.REPLACE_EXISTING);

            // testFile пустой
            assertEquals(Files.readAllLines(testFile), Collections.emptyList());

            // запись тестовых данных в пустой testFile
            Files.write(testFile, FILE_DATA.getBytes());
            assertEquals(Files.readAllLines(testFile), Files.readAllLines(testActualFile));
        } finally {
            Files.deleteIfExists(testFile);
            Files.deleteIfExists(testActualFile);
        }
    }

    @Test
    public void accessErrorToMethodsTest() throws IOException, TException {
        // создание файла с доступом к файлу на день
        String expirationTime = generateCurrentTimePlusDay().toString();
        NewFileResult fileResult = fileStorageClient.createNewFile(Collections.emptyMap(), expirationTime);

        String fileDataId = fileResult.getFileDataId();

        // ошибка доступа - файла не существует, тк не было upload
        assertThrows(FileNotFound.class, () -> fileStorageClient.generateDownloadUrl(fileDataId, expirationTime));
        assertThrows(FileNotFound.class, () -> fileStorageClient.getFileData(fileDataId));
    }

    @Test
    public void uploadUrlConnectionAccessTest() throws IOException, TException {
        // создание файла с доступом к файлу на день
        String expirationTime = generateCurrentTimePlusDay().toString();
        NewFileResult fileResult = fileStorageClient.createNewFile(Collections.emptyMap(), expirationTime);

        URL uploadUrl = new URL(fileResult.getUploadUrl());

        // ошибка при запросе по url методом get
        assertEquals(
                HttpStatus.FORBIDDEN.value(),
                getHttpURLConnection(uploadUrl, "GET", false).getResponseCode());

        uploadTestData(fileResult, FILE_NAME);
    }

    @Test
    public void downloadUrlConnectionAccessTest() throws IOException, TException {
        // создание файла с доступом к файлу на день
        String expirationTime = generateCurrentTimePlusDay().toString();
        NewFileResult fileResult = fileStorageClient.createNewFile(Collections.emptyMap(), expirationTime);

        String fileDataId = fileResult.getFileDataId();

        // upload тестовых данных в хранилище
        uploadTestData(fileResult, FILE_NAME);

        // генерация url с доступом только для загрузки
        URL url = new URL(fileStorageClient.generateDownloadUrl(fileDataId, expirationTime));

        // с данной ссылкой нельзя записывать
        assertEquals(HttpStatus.FORBIDDEN.value(), getHttpURLConnection(url, "PUT", FILE_NAME, true).getResponseCode());

        // можно читать
        assertEquals(HttpStatus.OK.value(), getHttpURLConnection(url, "GET", false).getResponseCode());
    }

    @Test
    public void expirationTimeTest() throws TException, InterruptedException, IOException {
        // создание файла с доступом к файлу на день
        String expirationTime = generateCurrentTimePlusDay().toString();
        NewFileResult validFileResult = fileStorageClient.createNewFile(Collections.emptyMap(), expirationTime);

        String validFileDataId = validFileResult.getFileDataId();

        // задержка перед upload для теста expiration
        Thread.sleep(1000);

        // сохранение тестовых данных в хранилище
        uploadTestData(validFileResult, FILE_NAME);

        // доступ есть
        fileStorageClient.getFileData(validFileDataId);
        fileStorageClient.generateDownloadUrl(validFileDataId, generateCurrentTimePlusDay().toString());

        // - - - - - сделаем задержку больше expiration
        // создание файла с доступом к файлу на секунду
        NewFileResult throwingFileResult = fileStorageClient.createNewFile(
                Collections.emptyMap(),
                generateCurrentTimePlusSecond().toString());

        String throwingFileDataId = throwingFileResult.getFileDataId();

        // ошибка доступа - файла не существует, тк не было upload
        assertThrows(
                FileNotFound.class,
                () -> fileStorageClient.generateDownloadUrl(throwingFileDataId, expirationTime));
        assertThrows(FileNotFound.class, () -> fileStorageClient.getFileData(throwingFileDataId));

        // задержка перед upload для теста expiration
        Thread.sleep(2000);

        // сохранение тестовых данных в хранилище вызывает ошибку доступа
        assertThrows(HttpResponseException.class, () -> uploadTestData(throwingFileResult, FILE_NAME));

        // ошибка доступа
        assertThrows(FileNotFound.class, () -> fileStorageClient.getFileData(throwingFileDataId));
        assertThrows(
                FileNotFound.class,
                () -> fileStorageClient.generateDownloadUrl(throwingFileDataId, expirationTime));
    }

    @Test
    public void extractMetadataTest() throws TException, IOException {
        String expirationTime = generateCurrentTimePlusDay().toString();
        var metadata = Map.of(
                "key1", b(true),
                "key2", i(1),
                "key3", flt(1),
                "key4", arr(new ArrayList<>()),
                "key5", str(FILE_DATA),
                "key6", bin(new byte[]{}));

        NewFileResult fileResult = fileStorageClient.createNewFile(metadata, expirationTime);
        uploadTestData(fileResult, FILE_NAME);

        FileData fileData = fileStorageClient.getFileData(fileResult.getFileDataId());

        assertEquals(fileData.getMetadata(), metadata);
    }

    @Test
    public void fileNameCyrillicTest() throws TException, IOException {
        // создание файла с доступом к файлу на день
        String expirationTime = generateCurrentTimePlusDay().toString();
        NewFileResult fileResult = fileStorageClient.createNewFile(Collections.emptyMap(), expirationTime);

        // upload тестовых данных в хранилище
        String fileName = "csgo-лучше-чем-1.6";
        uploadTestData(fileResult, fileName);

        FileData fileData = fileStorageClient.getFileData(fileResult.getFileDataId());

        // тут используется энкодер\декодер, потому что apache http клиент менять кодировку.
        // при аплоаде напрямую по uploadUrl в ceph такой проблемы нет
        assertEquals(fileName, URLDecoder.decode(fileData.getFileName(), StandardCharsets.UTF_8.name()));
    }

    @Test
    public void s3ConnectionPoolTest() throws Exception {
        var expirationTime = generateCurrentTimePlusDay().toString();
        var fileResult = fileStorageClient.createNewFile(Collections.emptyMap(), expirationTime);
        var path = getFileFromResources("respect");
        uploadTestData(fileResult, FILE_NAME, path);
        // генерация url с доступом только для загрузки
        var downloadUrl = fileStorageClient.generateDownloadUrl(fileResult.getFileDataId(), expirationTime);
        downloadTestData(downloadUrl, path);
        var countDownLatch = new CountDownLatch(1000);
        var executor = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 1000; i++) {
            executor.execute(
                    () -> {
                        try {
                            fileStorageClient.getFileData(fileResult.getFileDataId());
                            countDownLatch.countDown();
                        } catch (TException fileNotFound) {
                            fail();
                        }
                    }
            );
        }
        countDownLatch.await();
        assertTrue(true);
    }

    @Test
    public void multipartUploadTest() throws Exception {
        var exception = assertThrows(WRuntimeException.class,
                () -> fileStorageClient.createMultipartUpload(Collections.emptyMap()));
        assertThat(exception.getErrorDefinition().getErrorReason(),
                containsString("Can't create multipart upload object without fileName"));

        dev.vality.msgpack.Value fileName = str("fileName");
        var metadata = Map.of("filename", fileName);
        CreateMultipartUploadResult createResult = fileStorageClient.createMultipartUpload(metadata);
        assertNotNull(createResult.getFileDataId());
        assertNotNull(createResult.getMultipartUploadId());

        List<CompletedMultipart> completedParts = new ArrayList<>();
        processMultipartUpload(createResult, completedParts);

        var completeRequest = new CompleteMultipartUploadRequest()
                .setMultipartUploadId(createResult.getMultipartUploadId())
                .setFileDataId(createResult.getFileDataId())
                .setCompletedParts(completedParts);

        CompleteMultipartUploadResult result = fileStorageClient.completeMultipartUpload(completeRequest);

        assertNotNull(result);
        assertNotNull(result.getUploadUrl());
    }

    @Test
    public void getMultipartFileData() throws Exception {
        dev.vality.msgpack.Value value = new dev.vality.msgpack.Value();
        String fileName = "test_registry.csv";
        value.setStr(fileName);
        Map<String, dev.vality.msgpack.Value> metadata = Map.of("filename", value);
        CreateMultipartUploadResult createResult = fileStorageClient.createMultipartUpload(metadata);
        assertNotNull(createResult.getFileDataId());
        assertNotNull(createResult.getMultipartUploadId());

        List<CompletedMultipart> completedParts = new ArrayList<>();
        processMultipartUpload(createResult, completedParts);

        var completeRequest = new CompleteMultipartUploadRequest()
                .setMultipartUploadId(createResult.getMultipartUploadId())
                .setFileDataId(createResult.getFileDataId())
                .setCompletedParts(completedParts);

        fileStorageClient.completeMultipartUpload(completeRequest);

        FileData multipartFileData = fileStorageClient.getFileData(createResult.getFileDataId());

        assertEquals(createResult.getFileDataId(), multipartFileData.getFileDataId());
        assertEquals(fileName, multipartFileData.getFileName());
        assertNotNull(multipartFileData.getCreatedAt());
        assertNotNull(multipartFileData.getMetadata());
    }

    @Test
    public void generateMultipartDownloadUrl() throws Exception {
        dev.vality.msgpack.Value value = new dev.vality.msgpack.Value();
        String fileName = "test_registry.csv";
        value.setStr(fileName);
        Map<String, dev.vality.msgpack.Value> metadata = Map.of("filename", value);
        CreateMultipartUploadResult createResult = fileStorageClient.createMultipartUpload(metadata);
        assertNotNull(createResult.getFileDataId());
        assertNotNull(createResult.getMultipartUploadId());

        List<CompletedMultipart> completedParts = new ArrayList<>();
        processMultipartUpload(createResult, completedParts);

        var completeRequest = new CompleteMultipartUploadRequest()
                .setMultipartUploadId(createResult.getMultipartUploadId())
                .setFileDataId(createResult.getFileDataId())
                .setCompletedParts(completedParts);

        fileStorageClient.completeMultipartUpload(completeRequest);

        String expiredTime = Instant.now().toString();
        String url = fileStorageClient.generateDownloadUrl(createResult.getFileDataId(), expiredTime);

        assertNotNull(url);
    }

    private void uploadTestData(NewFileResult fileResult, String fileName, Path testData) throws IOException {
        uploadTestData(fileResult, fileName, new FileEntity(testData.toFile()));
    }

    private void uploadTestData(NewFileResult fileResult, String fileName) throws IOException {
        uploadTestData(fileResult, fileName, new ByteArrayEntity(FILE_DATA.getBytes(StandardCharsets.UTF_8)));
    }

    private void uploadTestData(NewFileResult fileResult, String fileName, HttpEntity testData) throws IOException {
        // запись данных методом put
        try (var client = HttpClients.createDefault()) {
            var requestPut = new HttpPut(fileResult.getUploadUrl());
            var encode = URLEncoder.encode(fileName, StandardCharsets.UTF_8);
            requestPut.setHeader("Content-Disposition", "attachment;filename=" + encode);
            requestPut.setEntity(testData);
            client.execute(requestPut, new BasicResponseHandler());
        }
    }

    private void downloadTestData(String downloadUrl, Path expected) throws IOException {
        try (var client = HttpClients.createDefault()) {
            var content = client.execute(
                    new HttpGet(downloadUrl),
                    new AbstractResponseHandler<Path>() {
                        @Override
                        public Path handleEntity(HttpEntity entity) throws IOException {
                            var inputFile = Files.createTempFile(generateId(), "");
                            try (var outstream = new FileOutputStream(inputFile.toFile())) {
                                entity.writeTo(outstream);
                                return inputFile;
                            }
                        }
                    });
            assertTrue(FileUtils.contentEquals(expected.toFile(), content.toFile()));
            assertArrayEquals(Files.readAllBytes(expected), Files.readAllBytes(content));
            content = client.execute(
                    new HttpGet(downloadUrl),
                    new AbstractResponseHandler<>() {
                        @Override
                        public Path handleEntity(HttpEntity entity) throws IOException {
                            var inputFile = Files.createTempFile(generateId(), "");
                            try (InputStream source = entity.getContent()) {
                                FileUtils.copyInputStreamToFile(source, inputFile.toFile());
                                return inputFile;
                            }
                        }
                    });
            assertTrue(FileUtils.contentEquals(expected.toFile(), content.toFile()));
            assertArrayEquals(Files.readAllBytes(expected), Files.readAllBytes(content));
            var contentByte = client.execute(
                    new HttpGet(downloadUrl),
                    new AbstractResponseHandler<byte[]>() {
                        @Override
                        public byte[] handleEntity(HttpEntity entity) throws IOException {
                            return EntityUtils.toByteArray(entity);
                        }
                    });
            assertArrayEquals(Files.readAllBytes(expected), contentByte);
        }
    }

    private Path getFileFromResources(String name) throws URISyntaxException {
        var classLoader = this.getClass().getClassLoader();
        var url = Objects.requireNonNull(classLoader.getResource(name));
        return Paths.get(url.toURI());
    }

    private static HttpURLConnection getHttpURLConnection(URL url, String method, boolean doOutput) throws IOException {
        return getHttpURLConnection(url, method, null, doOutput);
    }

    private static HttpURLConnection getHttpURLConnection(URL url, String method, String fileName, boolean doOutput)
            throws IOException {
        var connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(doOutput);
        connection.setRequestMethod(method);
        if (fileName != null) {
            var encode = URLEncoder.encode(fileName, StandardCharsets.UTF_8);
            connection.setRequestProperty("Content-Disposition", "attachment;filename=" + encode);
        }
        return connection;
    }

    private void processMultipartUpload(CreateMultipartUploadResult createResult,
                                        List<CompletedMultipart> completedParts) throws URISyntaxException {
        int partNumber = 1;
        ByteBuffer buffer = ByteBuffer.allocate(5 * 1024 * 1024);
        Path path = getFileFromResources("test_registry.csv");
        try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "r")) {
            long fileSize = file.length();
            long position = 0;
            while (position < fileSize) {
                file.seek(position);
                int bytesRead = file.getChannel().read(buffer);
                buffer.flip();
                var requestData = new UploadMultipartRequestData()
                        .setFileDataId(createResult.getFileDataId())
                        .setMultipartUploadId(createResult.getMultipartUploadId())
                        .setContent(buffer)
                        .setContentLength(bytesRead)
                        .setSequencePart(partNumber);

                UploadMultipartResult response = fileStorageClient.uploadMultipart(requestData);

                completedParts.add(new CompletedMultipart()
                        .setSequencePart(partNumber)
                        .setPartId(response.getPartId()));

                buffer.clear();
                position += bytesRead;
                partNumber++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
