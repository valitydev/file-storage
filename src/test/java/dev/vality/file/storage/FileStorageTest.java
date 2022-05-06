package dev.vality.file.storage;

import dev.vality.woody.thrift.impl.http.THSpawnClientBuilder;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.thrift.TException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static dev.vality.file.storage.msgpack.Value.*;
import static dev.vality.testcontainers.annotations.util.ValuesGenerator.*;
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
                .withAddress(new URI("http://localhost:" + port + "/file_storage"))
                .withNetworkTimeout(TIMEOUT)
                .build(FileStorageSrv.Iface.class);
    }

    @Test
    public void fileUploadWithHttpClientBuilderTest() throws IOException, URISyntaxException, TException {
        String expirationTime = generateCurrentTimePlusDay().toString();
        Map<String, dev.vality.file.storage.msgpack.Value> metadata = new HashMap<>();
        metadata.put("author", dev.vality.file.storage.msgpack.Value.str("Mary Doe"));
        metadata.put("version", dev.vality.file.storage.msgpack.Value.str("1.0.0.0"));

        NewFileResult fileResult = fileStorageClient.createNewFile(metadata, expirationTime);

        Path path = getFileFromResources();

        HttpPut requestPut = new HttpPut(fileResult.getUploadUrl());
        requestPut.setHeader(
                "Content-Disposition",
                "attachment;filename=" + URLEncoder.encode(FILE_NAME, StandardCharsets.UTF_8.name()));
        requestPut.setEntity(new FileEntity(path.toFile()));

        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpResponse response = httpClient.execute(requestPut);
        assertEquals(response.getStatusLine().getStatusCode(), org.apache.http.HttpStatus.SC_OK);

        // генерация url с доступом только для загрузки
        String downloadUrl = fileStorageClient.generateDownloadUrl(fileResult.getFileDataId(), expirationTime);

        HttpResponse responseGet = httpClient.execute(new HttpGet(downloadUrl));
        InputStream content = responseGet.getEntity().getContent();
        assertEquals(getContent(Files.newInputStream(path)), getContent(content));
    }

    @Test
    public void uploadAndDownloadFileFromStorageTest() throws IOException, TException {
        Path testFile = Files.createTempFile("", "test_file");

        Path testActualFile = Files.createTempFile("", "test_actual_file");

        try {
            // создание нового файла
            String expirationTime = generateCurrentTimePlusDay().toString();
            Map<String, dev.vality.file.storage.msgpack.Value> metadata = new HashMap<>();
            metadata.put("author", dev.vality.file.storage.msgpack.Value.str("Mary Doe"));
            metadata.put("version", dev.vality.file.storage.msgpack.Value.str("1.0.0.0"));
            NewFileResult fileResult = fileStorageClient.createNewFile(metadata, expirationTime);
            uploadTestData(fileResult, FILE_NAME, FILE_DATA);

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

        uploadTestData(fileResult, FILE_NAME, FILE_DATA);
    }

    @Test
    public void downloadUrlConnectionAccessTest() throws IOException, TException {
        // создание файла с доступом к файлу на день
        String expirationTime = generateCurrentTimePlusDay().toString();
        NewFileResult fileResult = fileStorageClient.createNewFile(Collections.emptyMap(), expirationTime);

        String fileDataId = fileResult.getFileDataId();

        // upload тестовых данных в хранилище
        uploadTestData(fileResult, FILE_NAME, FILE_DATA);

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
        uploadTestData(validFileResult, FILE_NAME, FILE_DATA);

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
        assertThrows(AssertionError.class, () -> uploadTestData(throwingFileResult, FILE_NAME, FILE_DATA));

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
        uploadTestData(fileResult, FILE_NAME, FILE_DATA);

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
        uploadTestData(fileResult, fileName, FILE_DATA);

        FileData fileData = fileStorageClient.getFileData(fileResult.getFileDataId());

        // тут используется энкодер\декодер, потому что apache http клиент менять кодировку.
        // при аплоаде напрямую по uploadUrl в ceph такой проблемы нет
        assertEquals(fileName, URLDecoder.decode(fileData.getFileName(), StandardCharsets.UTF_8.name()));
    }

    @Test
    public void s3ConnectionPoolTest() throws Exception {
        String expirationTime = generateCurrentTimePlusDay().toString();
        HttpClient httpClient = HttpClientBuilder.create().build();

        NewFileResult fileResult = fileStorageClient.createNewFile(Collections.emptyMap(), expirationTime);

        Path path = getFileFromResources();

        HttpPut requestPut = new HttpPut(fileResult.getUploadUrl());
        requestPut.setHeader(
                "Content-Disposition",
                "attachment;filename=" + URLEncoder.encode(FILE_NAME, StandardCharsets.UTF_8.name()));
        requestPut.setEntity(new FileEntity(path.toFile()));

        HttpResponse response = httpClient.execute(requestPut);
        assertEquals(response.getStatusLine().getStatusCode(), org.apache.http.HttpStatus.SC_OK);

        // генерация url с доступом только для загрузки
        String downloadUrl = fileStorageClient.generateDownloadUrl(fileResult.getFileDataId(), expirationTime);

        HttpResponse responseGet = httpClient.execute(new HttpGet(downloadUrl));
        InputStream content = responseGet.getEntity().getContent();
        assertEquals(getContent(Files.newInputStream(path)), getContent(content));

        CountDownLatch countDownLatch = new CountDownLatch(1000);
        ExecutorService executor = Executors.newFixedThreadPool(5);
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

    private void uploadTestData(NewFileResult fileResult, String fileName, String testData) throws IOException {
        // запись данных методом put
        URL uploadUrl = new URL(fileResult.getUploadUrl());

        HttpURLConnection uploadUrlConnection = getHttpURLConnection(uploadUrl, "PUT", fileName, true);

        OutputStreamWriter out = new OutputStreamWriter(uploadUrlConnection.getOutputStream());
        out.write(testData);
        out.close();

        // чтобы завершить загрузку вызываем getResponseCode
        assertEquals(HttpStatus.OK.value(), uploadUrlConnection.getResponseCode());
    }

    private Path getFileFromResources() throws URISyntaxException {
        ClassLoader classLoader = this.getClass().getClassLoader();

        URL url = Objects.requireNonNull(classLoader.getResource("respect"));
        return Paths.get(url.toURI());
    }

    public static HttpURLConnection getHttpURLConnection(URL url, String method, boolean doOutput) throws IOException {
        return getHttpURLConnection(url, method, null, doOutput);
    }

    public static HttpURLConnection getHttpURLConnection(URL url, String method, String fileName, boolean doOutput)
            throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(doOutput);
        connection.setRequestMethod(method);
        if (fileName != null) {
            connection.setRequestProperty(
                    "Content-Disposition",
                    "attachment;filename=" + URLEncoder.encode(fileName, StandardCharsets.UTF_8.name()));
        }
        return connection;
    }
}
