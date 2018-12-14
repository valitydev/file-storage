package com.rbkmoney.file.storage;

import com.rbkmoney.damsel.msgpack.Value;
import org.apache.thrift.TException;
import org.junit.Test;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// все тесты в 1 классе , чтобы сэкономить время на поднятии тест контейнера
public class FileStorageTest extends AbstractIntegrationTest {

    private static final String FILE_DATA = "test";
    private static final String FILE_NAME = "rainbow-champion";

    @Test
    public void uploadAndDownloadFileFromStorageTest() throws IOException, TException {
        Path testFile = Files.createTempFile("", "test_file");

        Path testActualFile = Files.createTempFile("", "test_actual_file");

        try {
            // создание нового файла
            String expirationTime = generateCurrentTimePlusDay().toString();
            NewFileResult fileResult = client.createNewFile(Collections.emptyMap(), expirationTime);
            uploadTestData(fileResult, FILE_NAME, FILE_DATA);

            // генерация url с доступом только для загрузки
            URL downloadUrl = new URL(client.generateDownloadUrl(fileResult.getId(), expirationTime));

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
        NewFileResult fileResult = client.createNewFile(Collections.emptyMap(), expirationTime);

        String fileDataId = fileResult.getId();

        // ошибка доступа - файла не существует, тк не было upload
        assertThrows(FileNotFound.class, () -> client.generateDownloadUrl(fileDataId, expirationTime));
        assertThrows(FileNotFound.class, () -> client.getFileData(fileDataId));
    }

    @Test
    public void uploadUrlConnectionAccessTest() throws IOException, TException {
        // создание файла с доступом к файлу на день
        String expirationTime = generateCurrentTimePlusDay().toString();
        NewFileResult fileResult = client.createNewFile(Collections.emptyMap(), expirationTime);

        URL uploadUrl = new URL(fileResult.getUploadUrl());

        // ошибка при запросе по url методом get
        assertEquals(HttpStatus.FORBIDDEN.value(), getHttpURLConnection(uploadUrl, "GET", false).getResponseCode());

        // Length Required при запросе по url методом put
        assertEquals(HttpStatus.LENGTH_REQUIRED.value(), getHttpURLConnection(uploadUrl, "PUT", FILE_NAME, true).getResponseCode());

        uploadTestData(fileResult, FILE_NAME, FILE_DATA);
    }

    @Test
    public void downloadUrlConnectionAccessTest() throws IOException, TException {
        // создание файла с доступом к файлу на день
        String expirationTime = generateCurrentTimePlusDay().toString();
        NewFileResult fileResult = client.createNewFile(Collections.emptyMap(), expirationTime);

        String fileDataId = fileResult.getId();

        // upload тестовых данных в хранилище
        uploadTestData(fileResult, FILE_NAME, FILE_DATA);

        // генерация url с доступом только для загрузки
        URL url = new URL(client.generateDownloadUrl(fileDataId, expirationTime));

        // с данной ссылкой нельзя записывать
        assertEquals(HttpStatus.FORBIDDEN.value(), getHttpURLConnection(url, "PUT", FILE_NAME, true).getResponseCode());

        // можно читать
        assertEquals(HttpStatus.OK.value(), getHttpURLConnection(url, "GET", false).getResponseCode());
    }

    @Test
    public void expirationTimeTest() throws TException, InterruptedException, IOException {
        // создание файла с доступом к файлу на день
        String expirationTime = generateCurrentTimePlusDay().toString();
        NewFileResult validFileResult = client.createNewFile(Collections.emptyMap(), expirationTime);

        String validFileDataId = validFileResult.getId();

        // задержка перед upload для теста expiration
        Thread.sleep(1000);

        // сохранение тестовых данных в хранилище
        uploadTestData(validFileResult, FILE_NAME, FILE_DATA);

        // доступ есть
        client.getFileData(validFileDataId);
        client.generateDownloadUrl(validFileDataId, generateCurrentTimePlusDay().toString());

        // - - - - - сделаем задержку больше expiration
        // создание файла с доступом к файлу на секунду
        NewFileResult throwingFileResult = client.createNewFile(Collections.emptyMap(), generateCurrentTimePlusSecond().toString());

        String throwingFileDataId = throwingFileResult.getId();

        // ошибка доступа - файла не существует, тк не было upload
        assertThrows(FileNotFound.class, () -> client.generateDownloadUrl(throwingFileDataId, expirationTime));
        assertThrows(FileNotFound.class, () -> client.getFileData(throwingFileDataId));

        // задержка перед upload для теста expiration
        Thread.sleep(2000);

        // сохранение тестовых данных в хранилище вызывает ошибку доступа
        assertThrows(AssertionError.class, () -> uploadTestData(throwingFileResult, FILE_NAME, FILE_DATA));

        // ошибка доступа
        assertThrows(FileNotFound.class, () -> client.getFileData(throwingFileDataId));
        assertThrows(FileNotFound.class, () -> client.generateDownloadUrl(throwingFileDataId, expirationTime));
    }

    @Test
    public void extractMetadataTest() throws TException, IOException {
        String expirationTime = generateCurrentTimePlusDay().toString();
        Map<String, Value> metadata = new HashMap<String, Value>() {{
            put("key1", Value.b(true));
            put("key2", Value.i(1));
            put("key3", Value.flt(1));
            put("key4", Value.arr(new ArrayList<>()));
            put("key5", Value.str(FILE_DATA));
            put("key6", Value.bin(new byte[]{}));
        }};

        NewFileResult fileResult = client.createNewFile(metadata, expirationTime);
        uploadTestData(fileResult, FILE_NAME, FILE_DATA);

        FileData fileData = client.getFileData(fileResult.getId());

        assertEquals(fileData.getMetadata(), metadata);
    }

    @Test
    public void fileNameCyrillicTest() throws TException, IOException {
        // создание файла с доступом к файлу на день
        String expirationTime = generateCurrentTimePlusDay().toString();
        NewFileResult fileResult = client.createNewFile(Collections.emptyMap(), expirationTime);

        // upload тестовых данных в хранилище
        String fileName = "csgo-лучше-чем-1.6";
        uploadTestData(fileResult, fileName, FILE_DATA);

        FileData fileData = client.getFileData(fileResult.getId());

        // тут используется энкодер\декодер, потому что apache http клиент менять кодировку.
        // при аплоаде напрямую по uploadUrl в ceph такой проблемы нет
        assertEquals(fileName, URLDecoder.decode(fileData.getFileName(), StandardCharsets.UTF_8.name()));
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
}