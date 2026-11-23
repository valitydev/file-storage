package dev.vality.file.storage.config;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import lombok.Getter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Добавляет переопределение заголовков ответа S3 и проверки presigned URL,
 * которые не поддерживаются в LocalS3.
 */
public class LocalS3Proxy implements AutoCloseable {

    private static final String CONTENT_DISPOSITION = "Content-Disposition";
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String VERSION_ID = "x-amz-version-id";
    private static final DateTimeFormatter AWS_DATE_FORMATTER = DateTimeFormatter
            .ofPattern("yyyyMMdd'T'HHmmss'Z'")
            .withZone(ZoneOffset.UTC);
    private static final Set<String> RESTRICTED_REQUEST_HEADERS = Set.of(
            "connection", "content-length", "expect", "host", "upgrade");
    private static final Set<String> RESTRICTED_RESPONSE_HEADERS = Set.of(
            "connection", "content-length", "transfer-encoding");

    private final URI localS3Uri;
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final Map<String, String> contentDispositions = new ConcurrentHashMap<>();
    private final Map<String, String> multipartContentDispositions = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final HttpServer server;

    @Getter
    private final int port;

    public LocalS3Proxy(int localS3Port) {
        try {
            localS3Uri = URI.create("http://localhost:" + localS3Port);
            server = HttpServer.create(new InetSocketAddress(0), 0);
            server.createContext("/", this::handle);
            server.setExecutor(executor);
            port = server.getAddress().getPort();
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to create LocalS3 proxy", ex);
        }
    }

    public void start() {
        server.start();
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try {
            if (isInvalidPresignedRequest(exchange)) {
                exchange.sendResponseHeaders(403, -1);
                return;
            }

            HttpResponse<byte[]> response = forward(exchange);
            rememberContentDisposition(exchange, response);
            writeResponse(exchange, response);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            exchange.sendResponseHeaders(503, -1);
        } finally {
            exchange.close();
        }
    }

    private HttpResponse<byte[]> forward(HttpExchange exchange) throws IOException, InterruptedException {
        URI requestUri = localS3Uri.resolve(exchange.getRequestURI().toString());
        byte[] body = exchange.getRequestBody().readAllBytes();
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(requestUri)
                .method(exchange.getRequestMethod(), HttpRequest.BodyPublishers.ofByteArray(body));

        exchange.getRequestHeaders().forEach((name, values) -> {
            if (!RESTRICTED_REQUEST_HEADERS.contains(name.toLowerCase())) {
                values.forEach(value -> requestBuilder.header(name, value));
            }
        });

        return httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    private void rememberContentDisposition(HttpExchange exchange, HttpResponse<byte[]> response) {
        Map<String, String> parameters = queryParameters(exchange.getRequestURI().getRawQuery());
        String path = exchange.getRequestURI().getPath();
        String contentDisposition = exchange.getRequestHeaders().getFirst(CONTENT_DISPOSITION);
        if ("POST".equals(exchange.getRequestMethod())
                && parameters.containsKey("uploads")
                && contentDisposition != null) {
            multipartContentDispositions.put(path, contentDisposition);
        }

        if ("POST".equals(exchange.getRequestMethod()) && parameters.containsKey("uploadId")) {
            contentDisposition = multipartContentDispositions.remove(path);
        }

        String versionId = response.headers().firstValue(VERSION_ID).orElse(null);
        if (contentDisposition != null && versionId != null) {
            contentDispositions.put(versionKey(path, versionId), contentDisposition);
        }
    }

    private static boolean isInvalidPresignedRequest(HttpExchange exchange) {
        Map<String, String> parameters = queryParameters(exchange.getRequestURI().getRawQuery());
        if (!parameters.containsKey("X-Amz-Signature")) {
            return false;
        }

        boolean downloadRequest = parameters.containsKey("versionId");
        if (downloadRequest && !"GET".equals(exchange.getRequestMethod())) {
            return true;
        }
        if (!downloadRequest && !"PUT".equals(exchange.getRequestMethod())) {
            return true;
        }

        String signedAt = parameters.get("X-Amz-Date");
        String expiresIn = parameters.get("X-Amz-Expires");
        if (signedAt == null || expiresIn == null) {
            return false;
        }

        Instant expiresAt = Instant.from(AWS_DATE_FORMATTER.parse(signedAt))
                .plusSeconds(Long.parseLong(expiresIn));
        return !Instant.now().isBefore(expiresAt);
    }

    private void writeResponse(HttpExchange exchange, HttpResponse<byte[]> response) throws IOException {
        Headers responseHeaders = exchange.getResponseHeaders();
        response.headers().map().forEach((name, values) -> {
            if (!RESTRICTED_RESPONSE_HEADERS.contains(name.toLowerCase())) {
                responseHeaders.put(name, List.copyOf(values));
            }
        });

        Map<String, String> parameters = queryParameters(exchange.getRequestURI().getRawQuery());
        String versionId = parameters.get("versionId");
        if (versionId != null) {
            String contentDisposition = contentDispositions.get(
                    versionKey(exchange.getRequestURI().getPath(), versionId));
            if (contentDisposition != null) {
                responseHeaders.set(CONTENT_DISPOSITION, contentDisposition);
            }
        }

        setOverride(responseHeaders, parameters, "response-content-disposition", CONTENT_DISPOSITION);
        setOverride(responseHeaders, parameters, "response-content-type", CONTENT_TYPE);

        byte[] body = response.body();
        long responseLength = "HEAD".equals(exchange.getRequestMethod()) ? -1 : body.length;
        exchange.sendResponseHeaders(response.statusCode(), responseLength);
        if (responseLength >= 0) {
            exchange.getResponseBody().write(body);
        }
    }

    private static void setOverride(
            Headers headers,
            Map<String, String> parameters,
            String parameterName,
            String headerName) {
        String value = parameters.get(parameterName);
        if (value != null) {
            headers.set(headerName, value);
        }
    }

    private static Map<String, String> queryParameters(String query) {
        Map<String, String> parameters = new HashMap<>();
        if (query == null) {
            return parameters;
        }

        for (String parameter : query.split("&")) {
            String[] pair = parameter.split("=", 2);
            String name = decode(pair[0]);
            String value = pair.length == 2 ? decode(pair[1]) : "";
            parameters.put(name, value);
        }
        return parameters;
    }

    private static String decode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private static String versionKey(String path, String versionId) {
        return path + ':' + versionId;
    }
}
