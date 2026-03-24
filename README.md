# file-storage

`file-storage` — сервис для хранения файлов поверх S3-совместимого object storage.
Сервис выступает как orchestration/control-plane над бакетом:

- создаёт идентификаторы файлов;
- сохраняет и читает metadata;
- выдаёт pre-signed URL для upload/download;
- поддерживает multipart upload.

Сервис ориентирован на S3-compatible backends, в тестах используются MinIO и Ceph.

## Что умеет

- Single upload через pre-signed `PUT URL`
- Download через pre-signed `GET URL`
- Получение `FileData` по `fileDataId`
- Multipart upload в двух режимах:
    - legacy data-plane flow через thrift
    - direct-to-S3 presigned multipart flow со статусом загрузки

## API

Сервис поднимает два thrift endpoint:

- `/file_storage/v2`
  Основной совместимый API для single upload, download, metadata и legacy multipart flow.
- `/file_storage/presigned-multipart/v1`
  Отдельный API для presigned multipart upload, где части загружаются напрямую в S3-compatible storage.

Точные контракты находятся в артефакте `file-storage-proto`.

Для presigned multipart flow публичный lifecycle выглядит так:

- `CreateMultipartUpload` — открыть multipart-сессию и получить её состояние
- `GetMultipartUpload` — получить текущее состояние multipart-сессии
- `PresignMultipartUploadPart` — получить URL и обязательные headers для загрузки части
- `CompleteMultipartUpload` — завершить multipart upload по списку `etag + sequence_part`
- `AbortMultipartUpload` — прервать multipart upload

Новый multipart API возвращает статус загрузки:

- `pending_upload`
- `uploaded`
- `aborted`

## Конфигурация

Основные параметры задаются через `application.yml`:

```yaml
s3-sdk-v2:
  endpoint: 'http://127.0.0.1:9000'
  bucket-name: 'files-v2'
  region: 'RU'
  access-key: 'minio'
  secret-key: 'minio123'
  multipart-url-ttl: '1h'
```

Что означают параметры:

- `endpoint` — URL S3-compatible storage
- `bucket-name` — бакет, в котором сервис хранит файлы и metadata
- `region` — S3 region
- `access-key` / `secret-key` — credentials для доступа к storage
- `multipart-url-ttl` — TTL для presigned URL на multipart parts

Сервис сам создаёт бакет и включает versioning, если это поддерживается backend-ом.

## Metadata Model

Для каждого `fileDataId` сервис хранит:

- metadata объекта;
- сам файл как отдельную object version.

Из этого строятся:

- `getFileData`
- `generateDownloadUrl`
- multipart completion flow

Иными словами, клиентский контракт завязан на `fileDataId`, а конкретные object versions остаются внутренней деталью
реализации.
