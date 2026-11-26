# file-storage

Сервис, обращающийся напрямую к s3 через AWS JAVA SDK. Используется для генерации pre-signed URL that can be used to
access an Amazon S3 resource without requiring the user of the URL to know the account's AWS security credentials.

## Параметры запуска

Для работы с 2 версией `AWS SDK S3 V2`

```yaml
s3-sdk-v2:
  endpoint: 'http://127.0.0.1:9000'
  bucket-name: 'files-v2'
  region: 'RU'
  access-key: 'local-access-key'
  secret-key: 'local-secret-key'
```
