package org.folio.service.s3storage;

import org.folio.s3.client.FolioS3Client;
import org.folio.s3.client.S3ClientFactory;
import org.folio.s3.client.S3ClientProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class FolioS3ClientFactory {

  @Value("${minio.endpoint}")
  private String endpoint;

  @Value("${minio.accessKey}")
  private String accessKey;

  @Value("${minio.secretKey}")
  private String secretKey;

  @Value("${minio.bucket}")
  private String bucket;

  @Value("${minio.region}")
  private String region;

  @Value("#{ T(Boolean).parseBoolean('${minio.awsSdk}')}")
  private boolean awsSdk;

  @Value("#{ T(Boolean).parseBoolean('${minio.forcePathStyle}')}")
  private boolean forcePathStyle;

  private FolioS3Client folioS3Client;

  public FolioS3ClientFactory() {
    this.folioS3Client = null;
  }

  public FolioS3Client getFolioS3Client() {
    if (folioS3Client != null) {
      return folioS3Client;
    }
    folioS3Client = createFolioS3Client();
    return folioS3Client;
  }

  private FolioS3Client createFolioS3Client() {
    return S3ClientFactory.getS3Client(
      S3ClientProperties
        .builder()
        .endpoint(endpoint)
        .accessKey(accessKey)
        .secretKey(secretKey)
        .bucket(bucket)
        .awsSdk(awsSdk)
        .forcePathStyle(forcePathStyle)
        .region(region)
        .build()
    );
  }
}
