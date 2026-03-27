package org.folio.service.s3storage;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.io.InputStream;
import java.util.List;
import javax.ws.rs.NotFoundException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.jaxrs.model.FileDownloadInfo;
import org.folio.rest.jaxrs.model.FileUploadInfo;
import org.folio.s3.client.FolioS3Client;
import org.folio.s3.exception.S3ClientException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MinioStorageServiceImpl implements MinioStorageService {

  private static final Logger LOGGER = LogManager.getLogger();

  private FolioS3ClientFactory folioS3ClientFactory;

  private Vertx vertx;

  @Autowired
  public MinioStorageServiceImpl(
    FolioS3ClientFactory folioS3ClientFactory,
    Vertx vertx
  ) {
    this.folioS3ClientFactory = folioS3ClientFactory;
    this.vertx = vertx;
  }

  @Override
  public Future<FileUploadInfo> getFileUploadFirstPartUrl(
    String uploadFileName,
    String tenantId
  ) {
    FolioS3Client client = folioS3ClientFactory.getFolioS3Client();
    String key = buildKey(tenantId, uploadFileName);

    return vertx.executeBlocking(() -> {
      // we just built the key; no need to verify
      try {
        String uploadId = client.initiateMultipartUpload(key);
        LOGGER.info("getFileUploadFirstPartUrl:: Created upload ID {} for key {}", uploadId, key);
        return uploadId;
      } catch (S3ClientException e) {
        LOGGER.warn("getFileUploadFirstPartUrl:: Failed to create upload ID for key {}", key, e);
        throw e;
      }
    }).compose(outcome -> getFileUploadPartUrl(key, outcome, 1));
  }

  @Override
  public Future<FileUploadInfo> getFileUploadPartUrl(
    String key,
    String uploadId,
    int partNumber
  ) {
    FolioS3Client client = folioS3ClientFactory.getFolioS3Client();

    return vertx
      .executeBlocking(() -> {
        try {
          verifyKey(key);
          LOGGER.info("getFileUploadPartUrl:: Getting presigned URL for part {} of key {}/upload ID {}", partNumber, key, uploadId);
          return client.getPresignedMultipartUploadUrl(key, uploadId, partNumber);
        } catch (S3ClientException e) {
          LOGGER.warn("getFileUploadPartUrl:: Failed to get presigned URL for part {} of key {}/upload ID {}",
            partNumber, key, uploadId, e);
          throw e;
        }
      })
      .map(url -> new FileUploadInfo().withUrl(url).withKey(key).withUploadId(uploadId));
  }

  @Override
  public Future<FileDownloadInfo> getFileDownloadUrl(String key) {
    FolioS3Client client = folioS3ClientFactory.getFolioS3Client();

    return vertx
      .executeBlocking(() -> {
        try {
          verifyKey(key);

          // ensure the key is present in the bucket
          // to check if it exists, we need to search with "key" as a prefix
          // hence the list() call and array checking
          if (client.list(key).stream().noneMatch(key::equals)) {
            throw LOGGER.throwing(new NotFoundException("Key " + key + " is not present in S3"));
          }

          LOGGER.info("getFileDownloadUrl:: Getting presigned URL for key {}", key);
          return client.getPresignedUrl(key);
        } catch (S3ClientException e) {
          LOGGER.warn("getFileDownloadUrl:: Failed to get presigned URL for key {}", key, e);
          throw e;
        }
      })
      .map(url -> new FileDownloadInfo().withUrl(url));
  }

  @Override
  public Future<InputStream> readFile(String key) {
    FolioS3Client client = folioS3ClientFactory.getFolioS3Client();

    return vertx.executeBlocking(() -> {
      try {
        verifyKey(key);
        InputStream inStream = client.read(key);
        LOGGER.info("readFile:: Created input stream to read remote file for key {}", key);
        return inStream;
      } catch (S3ClientException e) {
        LOGGER.error("readFile:: Could not read from S3 for key {}", key, e);
        throw e;
      }
    });
  }

  @Override
  public Future<String> write(String path, InputStream is) {
    FolioS3Client client = folioS3ClientFactory.getFolioS3Client();

    return vertx.executeBlocking(() -> {
      try {
        verifyKey(path);
        LOGGER.info("Writing remote file for path {}", path);
        return client.write(path, is);
      } catch (S3ClientException e) {
        LOGGER.error("Error while writing file to S3 for path {} cause: ", path, e);
        throw e;
      }
    });
  }

  @Override
  public Future<Void> remove(String key) {
    FolioS3Client client = folioS3ClientFactory.getFolioS3Client();

    return vertx.executeBlocking(() -> {
      try {
        verifyKey(key);

        LOGGER.info("remove:: Deleting file {}", key);
        client.remove(key);
        return null;
      } catch (S3ClientException e) {
        LOGGER.error("remove:: Could not remove from S3 by key {} cause:", key, e);
        throw e;
      }
    });
  }

  public Future<Void> completeMultipartFileUpload(
    String path,
    String uploadId,
    List<String> partEtags
  ) {
    FolioS3Client client = folioS3ClientFactory.getFolioS3Client();

    return vertx.executeBlocking(() -> {
      try {
        verifyKey(path);
        client.completeMultipartUpload(path, uploadId, partEtags);
        return null;
      } catch (S3ClientException e) {
        LOGGER.error("completeMultipartFileUpload:: Failed to complete multipart upload for path {}, uploadId {}",
          path, uploadId, e);
        throw e;
      }
    });
  }

  private static void verifyKey(String key) {
    if (!key.startsWith("data-import/")) {
      throw new IllegalArgumentException(
        "Key must be located in folder 'data-import/' but was " + key
      );
    }
  }

  private static String buildKey(String tenantId, String fileName) {
    return String.format(
      "data-import/%s/%d-%s",
      tenantId,
      System.currentTimeMillis(),
      fileName
    );
  }
}
