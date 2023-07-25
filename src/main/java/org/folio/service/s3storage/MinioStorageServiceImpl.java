package org.folio.service.s3storage;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

  public Future<FileUploadInfo> getFileUploadFirstPartUrl(
    String uploadFileName,
    String tenantId
  ) {
    Promise<String> uploadIdPromise = Promise.promise();
    FolioS3Client client = folioS3ClientFactory.getFolioS3Client();

    String key = buildKey(tenantId, uploadFileName);

    vertx.executeBlocking(
      (Promise<String> blockingFuture) -> {
        try {
          String uploadId = client.initiateMultipartUpload(key);
          LOGGER.info("Created upload ID {} for key {}", uploadId, key);
          blockingFuture.complete(uploadId);
        } catch (S3ClientException e) {
          blockingFuture.fail(e);
        }
      },
      (AsyncResult<String> asyncResult) -> {
        if (asyncResult.failed()) {
          uploadIdPromise.fail(asyncResult.cause());
        } else {
          uploadIdPromise.complete(asyncResult.result());
        }
      }
    );

    return uploadIdPromise
      .future()
      .compose(outcome -> getFileUploadPartUrl(key, outcome, 1));
  }

  public Future<FileUploadInfo> getFileUploadPartUrl(
    String key,
    String uploadId,
    int partNumber
  ) {
    Promise<FileUploadInfo> promise = Promise.promise();
    FolioS3Client client = folioS3ClientFactory.getFolioS3Client();

    vertx.executeBlocking(
      (Promise<String> blockingFuture) -> {
        try {
          LOGGER.info(
            "Getting presigned URL for part {} of key {}/upload ID {}",
            partNumber,
            key,
            uploadId
          );
          blockingFuture.complete(
            client.getPresignedMultipartUploadUrl(key, uploadId, partNumber)
          );
        } catch (S3ClientException e) {
          blockingFuture.fail(e);
        }
      },
      (AsyncResult<String> asyncResult) -> {
        if (asyncResult.failed()) {
          promise.fail(asyncResult.cause());
        } else {
          String url = asyncResult.result();
          FileUploadInfo fileUpload = new FileUploadInfo();
          fileUpload.setUrl(url);
          fileUpload.setKey(key);
          fileUpload.setUploadId(uploadId);
          promise.complete(fileUpload);
        }
      }
    );

    return promise.future();
  }
  public Future<Boolean> completeMultipartFileUpload(String path,String uploadId,List<String> partEtags ) {
    Promise<Boolean> promise = Promise.promise();
    FolioS3Client client = folioS3ClientFactory.getFolioS3Client();

    vertx.executeBlocking(
        (Promise<String> blockingFuture) -> {
      try {
        
            client.completeMultipartUpload(path, uploadId, partEtags);
            blockingFuture.complete();
            promise.complete(true);
      
      } catch(Exception e) {
       
        LOGGER.error("Failed to complete multipart upload {}", e.getMessage());
        blockingFuture.fail(e);
        promise.complete(false);
      }
    });
    return promise.future();
  }
  private static String buildKey(String tenantId, String fileName) {
    return String.format(
      "%s/%d-%s",
      tenantId,
      System.currentTimeMillis(),
      fileName
    );
  }
}
