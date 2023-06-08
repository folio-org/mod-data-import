package org.folio.service.s3storage;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import org.folio.rest.jaxrs.model.FileUploadInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


@Service
public class MinioStorageServiceImpl implements MinioStorageService {

  @Autowired
  private FolioS3ClientFactory folioS3ClientFactory;

  @Autowired
  private Vertx vertx;

  @Value("${minio.bucket}")
  private String bucket;

  @SneakyThrows
  public Future<FileUploadInfo> getFileUploadUrl(String upLoadFileName, String tenantId) {
    Promise<FileUploadInfo> promise = Promise.promise();
    var client = folioS3ClientFactory.getFolioS3Client();
    //var key = buildPrefix(tenantId) + "/" + upLoadFileName;
    var key =  upLoadFileName;

   /*
   if (!StringUtils.hasText(bucket)) {
       throw new Exception("Bucket not found");
    }
    */

    vertx.executeBlocking(blockingFuture -> {
      String url = null;
      try {
        url = client.getPresignedUrl(key);
      } catch (Exception e) {
        blockingFuture.fail(e);
      }
      blockingFuture.complete(url);
    }, asyncResult -> {
      if (asyncResult.failed()) {
        promise.fail(asyncResult.cause());
      } else {
        String url = (String) asyncResult.result();
        FileUploadInfo fileUpload = new FileUploadInfo();
        fileUpload.setUrl(url);
        fileUpload.setKey(key);
        promise.complete(fileUpload);
      }
    });

    return promise.future();

  }

  private String buildPrefix(String tenantId) {
    return tenantId ;
  }
}
