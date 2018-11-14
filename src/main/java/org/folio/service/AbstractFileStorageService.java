package org.folio.service;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;
import org.apache.commons.io.IOUtils;
import org.folio.rest.jaxrs.model.FileDefinition;

import javax.ws.rs.BadRequestException;
import java.io.InputStream;

public class AbstractFileStorageService implements FileStorageService {

  private Vertx vertx;
  private String tenantId;
  private FileSystem fs;

  public AbstractFileStorageService(Vertx vertx, String tenantId) {
    this.vertx = vertx;
    this.tenantId = tenantId;
    this.fs = vertx.fileSystem();
  }

  @Override
  public String getServiceName() {
    return "LOCAL_STORAGE";
  }


  @Override
  public Future<Buffer> getFile(String path) {
    return null;
  }

  @Override
  public Future<FileDefinition> saveFile(InputStream data, FileDefinition fileDefinition) {
    Future<FileDefinition> future = Future.future();
    getStoragePath(fileDefinition.getUploadDefinitionId())
      .setHandler(pathReply -> {
        if (pathReply.succeeded()) {
          try {
            String path = pathReply.result();
            fs.mkdirsBlocking(path);
            fs.writeFileBlocking(path + "/" + fileDefinition.getName(), Buffer.buffer(IOUtils.toByteArray(data)));
            fileDefinition.setSourcePath(path + "/" + fileDefinition.getName());
            fileDefinition.setLoaded(true);
            future.complete(fileDefinition);
          } catch (Exception e) {
            future.fail(e);
          }
        } else {
          future.fail(new BadRequestException());
        }
      });
    return future;
  }

  public Future<String> getStoragePath(String uploadDefinitionId) {
    return Future.succeededFuture("./storage/upload/" + uploadDefinitionId);
  }

  @Override
  public Future<Boolean> deleteFile(String path) {
    return null;
  }

}
