package org.folio.service.storage;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;
import org.folio.dataImport.util.ConfigurationUtil;
import org.folio.dataImport.util.OkapiConnectionParams;

public abstract class AbstractFileStorageService implements FileStorageService {

  protected Vertx vertx;
  protected String tenantId;
  protected FileSystem fs;

  public AbstractFileStorageService(Vertx vertx, String tenantId) {
    this.vertx = vertx;
    this.tenantId = tenantId;
    this.fs = vertx.fileSystem();
  }

  @Override
  public Future<Buffer> getFile(String path) {
    Future<Buffer> future = Future.future();
    fs.readFile(path, future.completer());
    return future;
  }

  protected Future<String> getStoragePath(String code, String uploadDefId, OkapiConnectionParams params) {
    Future<String> pathFuture = Future.future();
    ConfigurationUtil.getPropertyByCode(code, params).setHandler(configValue -> {
      if (configValue.succeeded()) {
        pathFuture.complete(configValue.result());
      } else {
        pathFuture.complete("./storage/upload/" + uploadDefId);
      }
    });
    return pathFuture;
  }
}
