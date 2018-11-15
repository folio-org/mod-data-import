package org.folio.service.storage;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;
import org.folio.util.ConfigurationUtil;

import java.util.Map;

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
    fs.readFile(path, result -> {
      if (result.succeeded()) {
        future.complete(result.result());
      } else {
        future.fail(result.cause());
      }
    });
    return future;
  }

  protected Future<String> getStoragePath(String code, String uploadDefId, Map<String, String> okapiHeaders) {
    Future<String> pathFuture = Future.future();
    ConfigurationUtil.getPropertyByCode(code, okapiHeaders).setHandler(configValue -> {
      if (configValue.succeeded()) {
        pathFuture.complete(configValue.result());
      } else {
        pathFuture.complete("./storage/upload/" + uploadDefId);
      }
    });
    return pathFuture;
  }
}
