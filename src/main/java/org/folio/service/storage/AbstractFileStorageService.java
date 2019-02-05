package org.folio.service.storage;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.file.FileSystem;
import org.folio.dataimport.util.ConfigurationUtil;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.FileDefinition;

import java.io.File;

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
  public File getFile(String path) {
    return new File(path);
  }

  protected Future<String> getStoragePath(String code, FileDefinition fileDefinition, OkapiConnectionParams params) {
    Future<String> pathFuture = Future.future();
    String suffix = "/" + fileDefinition.getUploadDefinitionId() + "/" + fileDefinition.getId();
    ConfigurationUtil.getPropertyByCode(code, params).setHandler(configValue -> {
      if (configValue.succeeded()) {
        pathFuture.complete(configValue.result() + suffix);
      } else {
        pathFuture.complete("./storage/upload" + suffix);
      }
    });
    return pathFuture;
  }
}
