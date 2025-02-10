package org.folio.service.storage;

import io.vertx.core.Vertx;
import io.vertx.core.file.FileSystem;
import org.folio.rest.jaxrs.model.FileDefinition;

import java.io.File;

public abstract class AbstractFileStorageService implements FileStorageService {

  protected Vertx vertx;
  protected String tenantId;
  protected FileSystem fs;

  protected AbstractFileStorageService(Vertx vertx, String tenantId) {
    this.vertx = vertx;
    this.tenantId = tenantId;
    this.fs = vertx.fileSystem();
  }

  @Override
  public File getFile(String path) {
    return new File(path);
  }

  protected String getStoragePath(FileDefinition fileDefinition) {
    String suffix = "/" + fileDefinition.getUploadDefinitionId() + "/" + fileDefinition.getId();
    return "./storage/upload" + suffix;
  }
}
