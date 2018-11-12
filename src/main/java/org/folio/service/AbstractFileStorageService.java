package org.folio.service;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.folio.rest.jaxrs.model.FileDefinition;

import java.io.File;
import java.io.InputStream;

public class AbstractFileStorageService implements FileStorageService {

  private Vertx vertx;
  private String tenantId;

  public AbstractFileStorageService(Vertx vertx, String tenantId) {
    this.vertx = vertx;
    this.tenantId = tenantId;
  }

  @Override
  public String getServiceName() {
    return "LOCAL_STORAGE";
  }


  @Override
  public Future<File> getFile(String path) {
    return null;
  }

  @Override
  public   Future<FileDefinition> saveFile(InputStream data, FileDefinition fileDefinition) {
    return Future.succeededFuture(fileDefinition);
  }

  @Override
  public Future<Boolean> deleteFile(String path) {
    return null;
  }
}
