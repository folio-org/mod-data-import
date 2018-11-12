package org.folio.service;

import io.vertx.core.Future;

import java.io.File;

public class LocalFileStorageService implements FileStorageService{
  @Override
  public String getServiceName() {
    return "LOCAL_STORAGE";
  }

  @Override
  public Future<File> getFile(String path) {
    return null;
  }

  @Override
  public Future<String> saveFile(File file) {
    return null;
  }

  @Override
  public Future<Boolean> deleteFile(String path) {
    return null;
  }
}
