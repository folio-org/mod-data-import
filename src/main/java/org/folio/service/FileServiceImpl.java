package org.folio.service;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.folio.rest.jaxrs.model.File;

import javax.ws.rs.NotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;


public class FileServiceImpl implements FileService {

  private Vertx vertx;
  private UploadDefinitionService uploadDefinitionService;

  public FileServiceImpl(UploadDefinitionService uploadDefinitionService) {
    this.uploadDefinitionService = uploadDefinitionService;
  }

  public FileServiceImpl(Vertx vertx, String tenantId) {
    this.vertx = vertx;
    this.uploadDefinitionService = new UploadDefinitionServiceImpl(vertx, tenantId);
  }

  public Future<List<File>> getFiles(String query, int offset, int limit) {
    return null;
  }

  @Override
  public Future<Optional<File>> getFileById(String id) {
    return null;
  }

  @Override
  public Future<List<File>> getFileByUploadDefinitionId(String id) {
    return null;
  }

  @Override
  public Future<String> addFile(File file) {
    file.setId(UUID.randomUUID().toString());
    return null;
  }

  @Override
  public Future<String> uploadFile(String fileId, InputStream data) {
//    fileDao.getFileByUploadDefinitionId(file.getUploadDefinitionId()).map(files -> files.);
    return Future.succeededFuture(fileId);
  }

  @Override
  public Future<Boolean> updateFile(File file) {
    return null;
  }

  @Override
  public Future<Boolean> deleteFile(String id, String uploadDefinitionId) {
    return uploadDefinitionService.getUploadDefinitionById(uploadDefinitionId)
      .compose(optionalDef -> optionalDef
        .map(def ->
          uploadDefinitionService.updateUploadDefinition(def.withFiles(def.getFiles()
            .stream()
            .filter(f -> !f.getId().equals(id))
            .collect(Collectors.toList()))))
        .orElse(Future.failedFuture(new NotFoundException(
          String.format("Upload definition with id '%s' not found", uploadDefinitionId)))
        )
      );
  }

  @Override
  public Future<Boolean> deleteFilesByUploadDefinitionId(String uploadDefinitionId) {
    return null;
  }

}
