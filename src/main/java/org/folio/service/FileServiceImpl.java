package org.folio.service;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.folio.rest.jaxrs.model.UploadDefinition;

import javax.ws.rs.NotFoundException;
import java.io.InputStream;
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

  @Override
  public Future<UploadDefinition> uploadFile(String fileId, String uploadDefinitionId, InputStream data) {
    //TODO replace stub
    return Future.succeededFuture(new UploadDefinition());
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
}
