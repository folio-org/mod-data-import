package org.folio.service.file;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.service.storage.FileStorageServiceBuilder;
import org.folio.service.upload.UploadDefinitionService;
import org.folio.util.OkapiConnectionParams;

import javax.ws.rs.NotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;


public class FileServiceImpl implements FileService {

  private Vertx vertx;
  private UploadDefinitionService uploadDefinitionService;
  private String tenantId;

  public FileServiceImpl(UploadDefinitionService uploadDefinitionService) {
    this.uploadDefinitionService = uploadDefinitionService;
  }

  public FileServiceImpl(Vertx vertx, String tenantId, UploadDefinitionService uploadDefinitionService) {
    this.vertx = vertx;
    this.tenantId = tenantId;
    this.uploadDefinitionService = uploadDefinitionService;
  }

  @Override
  public Future<UploadDefinition> uploadFile(String fileId, String uploadDefinitionId, InputStream data, OkapiConnectionParams params) {
    return uploadDefinitionService.updateBlocking(uploadDefinitionId, uploadDefinition -> {
      Future<UploadDefinition> future = Future.future();
      Optional<FileDefinition> optionalFileDefinition = uploadDefinition.getFileDefinitions().stream().filter(fileFilter -> fileFilter.getId().equals(fileId))
        .findFirst();
      if (optionalFileDefinition.isPresent()) {
        FileDefinition fileDefinition = optionalFileDefinition.get();
        FileStorageServiceBuilder
          .build(vertx, tenantId, params)
          .map(service -> service.saveFile(data, fileDefinition, params)
            .setHandler(onFileSave -> {
              if (onFileSave.succeeded()) {
                uploadDefinition.setFileDefinitions(replaceFile(uploadDefinition.getFileDefinitions(), onFileSave.result()));
                uploadDefinition.setStatus(uploadDefinition.getFileDefinitions().stream().allMatch(FileDefinition::getLoaded)
                  ? UploadDefinition.Status.LOADED
                  : UploadDefinition.Status.IN_PROGRESS);
                future.complete(uploadDefinition);
              } else {
                future.fail("Error during file save");
              }
            }));
      } else {
        future.fail("FileDefinition not found. FileDefinition ID: " + fileId);
      }
      return future;
    });
  }

  @Override
  public Future<Boolean> deleteFile(String id, String uploadDefinitionId) {
    return uploadDefinitionService.getUploadDefinitionById(uploadDefinitionId)
      .compose(optionalDef -> optionalDef
        .map(def ->
          uploadDefinitionService.updateUploadDefinition(def.withFileDefinitions(def.getFileDefinitions()
            .stream()
            .filter(f -> !f.getId().equals(id))
            .collect(Collectors.toList())))
            .map(Objects::nonNull))
        .orElse(Future.failedFuture(new NotFoundException(
          String.format("Upload definition with id '%s' not found", uploadDefinitionId)))
        )
      );
  }

  private List<FileDefinition> replaceFile(List<FileDefinition> list, FileDefinition fileDefinition) {
    FileDefinition fileToReplace = null;
    for (FileDefinition definition : list) {
      if (definition.getId().equals(fileDefinition.getId())) {
        fileToReplace = definition;
        break;
      }
    }
    if (fileToReplace != null) {
      list.remove(fileToReplace);
    }
    list.add(fileDefinition);
    return list;
  }
}
