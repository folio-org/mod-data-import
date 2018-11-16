package org.folio.service.file;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.service.storage.FileStorageServiceBuilder;
import org.folio.service.upload.UploadDefinitionService;

import javax.ws.rs.NotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
  public Future<UploadDefinition> uploadFile(String fileId, String uploadDefinitionId, InputStream data, Map<String, String> okapiHeaders) {
    return uploadDefinitionService.getUploadDefinitionById(uploadDefinitionId)
      .map(optionalUploadDefinition -> optionalUploadDefinition
        .map(UploadDefinition::getFileDefinitions)
        .orElseThrow(NotFoundException::new))
      .map(fileDefinitions -> fileDefinitions
        .stream()
        .filter(fileFilter -> fileFilter.getId().equals(fileId))
        .findFirst()
        .map(filteredFileDefinition -> FileStorageServiceBuilder.build(vertx, tenantId, okapiHeaders)
          .map(service -> service.saveFile(data, filteredFileDefinition, okapiHeaders)
            .map(savedFile -> uploadDefinitionService.getUploadDefinitionById(uploadDefinitionId)
              .map(optionalDef ->
                optionalDef.map(def ->
                  uploadDefinitionService.updateUploadDefinition(def
                    .withFileDefinitions(replaceFile(def.getFileDefinitions(), savedFile))
                    .withStatus(UploadDefinition.Status.IN_PROGRESS))
                ).orElseThrow(NotFoundException::new))))
        )
        .orElseThrow(NotFoundException::new))
      .compose(reply -> reply)
      .compose(fileReply -> fileReply)
      .compose(defReply -> defReply)
      .compose(result -> result);
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
