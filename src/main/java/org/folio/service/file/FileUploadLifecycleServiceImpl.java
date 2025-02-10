package org.folio.service.file;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.service.storage.FileStorageService;
import org.folio.service.storage.FileStorageServiceBuilder;
import org.folio.service.upload.UploadDefinitionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Service
public class FileUploadLifecycleServiceImpl implements FileUploadLifecycleService {

  private static final Logger LOGGER = LogManager.getLogger();

  @Autowired
  private UploadDefinitionService uploadDefinitionService;

  private Future<FileStorageService> fileStorage = Future.succeededFuture();

  private Optional<FileDefinition> findFileDefinition(UploadDefinition uploadDefinition, String fileId) {
    return uploadDefinition.getFileDefinitions()
      .stream()
      .filter(fileFilter -> fileFilter.getId().equals(fileId))
      .findFirst();
  }

  private void setUploadDefinitionStatusAfterFileUpload(UploadDefinition definition) {
    definition.setStatus(definition.getFileDefinitions()
      .stream()
      .allMatch(fileDef -> fileDef.getStatus().equals(FileDefinition.Status.UPLOADED))
      ? UploadDefinition.Status.LOADED
      : UploadDefinition.Status.IN_PROGRESS);
    definition.setStatus(definition.getFileDefinitions()
      .stream()
      .allMatch(fileDef -> fileDef.getStatus().equals(FileDefinition.Status.ERROR))
      ? UploadDefinition.Status.ERROR
      : definition.getStatus());
  }

  @Override
  public Future<UploadDefinition> beforeFileSave(String fileId, String uploadDefinitionId, OkapiConnectionParams params) {
    return uploadDefinitionService.updateBlocking(uploadDefinitionId, uploadDef -> {
      Promise<UploadDefinition> promise = Promise.promise();
      Optional<FileDefinition> optionalFileDefinition = findFileDefinition(uploadDef, fileId);
      if (optionalFileDefinition.isPresent()) {
        uploadDef.setFileDefinitions(replaceFile(uploadDef.getFileDefinitions(), optionalFileDefinition.get().withStatus(FileDefinition.Status.UPLOADING)));
        promise.complete(uploadDef);
      } else {
        String errorMessage = "FileDefinition not found. FileDefinition ID: " + fileId;
        LOGGER.warn(errorMessage);
        promise.fail(new NotFoundException(errorMessage));
      }
      return promise.future();
    }, params.getTenantId());
  }

  @Override
  public Future<UploadDefinition> afterFileSave(FileDefinition fileDefinition, OkapiConnectionParams params) {
    LOGGER.debug("afterFileSave:: fileDefinition.jobExecutionId {}", fileDefinition.getJobExecutionId());
    return uploadDefinitionService.updateBlocking(fileDefinition.getUploadDefinitionId(), definition -> {
      definition.setFileDefinitions(replaceFile(definition.getFileDefinitions(),
        fileDefinition.withUploadedDate(new Date()).withStatus(FileDefinition.Status.UPLOADED)));
      setUploadDefinitionStatusAfterFileUpload(definition);
      uploadDefinitionService.updateJobExecutionStatus(fileDefinition.getJobExecutionId(), new StatusDto().withStatus(StatusDto.Status.FILE_UPLOADED), params)
        .onComplete(booleanAsyncResult -> {
          if (booleanAsyncResult.failed()) {
            LOGGER.warn("afterFileSave:: Couldn't update JobExecution status with id {} to FILE_UPLOADED after file with id {} was saved to storage",
              fileDefinition.getJobExecutionId(), fileDefinition.getId(), booleanAsyncResult.cause());
          }
        });
      return Future.succeededFuture(definition);
    }, params.getTenantId());
  }

  @Override
  public Future<FileDefinition> saveFileChunk(String fileId, UploadDefinition uploadDefinition, byte[] data, OkapiConnectionParams params) {
    LOGGER.debug("saveFileChunk:: fileId {}", fileId);
    Optional<FileDefinition> optionalFileDefinition = findFileDefinition(uploadDefinition, fileId);
    if (optionalFileDefinition.isPresent()) {
      FileDefinition fileDefinition = optionalFileDefinition.get();
      return getStorage(params).compose(service -> service.saveFile(data, fileDefinition, params));
    } else {
      String errorMessage = "FileDefinition not found. FileDefinition ID: " + fileId;
      LOGGER.warn(errorMessage);
      return Future.failedFuture(new NotFoundException(errorMessage));
    }
  }

  @Override
  public Future<Boolean> deleteFile(String id, String uploadDefinitionId, OkapiConnectionParams params) {
    LOGGER.debug("deleteFile:: fileId {}, uploadDefinitionId {}", id, uploadDefinitionId);
    return uploadDefinitionService.updateBlocking(uploadDefinitionId, uploadDefinition -> {
      Promise<UploadDefinition> promise = Promise.promise();
      List<FileDefinition> definitionList = uploadDefinition.getFileDefinitions();
      Optional<FileDefinition> optionalFileDefinition = definitionList
        .stream()
        .filter(f -> f.getId().equals(id)).findFirst();
      if (optionalFileDefinition.isPresent()) {
        FileDefinition fileDefinition = optionalFileDefinition.get();
        uploadDefinitionService.deleteFile(fileDefinition, params)
          .onComplete(deleteFileResult -> {
            if (deleteFileResult.succeeded()) {
              definitionList.remove(fileDefinition);
              uploadDefinition.setFileDefinitions(definitionList);
              promise.complete(uploadDefinition);
              uploadDefinitionService.updateJobExecutionStatus(fileDefinition.getJobExecutionId(), new StatusDto().withStatus(StatusDto.Status.DISCARDED), params)
                .onComplete(updateStatusResult -> {
                  if (updateStatusResult.failed()) {
                    LOGGER.warn(
                      "deleteFile:: Couldn't update JobExecution status with id {} to DISCARDED after file with id {} was deleted",
                      fileDefinition.getJobExecutionId(), id, updateStatusResult.cause());
                  }
                });
            } else {
              promise.fail(deleteFileResult.cause());
            }
          });
      } else {
        String errorMessage = String.format("FileDefinition with id %s was not fount", id);
        LOGGER.warn(errorMessage);
        promise.fail(errorMessage);
      }
      return promise.future();
    }, params.getTenantId()).map(Objects::nonNull);
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

  private Future<FileStorageService> getStorage(OkapiConnectionParams params) {
    return fileStorage.compose(fileStorageService -> fileStorageService == null ? FileStorageServiceBuilder
      .build(params.getVertx(), params.getTenantId())
      .compose(h -> {
        fileStorage = Future.succeededFuture(h);
        return fileStorage;
      })
      : fileStorage);
  }
}
