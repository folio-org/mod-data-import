package org.folio.service.file;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.service.storage.FileStorageService;
import org.folio.service.storage.FileStorageServiceBuilder;
import org.folio.service.upload.UploadDefinitionService;

import javax.ws.rs.NotFoundException;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class FileUploadLifecycleServiceImpl implements FileUploadLifecycleService {

  private static final Logger logger = LoggerFactory.getLogger(FileUploadLifecycleServiceImpl.class);

  private Vertx vertx;
  private UploadDefinitionService uploadDefinitionService;
  private String tenantId;
  private FileStorageService fileStorage;

  public FileUploadLifecycleServiceImpl(UploadDefinitionService uploadDefinitionService) {
    this.uploadDefinitionService = uploadDefinitionService;
  }

  public FileUploadLifecycleServiceImpl(Vertx vertx, String tenantId, UploadDefinitionService uploadDefinitionService) {
    this.vertx = vertx;
    this.tenantId = tenantId;
    this.uploadDefinitionService = uploadDefinitionService;
  }

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
      Future<UploadDefinition> future = Future.future();
      Optional<FileDefinition> optionalFileDefinition = findFileDefinition(uploadDef, fileId);
      if (optionalFileDefinition.isPresent()) {
        uploadDef.setFileDefinitions(replaceFile(uploadDef.getFileDefinitions(), optionalFileDefinition.get().withStatus(FileDefinition.Status.UPLOADING)));
        future.complete(uploadDef);
      } else {
        String errorMessage = "FileDefinition not found. FileDefinition ID: " + fileId;
        logger.error(errorMessage);
        future.fail(new NotFoundException(errorMessage));
      }
      return future;
    });
  }

  @Override
  public Future<UploadDefinition> afterFileSave(FileDefinition fileDefinition, OkapiConnectionParams params) {
    Future<UploadDefinition> future = Future.future();
    uploadDefinitionService.updateBlocking(fileDefinition.getUploadDefinitionId(), definition -> {
      Future<UploadDefinition> updatingFuture = Future.future();
      definition.setFileDefinitions(replaceFile(definition.getFileDefinitions(),
        fileDefinition.withUploadedDate(new Date()).withStatus(FileDefinition.Status.UPLOADED)));
      setUploadDefinitionStatusAfterFileUpload(definition);
      uploadDefinitionService.updateJobExecutionStatus(fileDefinition.getJobExecutionId(), new StatusDto().withStatus(StatusDto.Status.FILE_UPLOADED), params)
        .setHandler(booleanAsyncResult -> {
          if (booleanAsyncResult.succeeded()) {
            updatingFuture.complete(definition);
            future.complete(definition);
          } else {
            String statusUpdateErrorMessage = "Error updating status for JobExecution with id " + fileDefinition.getJobExecutionId();
            logger.error(statusUpdateErrorMessage);
            updatingFuture.fail(statusUpdateErrorMessage);
            future.fail(statusUpdateErrorMessage);
          }
        });
      return updatingFuture;
    });
    return future;
  }

  @Override
  public Future<FileDefinition> saveFileChunk(String fileId, UploadDefinition uploadDefinition, byte[] data, OkapiConnectionParams params) {
    Optional<FileDefinition> optionalFileDefinition = findFileDefinition(uploadDefinition, fileId);
    if (optionalFileDefinition.isPresent()) {
      FileDefinition fileDefinition = optionalFileDefinition.get();
      return getStorage(params).compose(service -> service.saveFile(data, fileDefinition, params));
    } else {
      String errorMessage = "FileDefinition not found. FileDefinition ID: " + fileId;
      logger.error(errorMessage);
      return Future.failedFuture(new NotFoundException(errorMessage));
    }
  }

  @Override
  public Future<Boolean> deleteFile(String id, String uploadDefinitionId, OkapiConnectionParams params) {
    return uploadDefinitionService.updateBlocking(uploadDefinitionId, uploadDefinition -> {
      Future<UploadDefinition> future = Future.future();
      List<FileDefinition> definitionList = uploadDefinition.getFileDefinitions();
      Optional<FileDefinition> optionalFileDefinition = definitionList
        .stream()
        .filter(f -> f.getId().equals(id)).findFirst();
      if (optionalFileDefinition.isPresent()) {
        FileDefinition fileDefinition = optionalFileDefinition.get();
        uploadDefinitionService.deleteFile(fileDefinition, params)
          .setHandler(deleteFileResult -> {
            if (deleteFileResult.succeeded()) {
              definitionList.remove(fileDefinition);
              uploadDefinition.setFileDefinitions(definitionList);
              future.complete(uploadDefinition);
              uploadDefinitionService.updateJobExecutionStatus(fileDefinition.getJobExecutionId(), new StatusDto().withStatus(StatusDto.Status.DISCARDED), params)
                .setHandler(updateStatusResult -> {
                  if (updateStatusResult.failed()) {
                    logger.error(
                      "Couldn't update JobExecution status with id {} to DISCARDED after file with id {} was deleted",
                      fileDefinition.getJobExecutionId(), id);
                  }
                });
            } else {
              future.fail(deleteFileResult.cause());
            }
          });
      } else {
        String errorMessage = String.format("FileDefinition with id %s was not fount", id);
        logger.error(errorMessage);
        future.fail(errorMessage);
      }
      return future;
    }).map(Objects::nonNull);
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
    return fileStorage != null ? Future.succeededFuture(fileStorage) : FileStorageServiceBuilder
      .build(vertx, tenantId, params);
  }
}
