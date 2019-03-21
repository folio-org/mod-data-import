package org.folio.service.upload;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.handler.impl.HttpStatusException;
import org.folio.HttpStatus;
import org.folio.dao.UploadDefinitionDao;
import org.folio.dao.UploadDefinitionDaoImpl;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.client.ChangeManagerClient;
import org.folio.rest.jaxrs.model.DefinitionCollection;
import org.folio.rest.jaxrs.model.Error;
import org.folio.rest.jaxrs.model.Errors;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollection;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.service.fileextension.FileExtensionService;
import org.folio.service.fileextension.FileExtensionServiceImpl;
import org.folio.service.storage.FileStorageServiceBuilder;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class UploadDefinitionServiceImpl implements UploadDefinitionService {

  private static final Logger logger = LoggerFactory.getLogger(UploadDefinitionServiceImpl.class);
  private static final String FILE_UPLOAD_ERROR_MESSAGE = "upload.fileSize.invalid";
  private static final String UPLOAD_FILE_EXTENSION_BLOCKED_ERROR_MESSAGE = "validation.uploadDefinition.fileExtension.blocked";

  private Vertx vertx;
  private String tenantId;
  private UploadDefinitionDao uploadDefinitionDao;
  private FileExtensionService fileExtensionService;

  public UploadDefinitionServiceImpl(UploadDefinitionDao uploadDefinitionDao) {
    this.uploadDefinitionDao = uploadDefinitionDao;
  }

  public UploadDefinitionServiceImpl(Vertx vertx, String tenantId) {
    this.vertx = vertx;
    this.tenantId = tenantId;
    this.uploadDefinitionDao = new UploadDefinitionDaoImpl(vertx, tenantId);
    this.fileExtensionService = new FileExtensionServiceImpl(vertx, tenantId);
  }

  public Future<DefinitionCollection> getUploadDefinitions(String query, int offset, int limit) {
    return uploadDefinitionDao.getUploadDefinitions(query, offset, limit);
  }

  @Override
  public Future<Optional<UploadDefinition>> getUploadDefinitionById(String id) {
    return uploadDefinitionDao.getUploadDefinitionById(id);
  }

  @Override
  public Future<UploadDefinition> addUploadDefinition(UploadDefinition uploadDefinition, OkapiConnectionParams params) {
    uploadDefinition.setId(UUID.randomUUID().toString());
    uploadDefinition.setStatus(UploadDefinition.Status.NEW);
    uploadDefinition.setCreateDate(new Date());
    uploadDefinition.getFileDefinitions().forEach(fileDefinition -> fileDefinition.withId(UUID.randomUUID().toString())
      .withCreateDate(new Date())
      .withStatus(FileDefinition.Status.NEW)
      .withUploadDefinitionId(uploadDefinition.getId()));
    return createJobExecutions(uploadDefinition, params)
      .map(this::checkUploadDefinitionBeforeSave).compose(defCheck -> defCheck)
      .map(def -> uploadDefinitionDao.addUploadDefinition(def))
      .map(uploadDefinition);
  }

  @Override
  public Future<UploadDefinition> updateBlocking(String uploadDefinitionId, UploadDefinitionDaoImpl.UploadDefinitionMutator mutator) {
    return uploadDefinitionDao.updateBlocking(uploadDefinitionId, mutator);
  }

  @Override
  public Future<Boolean> deleteUploadDefinition(String id, OkapiConnectionParams params) {
    return getUploadDefinitionById(id)
      .compose(optionalUploadDefinition -> optionalUploadDefinition
        .map(uploadDefinition -> getJobExecutions(uploadDefinition, params)
          .compose(jobExecutionList -> {
            if (canDeleteUploadDefinition(jobExecutionList)) {
              return deleteFiles(uploadDefinition.getFileDefinitions(), params)
                .compose(deleted -> uploadDefinitionDao.deleteUploadDefinition(id))
                .compose(result -> {
                  updateJobExecutionStatuses(jobExecutionList, new StatusDto().withStatus(StatusDto.Status.DISCARDED), params)
                    .otherwise(throwable -> {
                      logger.error("Couldn't update JobExecution status to DISCARDED after UploadDefinition {} was deleted", id, throwable);
                      return result;
                    });
                  return Future.succeededFuture(result);
                });
            } else {
              return Future.failedFuture(new BadRequestException(
                String.format("Cannot delete uploadDefinition %s - linked files are already being processed", id)));
            }
          }))
        .orElse(Future.failedFuture(new NotFoundException(
          String.format("UploadDefinition with id '%s' was not found", id))))
      );
  }

  @Override
  public Future<UploadDefinition> addFileDefinitionToUpload(FileDefinition fileDefinition) {
    return uploadDefinitionDao.updateBlocking(fileDefinition.getUploadDefinitionId(), uploadDef -> {
      Future<UploadDefinition> uploadDefFuture = Future.future();
      uploadDef.withFileDefinitions(addNewFileDefinition(uploadDef.getFileDefinitions(), fileDefinition));
      uploadDefFuture.complete(uploadDef);
      return uploadDefFuture;
    });
  }

  @Override
  public Future<Boolean> updateJobExecutionStatus(String jobExecutionId, StatusDto status, OkapiConnectionParams params) {
    Future<Boolean> future = Future.future();
    ChangeManagerClient client = new ChangeManagerClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.putChangeManagerJobExecutionsStatusById(jobExecutionId, status, response -> {
        if (response.statusCode() == HttpStatus.HTTP_OK.toInt()) {
          future.complete(true);
        } else {
          logger.error("Error updating status of JobExecution with id {}", jobExecutionId, response.statusMessage());
          future.fail(new HttpStatusException(response.statusCode(), "Error updating status of JobExecution"));
        }
      });
    } catch (Exception e) {
      future.fail(e);
    }
    return future;
  }

  @Override
  public Future<Boolean> deleteFile(FileDefinition fileDefinition, OkapiConnectionParams params) {
    return FileStorageServiceBuilder
      .build(vertx, tenantId, params)
      .compose(service -> service.deleteFile(fileDefinition));
  }

  @Override
  public Future<Errors> checkNewUploadDefinition(UploadDefinition definition) {
    Errors errors = new Errors()
      .withTotalRecords(0);
    return Future.succeededFuture()
      .map(v -> validateFreeSpace(definition, errors))
      .compose(errorsReply -> validateFilesExtensionName(definition, errors));
  }

  private Future<Errors> validateFilesExtensionName(UploadDefinition definition, Errors errors) {
    List<Future> listOfValidations = new ArrayList<>(definition.getFileDefinitions().size());
    for (FileDefinition fileDefinition : definition.getFileDefinitions()) {
      String fileExtension = getFileExtensionFromString(fileDefinition.getName());
      listOfValidations.add(fileExtensionService.getFileExtensionByExtenstion(fileExtension)
        .map(extension -> {
          if (extension.isPresent() && extension.get().getImportBlocked()) {
            errors.getErrors().add(new Error().withMessage(UPLOAD_FILE_EXTENSION_BLOCKED_ERROR_MESSAGE).withCode(fileDefinition.getName()));
            errors.setTotalRecords(errors.getTotalRecords() + 1);
          }
          return errors;
        }));
    }
    return CompositeFuture.all(listOfValidations)
      .map(errors);
  }

  private String getFileExtensionFromString(String fileName) {
    String extension = "";
    int i = fileName.lastIndexOf('.');
    if (i > 0) {
      extension = fileName.substring(i);
    }
    return extension;
  }

  /**
   * Calculate free disk space
   *
   * @return - free disck space in kbytes
   */
  private long getFreeDiskSpaceKb() {
    long freeSpaceKb = 0L;
    for (Path root : FileSystems.getDefault().getRootDirectories()) {
      try {
        FileStore store = Files.getFileStore(root);
        freeSpaceKb += store.getUsableSpace() / 1024;
      } catch (IOException e) {
        String errorMessage = "Error during check free disk size";
        logger.error(errorMessage, e);
        throw new InternalServerErrorException(errorMessage, e);
      }
    }
    return freeSpaceKb;
  }

  private Errors validateFreeSpace(UploadDefinition definition, Errors errors) {
    long totalFilesSize = 0L;
    long freeSpace = getFreeDiskSpaceKb();
    for (FileDefinition fileDefinition : definition.getFileDefinitions()) {
      if (fileDefinition.getSize() != null) {
        totalFilesSize += fileDefinition.getSize();
        if (freeSpace - fileDefinition.getSize() <= 0 || freeSpace - totalFilesSize <= 0) {
          errors.getErrors().add(new Error()
            .withMessage(FILE_UPLOAD_ERROR_MESSAGE)
            .withCode(fileDefinition.getName()));
          errors.setTotalRecords(errors.getTotalRecords() + 1);
        }
      }
    }
    return errors;
  }

  private Future<UploadDefinition> createJobExecutions(UploadDefinition definition, OkapiConnectionParams params) {
    Metadata metadata = definition.getMetadata();

    InitJobExecutionsRqDto initJobExecutionsRqDto =
      new InitJobExecutionsRqDto()
        .withFiles(definition.getFileDefinitions()
          .stream()
          .map(fd -> new File().withName(fd.getName()))
          .collect(Collectors.toList()))
        .withUserId(Objects.nonNull(metadata) ? metadata.getCreatedByUserId() : null);

    Future<UploadDefinition> future = Future.future();
    ChangeManagerClient client = new ChangeManagerClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.postChangeManagerJobExecutions(initJobExecutionsRqDto, response -> {
        if (response.statusCode() != HttpStatus.HTTP_CREATED.toInt()) {
          logger.error("Error creating new JobExecution for UploadDefinition with id {}", definition.getId(), response.statusMessage());
          future.fail(new HttpStatusException(response.statusCode(), "Error creating new JobExecution"));
        } else {
          response.bodyHandler(buffer -> {
            JsonObject responseBody = buffer.toJsonObject();
            JsonArray jobExecutions = responseBody.getJsonArray("jobExecutions");
            for (int i = 0; i < jobExecutions.size(); i++) {
              JsonObject jobExecution = jobExecutions.getJsonObject(i);
              String jobExecutionPath = jobExecution.getString("sourcePath");
              definition.getFileDefinitions()
                .forEach(fileDefinition -> {
                  if (jobExecutionPath != null && jobExecutionPath.equals(fileDefinition.getName())) {
                    fileDefinition.setJobExecutionId(jobExecution.getString("id"));
                  }
                });
            }
            definition.setMetaJobExecutionId(responseBody.getString("parentJobExecutionId"));
            future.complete(definition);
          });
        }
      });
    } catch (Exception e) {
      future.fail(e);
    }
    return future;
  }

  private List<FileDefinition> addNewFileDefinition(List<FileDefinition> list, FileDefinition def) {
    if (list == null) {
      list = new ArrayList<>();
    }
    list.add(def
      .withCreateDate(new Date())
      .withStatus(FileDefinition.Status.NEW)
      .withId(UUID.randomUUID().toString()));
    return list;
  }

  private Future<UploadDefinition> checkUploadDefinitionBeforeSave(UploadDefinition definition) {
    Future<UploadDefinition> future = Future.future();
    if (definition.getMetaJobExecutionId() == null || definition.getMetaJobExecutionId().isEmpty()) {
      future.fail(new BadRequestException());
      logger.error("Cant save Upload Definition without MetaJobExecutionId");
      return future;
    }
    for (FileDefinition fileDefinition : definition.getFileDefinitions()) {
      if (fileDefinition.getJobExecutionId() == null || fileDefinition.getJobExecutionId().isEmpty()) {
        logger.error("Cant save File Definition without JobExecutionId");
        future.fail(new BadRequestException());
        return future;
      }
    }
    future.complete(definition);
    return future;
  }

  @Override
  public Future<List<JobExecution>> getJobExecutions(UploadDefinition uploadDefinition, OkapiConnectionParams params) {
    if (!uploadDefinition.getFileDefinitions().isEmpty()) {
      return getJobExecutionById(uploadDefinition.getMetaJobExecutionId(), params)
        .compose(jobExecution -> {
          if (JobExecution.SubordinationType.PARENT_MULTIPLE.equals(jobExecution.getSubordinationType())) {
            return getChildrenJobExecutions(jobExecution.getId(), params)
              .map(JobExecutionCollection::getJobExecutions);
          } else {
            return Future.succeededFuture(Collections.singletonList(jobExecution));
          }
        });
    } else {
      return Future.succeededFuture(Collections.emptyList());
    }
  }

  private Future<JobExecutionCollection> getChildrenJobExecutions(String jobExecutionParentId, OkapiConnectionParams params) {
    Future<JobExecutionCollection> future = Future.future();
    ChangeManagerClient client = new ChangeManagerClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.getChangeManagerJobExecutionsChildrenById(jobExecutionParentId, Integer.MAX_VALUE, null, 0, response -> {
        if (response.statusCode() == HttpStatus.HTTP_OK.toInt()) {
          response.bodyHandler(buffer -> future.complete(buffer.toJsonObject().mapTo(JobExecutionCollection.class)));
        } else {
          String errorMessage = "Error getting children JobExecutions for parent " + jobExecutionParentId;
          logger.error(errorMessage);
          future.fail(errorMessage);
        }
      });
    } catch (Exception e) {
      future.fail(e);
    }
    return future;
  }

  private Future<JobExecution> getJobExecutionById(String jobExecutionId, OkapiConnectionParams params) {
    Future<JobExecution> future = Future.future();
    ChangeManagerClient client = new ChangeManagerClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.getChangeManagerJobExecutionsById(jobExecutionId, null, response -> {
        if (response.statusCode() == HttpStatus.HTTP_OK.toInt()) {
          response.bodyHandler(buffer -> future.complete(buffer.toJsonObject().mapTo(JobExecution.class)));
        } else {
          String errorMessage = "Error getting JobExecution by id " + jobExecutionId;
          logger.error(errorMessage);
          future.fail(errorMessage);
        }
      });
    } catch (Exception e) {
      future.fail(e);
    }
    return future;
  }

  private boolean canDeleteUploadDefinition(List<JobExecution> jobExecutions) {
    return jobExecutions.stream().filter(jobExecution ->
      JobExecution.Status.NEW.equals(jobExecution.getStatus()) ||
        JobExecution.Status.FILE_UPLOADED.equals(jobExecution.getStatus()) ||
        JobExecution.Status.DISCARDED.equals(jobExecution.getStatus()))
      .collect(Collectors.toList()).size() == jobExecutions.size();
  }

  private Future<Boolean> updateJobExecutionStatuses(List<JobExecution> jobExecutions, StatusDto status, OkapiConnectionParams params) {
    List<Future> futures = new ArrayList<>();
    for (JobExecution jobExecution : jobExecutions) {
      futures.add(updateJobExecutionStatus(jobExecution.getId(), status, params));
    }
    return CompositeFuture.all(futures).map(Future::succeeded);
  }

  private Future<Boolean> deleteFiles(List<FileDefinition> fileDefinitions, OkapiConnectionParams params) {
    List<Future> futures = new ArrayList<>();
    for (FileDefinition fileDefinition : fileDefinitions) {
      futures.add(deleteFile(fileDefinition, params));
    }
    return CompositeFuture.all(futures).map(Future::succeeded);
  }

}
