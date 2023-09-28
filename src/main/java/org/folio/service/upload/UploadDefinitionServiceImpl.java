package org.folio.service.upload;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.HttpException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HttpStatus;
import org.folio.dao.UploadDefinitionDao;
import org.folio.dao.UploadDefinitionDaoImpl;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.rest.client.ChangeManagerClient;
import org.folio.rest.impl.util.BufferMapper;
import org.folio.rest.jaxrs.model.Error;
import org.folio.rest.jaxrs.model.*;
import org.folio.service.fileextension.FileExtensionService;
import org.folio.service.fileextension.FileExtensionServiceImpl;
import org.folio.service.storage.FileStorageServiceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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

@Service
public class UploadDefinitionServiceImpl implements UploadDefinitionService {

  private static final Logger LOGGER = LogManager.getLogger();

  private static final String FILE_UPLOAD_ERROR_MESSAGE = "upload.fileSize.invalid";
  private static final String UPLOAD_FILE_EXTENSION_BLOCKED_ERROR_MESSAGE = "validation.uploadDefinition.fileExtension.blocked";

  private Vertx vertx;

  @Autowired
  private UploadDefinitionDao uploadDefinitionDao;

  @Autowired
  private FileExtensionService fileExtensionService;

  /**
   * This constructor is used till {@link org.folio.service.processing.ParallelFileChunkingProcessor}
   * will be rewritten with DI support.
   *
   * @param vertx - Vertx argument
   */
  public UploadDefinitionServiceImpl(Vertx vertx) {
    this.vertx = vertx;

    this.uploadDefinitionDao = new UploadDefinitionDaoImpl(vertx);
    this.fileExtensionService = new FileExtensionServiceImpl(vertx);
  }

  @Override
  public Future<DefinitionCollection> getUploadDefinitions(String query, int offset, int limit, String tenantId) {
    return uploadDefinitionDao.getUploadDefinitions(query, offset, limit, tenantId);
  }

  @Override
  public Future<Optional<UploadDefinition>> getUploadDefinitionById(String id, String tenantId) {
    return uploadDefinitionDao.getUploadDefinitionById(id, tenantId);
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
      .map(def -> uploadDefinitionDao.addUploadDefinition(def, params.getTenantId()))
      .map(uploadDefinition);
  }

  @Override
  public Future<UploadDefinition> updateBlocking(String uploadDefinitionId, UploadDefinitionDaoImpl.UploadDefinitionMutator mutator, String tenantId) {
    return uploadDefinitionDao.updateBlocking(uploadDefinitionId, mutator, tenantId);
  }

  @Override
  public Future<Boolean> deleteUploadDefinition(String id, OkapiConnectionParams params) {
    return getUploadDefinitionById(id, params.getTenantId())
      .compose(optionalUploadDefinition -> optionalUploadDefinition
        .map(uploadDefinition -> getJobExecutions(uploadDefinition, params)
          .compose(jobExecutionList -> {
            if (canDeleteUploadDefinition(jobExecutionList)) {
              return deleteFiles(uploadDefinition.getFileDefinitions(), params)
                .compose(deleted -> uploadDefinitionDao.deleteUploadDefinition(id, params.getTenantId()))
                .compose(result -> {
                  updateJobExecutionStatuses(jobExecutionList, new StatusDto().withStatus(StatusDto.Status.DISCARDED), params)
                    .otherwise(throwable -> {
                      LOGGER.warn("deleteUploadDefinition:: Couldn't update JobExecution status to DISCARDED after UploadDefinition {} was deleted", id, throwable);
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
  public Future<UploadDefinition> addFileDefinitionToUpload(FileDefinition fileDefinition, String tenantId) {
    return uploadDefinitionDao.updateBlocking(fileDefinition.getUploadDefinitionId(), uploadDef -> {
      Promise<UploadDefinition> promise = Promise.promise();
      uploadDef.withFileDefinitions(addNewFileDefinition(uploadDef.getFileDefinitions(), fileDefinition));
      promise.complete(uploadDef);
      return promise.future();
    }, tenantId);
  }

  @Override
  public Future<Boolean> updateJobExecutionStatus(String jobExecutionId, StatusDto status, OkapiConnectionParams params) {
    Promise<Boolean> promise = Promise.promise();
    ChangeManagerClient client = new ChangeManagerClient(params.getOkapiUrl(), params.getTenantId(), params.getToken(), vertx.createHttpClient());
    try {
      client.putChangeManagerJobExecutionsStatusById(jobExecutionId, status, response -> {
        if (response.result().statusCode() == HttpStatus.HTTP_OK.toInt()) {
          promise.complete(true);
        } else {
          LOGGER.warn("updateJobExecutionStatus:: Error updating status of JobExecution with id {}. Status message: {}", jobExecutionId, response.result().statusMessage());
          promise.fail(new HttpException(response.result().statusCode(), "Error updating status of JobExecution"));
        }
      });
    } catch (Exception e) {
      promise.fail(e);
    }
    return promise.future();
  }

  @Override
  public Future<Boolean> deleteFile(FileDefinition fileDefinition, OkapiConnectionParams params) {
    return FileStorageServiceBuilder
      .build(vertx, params.getTenantId(), params)
      .compose(service -> service.deleteFile(fileDefinition));
  }

  @Override
  public Future<Errors> checkNewUploadDefinition(UploadDefinition definition, String tenantId) {
    Errors errors = new Errors()
      .withTotalRecords(0);
    return Future.succeededFuture()
      .map(v -> validateFreeSpace(definition, errors))
      .compose(errorsReply -> validateFilesExtensionName(definition, errors, tenantId));
  }

  private Future<Errors> validateFilesExtensionName(UploadDefinition definition, Errors errors, String tenantId) {
    List<Future<Errors>> listOfValidations = new ArrayList<>(definition.getFileDefinitions().size());
    for (FileDefinition fileDefinition : definition.getFileDefinitions()) {
      String fileExtension = getFileExtensionFromString(fileDefinition.getName());
      listOfValidations.add(fileExtensionService.getFileExtensionByExtenstion(fileExtension, tenantId)
        .map(extension -> {
          if (extension.isPresent() && extension.get().getImportBlocked()) {
            errors.getErrors().add(new Error().withMessage(UPLOAD_FILE_EXTENSION_BLOCKED_ERROR_MESSAGE).withCode(fileDefinition.getName()));
            errors.setTotalRecords(errors.getTotalRecords() + 1);
          }
          return errors;
        }));
    }
    return GenericCompositeFuture.all(listOfValidations)
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
        LOGGER.error(errorMessage, e);
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
        .withUserId(Objects.nonNull(metadata) ? metadata.getCreatedByUserId() : null)
        .withSourceType(InitJobExecutionsRqDto.SourceType.FILES);

    Promise<UploadDefinition> promise = Promise.promise();
    ChangeManagerClient client = new ChangeManagerClient(params.getOkapiUrl(), params.getTenantId(), params.getToken(), vertx.createHttpClient());
    try {
      client.postChangeManagerJobExecutions(initJobExecutionsRqDto, response -> {
        if (response.result().statusCode() != HttpStatus.HTTP_CREATED.toInt()) {
          LOGGER.warn("createJobExecutions:: Error creating new JobExecution for UploadDefinition with id {}. Status message: {}", definition.getId(), response.result().statusMessage());
          promise.fail(new HttpException(response.result().statusCode(), "Error creating new JobExecution"));
        } else {
          JsonObject responseBody = response.result().bodyAsJsonObject();
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
          promise.complete(definition);
        }
      });
    } catch (Exception e) {
      promise.fail(e);
    }
    return promise.future();
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
    Promise<UploadDefinition> promise = Promise.promise();
    if (definition.getMetaJobExecutionId() == null || definition.getMetaJobExecutionId().isEmpty()) {
      promise.fail(new BadRequestException());
      LOGGER.warn("checkUploadDefinitionBeforeSave:: Cant save Upload Definition without MetaJobExecutionId");
      return promise.future();
    }
    for (FileDefinition fileDefinition : definition.getFileDefinitions()) {
      if (fileDefinition.getJobExecutionId() == null || fileDefinition.getJobExecutionId().isEmpty()) {
        LOGGER.warn("checkUploadDefinitionBeforeSave:: Cant save File Definition without JobExecutionId");
        promise.fail(new BadRequestException());
        return promise.future();
      }
    }
    promise.complete(definition);
    return promise.future();
  }

  @Override
  public Future<List<JobExecutionDto>> getJobExecutions(UploadDefinition uploadDefinition, OkapiConnectionParams params) {
    if (!uploadDefinition.getFileDefinitions().isEmpty()) {
      return getJobExecutionById(uploadDefinition.getMetaJobExecutionId(), params)
        .compose(jobExecution -> {
          if (JobExecution.SubordinationType.PARENT_MULTIPLE.equals(jobExecution.getSubordinationType())) {
            return getChildrenJobExecutions(jobExecution.getId(), params)
              .map(JobExecutionDtoCollection::getJobExecutions);
          } else {
            return Future.succeededFuture(Collections.singletonList(convertToJobExecutionDto(jobExecution)));
          }
        });
    } else {
      return Future.succeededFuture(Collections.emptyList());
    }
  }

  private Future<JobExecutionDtoCollection> getChildrenJobExecutions(String jobExecutionParentId, OkapiConnectionParams params) {
    Promise<JobExecutionDtoCollection> promise = Promise.promise();
    ChangeManagerClient client = new ChangeManagerClient(params.getOkapiUrl(), params.getTenantId(), params.getToken(), vertx.createHttpClient());
    try {
      client.getChangeManagerJobExecutionsChildrenById(jobExecutionParentId, Integer.MAX_VALUE, 0, response -> {
        if (response.result().statusCode() == HttpStatus.HTTP_OK.toInt()) {
          Buffer responseAsBuffer = response.result().bodyAsBuffer();
          promise.handle(BufferMapper.mapBufferContentToEntityAsync(responseAsBuffer, JobExecutionDtoCollection.class));
        } else {
          String errorMessage = "Error getting children JobExecutions for parent " + jobExecutionParentId;
          LOGGER.warn(errorMessage);
          promise.fail(errorMessage);
        }
      });
    } catch (Exception e) {
      promise.fail(e);
    }
    return promise.future();
  }

  @Override
  public Future<JobExecution> getJobExecutionById(String jobExecutionId, OkapiConnectionParams params) {
    Promise<JobExecution> promise = Promise.promise();
    ChangeManagerClient client = new ChangeManagerClient(params.getOkapiUrl(), params.getTenantId(), params.getToken(), vertx.createHttpClient());
    try {
      client.getChangeManagerJobExecutionsById(jobExecutionId, null, response -> {
        if (response.result().statusCode() == HttpStatus.HTTP_OK.toInt()) {
          Buffer responseAsBuffer = response.result().bodyAsBuffer();
          promise.handle(BufferMapper.mapBufferContentToEntityAsync(responseAsBuffer, JobExecution.class));
        } else {
          String errorMessage = "Error getting JobExecution by id " + jobExecutionId;
          LOGGER.warn(errorMessage);
          promise.fail(errorMessage);
        }
      });
    } catch (Exception e) {
      promise.fail(e);
    }
    return promise.future();
  }

  @Override
  public Future<UploadDefinition> updateFileDefinitionStatus(String uploadDefinitionId, String fileDefinitionId, FileDefinition.Status status, String tenantId) {
    return uploadDefinitionDao.updateBlocking(uploadDefinitionId, uploadDefinition -> {
      uploadDefinition.getFileDefinitions()
        .stream()
        .filter(fileDefinition -> fileDefinition.getId().equals(fileDefinitionId))
        .findFirst()
        .orElseThrow(() -> new NotFoundException(String.format("FileDefinition with id '%s' was not found", fileDefinitionId)))
        .withStatus(status);
      return Future.succeededFuture(uploadDefinition);
    }, tenantId);
  }

  @Override
  public Future<UploadDefinition> updateUploadDefinitionStatus(String uploadDefinitionId, UploadDefinition.Status status, String tenantId) {
    return uploadDefinitionDao
      .updateBlocking(uploadDefinitionId, definition -> Future.succeededFuture(definition.withStatus(status)), tenantId);
  }

  private boolean canDeleteUploadDefinition(List<JobExecutionDto> jobExecutions) {
    return jobExecutions.stream().filter(jobExecution ->
      JobExecutionDto.Status.NEW.equals(jobExecution.getStatus()) ||
        JobExecutionDto.Status.FILE_UPLOADED.equals(jobExecution.getStatus()) ||
        JobExecutionDto.Status.DISCARDED.equals(jobExecution.getStatus())).count() == jobExecutions.size();
  }

  private Future<Boolean> updateJobExecutionStatuses(List<JobExecutionDto> jobExecutions, StatusDto status, OkapiConnectionParams params) {
    List<Future<Boolean>> futures = new ArrayList<>();
    for (JobExecutionDto jobExecution : jobExecutions) {
      futures.add(updateJobExecutionStatus(jobExecution.getId(), status, params));
    }
    return GenericCompositeFuture.all(futures).map(Future::succeeded);
  }

  private Future<Boolean> deleteFiles(List<FileDefinition> fileDefinitions, OkapiConnectionParams params) {
    List<Future<Boolean>> futures = new ArrayList<>();
    for (FileDefinition fileDefinition : fileDefinitions) {
      futures.add(deleteFile(fileDefinition, params));
    }
    return GenericCompositeFuture.all(futures).map(Future::succeeded);
  }

  private JobExecutionDto convertToJobExecutionDto(JobExecution jobExecution) {
    JobExecutionDto result = new JobExecutionDto();
    result.setId(jobExecution.getId());
    result.setHrId(jobExecution.getHrId());
    result.setParentJobId(jobExecution.getParentJobId());
    result.setSubordinationType(Optional.ofNullable(jobExecution.getSubordinationType())
     .map(type -> JobExecutionDto.SubordinationType.fromValue(type.value())).orElse(null));
    result.setJobProfileInfo(jobExecution.getJobProfileInfo());
    result.setSourcePath(jobExecution.getSourcePath());
    result.setFileName(jobExecution.getFileName());
    result.setRunBy(jobExecution.getRunBy());
    result.setProgress(jobExecution.getProgress());
    result.setStartedDate(jobExecution.getStartedDate());
    result.setCompletedDate(jobExecution.getCompletedDate());
    result.setStatus(Optional.ofNullable(jobExecution.getStatus())
      .map(status -> JobExecutionDto.Status.fromValue(status.value())).orElse(null));
    result.setUiStatus(Optional.ofNullable(jobExecution.getUiStatus())
      .map(uiStatus -> JobExecutionDto.UiStatus.fromValue(uiStatus.value())).orElse(null));
    result.setErrorStatus(Optional.ofNullable(jobExecution.getErrorStatus())
      .map(errorStatus -> JobExecutionDto.ErrorStatus.fromValue(errorStatus.value())).orElse(null));
    result.setUserId(jobExecution.getUserId());

    return result;
  }
}
