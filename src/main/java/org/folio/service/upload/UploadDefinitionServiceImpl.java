package org.folio.service.upload;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.io.FileSystemUtils;
import org.folio.dao.UploadDefinitionDao;
import org.folio.dao.UploadDefinitionDaoImpl;
import org.folio.dataImport.util.OkapiConnectionParams;
import org.folio.dataImport.util.RestUtil;
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
import org.folio.rest.jaxrs.resource.DataImport;
import org.folio.service.storage.FileStorageServiceBuilder;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static javax.ws.rs.core.Response.Status.CREATED;
import static org.folio.dataImport.util.RestUtil.validateAsyncResult;

public class UploadDefinitionServiceImpl implements UploadDefinitionService {

  private static final Logger logger = LoggerFactory.getLogger(UploadDefinitionServiceImpl.class);
  private static final String JOB_EXECUTION_CREATE_URL = "/change-manager/jobExecutions";
  private static final String JOB_EXECUTION_URL = "/change-manager/jobExecution";
  private static final String FILE_UPLOAD_ERROR_MESSAGE = "upload.fileSize.invalid";

  private Vertx vertx;
  private String tenantId;
  private UploadDefinitionDao uploadDefinitionDao;

  public UploadDefinitionServiceImpl(UploadDefinitionDao uploadDefinitionDao) {
    this.uploadDefinitionDao = uploadDefinitionDao;
  }

  public UploadDefinitionServiceImpl(Vertx vertx, String tenantId) {
    this.vertx = vertx;
    this.tenantId = tenantId;
    uploadDefinitionDao = new UploadDefinitionDaoImpl(vertx, tenantId);
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
  public Future<UploadDefinition> updateUploadDefinition(UploadDefinition uploadDefinition) {
    return getUploadDefinitionById(uploadDefinition.getId())
      .compose(optionalUploadDefinition -> optionalUploadDefinition
        .map(t -> uploadDefinitionDao.updateUploadDefinition(uploadDefinition)
          .map(uploadDefinition))
        .orElse(Future.failedFuture(new NotFoundException(
          String.format("UploadDefinition with id '%s' not found", uploadDefinition.getId()))))
      );
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
              return uploadDefinitionDao.deleteUploadDefinition(id)
                .compose(deleted -> deleteFiles(uploadDefinition.getFileDefinitions(), params))
                .compose(deleted ->
                  updateJobExecutionStatuses(jobExecutionList, new StatusDto().withStatus(StatusDto.Status.DISCARDED), params));
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
    return getUploadDefinitionById(fileDefinition.getUploadDefinitionId())
      .compose(optionalUploadDefinition -> optionalUploadDefinition
        .map(uploadDefinition -> uploadDefinitionDao.updateUploadDefinition(uploadDefinition
          .withFileDefinitions(addNewFileDefinition(uploadDefinition.getFileDefinitions(), fileDefinition)))
          .map(uploadDefinition))
        .orElse(Future.failedFuture(new NotFoundException(
          String.format("UploadDefinition with id '%s' not found", fileDefinition.getUploadDefinitionId()))))
      );
  }

  @Override
  public Future<Boolean> updateJobExecutionStatus(String jobExecutionId, StatusDto status, OkapiConnectionParams params) {
    Future<Boolean> future = Future.future();
    RestUtil.doRequest(params, JOB_EXECUTION_URL + "/" + jobExecutionId + "/status", HttpMethod.PUT, status)
      .setHandler(updateResult -> {
        if (validateAsyncResult(updateResult, future)) {
          future.complete(true);
        }
      });
    return future;
  }

  @Override
  public Future<Boolean> deleteFile(FileDefinition fileDefinition, OkapiConnectionParams params) {
    return FileStorageServiceBuilder
      .build(vertx, tenantId, params)
      .compose(service -> service.deleteFile(fileDefinition));
  }

  @Override
  public Boolean checkNewUploadDefinition(UploadDefinition definition, Handler<AsyncResult<Response>> asyncResultHandler) {
    Set<Error> errorsList = new HashSet<>(definition.getFileDefinitions().size());
    boolean isValid = validateFreeSpace(definition.getFileDefinitions(), errorsList)
      && validateHeapSpace(definition.getFileDefinitions(), errorsList);
    if (!isValid) {
      asyncResultHandler.handle(Future.succeededFuture(DataImport.PostDataImportUploadDefinitionResponse
        .respond422WithApplicationJson(new Errors()
          .withErrors(new ArrayList<>(errorsList))
          .withTotalRecords(errorsList.size()))));
    }
    return isValid;
  }

  private boolean validateFreeSpace(List<FileDefinition> fileDefinitions, Collection<Error> errors) {
    boolean valid = true;
    for (FileDefinition fileDefinition : fileDefinitions) {
      try {
        if (fileDefinition.getSize() != null && FileSystemUtils.freeSpaceKb() - fileDefinition.getSize() <= 0) { //NOSONAR
          valid = false;
          errors.add(new Error()
            .withMessage(FILE_UPLOAD_ERROR_MESSAGE)
            .withCode(fileDefinition.getName()));
        }
      } catch (IOException e) {
        throw new InternalServerErrorException("Error during check file's size");
      }
    }
    return valid;
  }

  private boolean validateHeapSpace(List<FileDefinition> fileDefinitions, Collection<Error> errors) {
    boolean valid = true;
    for (FileDefinition fileDefinition : fileDefinitions) {
      if (fileDefinition.getSize() != null && (Runtime.getRuntime().maxMemory() / 1024) - fileDefinition.getSize() <= 0) {
        valid = false;
        errors.add(new Error()
          .withMessage(FILE_UPLOAD_ERROR_MESSAGE)
          .withCode(fileDefinition.getName()));
      }
    }
    return valid;
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

    RestUtil.doRequest(params, JOB_EXECUTION_CREATE_URL, HttpMethod.POST, initJobExecutionsRqDto)
      .setHandler(responseResult -> {
        try {
          int responseCode = responseResult.result().getCode();
          if (responseResult.failed()) {
            logger.error("Error during request new jobExecution. Response code: {}", responseCode, responseResult.cause());
            future.fail(responseResult.cause());
          } else if (responseCode != CREATED.getStatusCode()) {
            JsonObject response = responseResult.result().getJson();
            future.fail(new IllegalArgumentException(Objects.nonNull(response) ? response.encode() : "HTTP Response: " + responseCode));
          } else {
            JsonObject responseBody = responseResult.result().getJson();
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
          }
        } catch (Exception e) {
          logger.error("Error during creating jobExecutions for files", e);
          future.fail(e);
        }
      });
    return future;
  }

  private List<FileDefinition> addNewFileDefinition(List<FileDefinition> list, FileDefinition def) {
    if (list == null) {
      list = new ArrayList<>();
    }
    list.add(def
      .withCreateDate(new Date())
      .withStatus(FileDefinition.Status.NEW)
      .withId(UUID.randomUUID().toString())
      //NEED replace with rest call
      .withJobExecutionId(UUID.randomUUID().toString()));
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

  private Future<List<JobExecution>> getJobExecutions(UploadDefinition uploadDefinition, OkapiConnectionParams params) {
    if (!uploadDefinition.getFileDefinitions().isEmpty()) {
      return getJobExecutionById(uploadDefinition.getMetaJobExecutionId(), params)
        .compose(jobExecution -> {
          if (JobExecution.SubordinationType.PARENT_MULTIPLE.equals(jobExecution.getSubordinationType())) {
            return getChildrenJobExecutions(jobExecution.getId(), params)
              .map(JobExecutionCollection::getJobExecutions);
          } else {
            return Future.succeededFuture(Arrays.asList(jobExecution));
          }
        });
    } else {
      return Future.succeededFuture(Collections.emptyList());
    }
  }

  private Future<JobExecutionCollection> getChildrenJobExecutions(String jobExecutionParentId, OkapiConnectionParams params) {
    Future<JobExecutionCollection> future = Future.future();
    String url = JOB_EXECUTION_URL + "/" + jobExecutionParentId + "/children";
    RestUtil.doRequest(params, url, HttpMethod.GET, null).setHandler(getResult -> {
      if (validateAsyncResult(getResult, future)) {
        JobExecutionCollection jobExecutionCollection = getResult.result().getJson().mapTo(JobExecutionCollection.class);
        future.complete(jobExecutionCollection);
      } else {
        String errorMessage = "Error getting children JobExecutions for parent " + jobExecutionParentId;
        logger.error(errorMessage);
        future.fail(errorMessage);
      }
    });
    return future;
  }

  private Future<JobExecution> getJobExecutionById(String jobExecutionId, OkapiConnectionParams params) {
    Future<JobExecution> future = Future.future();
    String url = JOB_EXECUTION_URL + "/" + jobExecutionId;
    RestUtil.doRequest(params, url, HttpMethod.GET, null).setHandler(getResult -> {
      if (validateAsyncResult(getResult, future)) {
        JobExecution jobExecution = getResult.result().getJson().mapTo(JobExecution.class);
        future.complete(jobExecution);
      } else {
        String errorMessage = "Error getting JobExecution by id " + jobExecutionId;
        future.fail(errorMessage);
      }
    });
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
