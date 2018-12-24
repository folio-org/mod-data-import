package org.folio.service.upload;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dao.UploadDefinitionDao;
import org.folio.dao.UploadDefinitionDaoImpl;
import org.folio.rest.jaxrs.model.DefinitionCollection;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.dataImport.util.OkapiConnectionParams;
import org.folio.dataImport.util.RestUtil;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static javax.ws.rs.core.Response.Status.CREATED;


public class UploadDefinitionServiceImpl implements UploadDefinitionService {

  private static final String JOB_EXECUTION_CREATE_URL = "/change-manager/jobExecutions";
  private static final Logger logger = LoggerFactory.getLogger(UploadDefinitionServiceImpl.class);

  private Vertx vertx;
  private UploadDefinitionDao uploadDefinitionDao;

  public UploadDefinitionServiceImpl(UploadDefinitionDao uploadDefinitionDao) {
    this.uploadDefinitionDao = uploadDefinitionDao;
  }

  public UploadDefinitionServiceImpl(Vertx vertx, String tenantId) {
    this.vertx = vertx;
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
      .withLoaded(false)
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
  public Future<Boolean> deleteUploadDefinition(String id) {
    return uploadDefinitionDao.deleteUploadDefinition(id);
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
            logger.error("Error during request new jobExecution. Response code: " + responseCode, responseResult.cause());
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
      .withLoaded(false)
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
}
