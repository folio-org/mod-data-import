package org.folio.service.upload;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.folio.dao.UploadDefinitionDao;
import org.folio.dao.UploadDefinitionDaoImpl;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.UploadDefinition;

import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;


public class UploadDefinitionServiceImpl implements UploadDefinitionService {

  private Vertx vertx;
  private UploadDefinitionDao uploadDefinitionDao;

  public UploadDefinitionServiceImpl(UploadDefinitionDao uploadDefinitionDao) {
    this.uploadDefinitionDao = uploadDefinitionDao;
  }

  public UploadDefinitionServiceImpl(Vertx vertx, String tenantId) {
    this.vertx = vertx;
    uploadDefinitionDao = new UploadDefinitionDaoImpl(vertx, tenantId);
  }

  public Future<List<UploadDefinition>> getUploadDefinitions(String query, int offset, int limit) {
    return uploadDefinitionDao.getUploadDefinitions(query, offset, limit);
  }

  @Override
  public Future<Optional<UploadDefinition>> getUploadDefinitionById(String id) {
    return uploadDefinitionDao.getUploadDefinitionById(id);
  }

  @Override
  public Future<UploadDefinition> addUploadDefinition(UploadDefinition uploadDefinition) {
    uploadDefinition.setId(UUID.randomUUID().toString());
    uploadDefinition.setStatus(UploadDefinition.Status.NEW);
    //NEED interact with source-record-manager and create job execution
    uploadDefinition.setMetaJobExecutionId(UUID.randomUUID().toString());
    uploadDefinition.setCreateDate(new Date());
    uploadDefinition.getFileDefinitions().forEach(fileDefinition -> fileDefinition.withId(UUID.randomUUID().toString())
      .withCreateDate(new Date())
      .withLoaded(false)
      //NEED interact with source-record-manager and create job execution
      .withJobExecutionId(UUID.randomUUID().toString())
      .withUploadDefinitionId(uploadDefinition.getId()));
    return uploadDefinitionDao.addUploadDefinition(uploadDefinition)
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
}
