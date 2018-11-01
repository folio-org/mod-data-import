package org.folio.service;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.folio.dao.UploadDefinitionDao;
import org.folio.dao.UploadDefinitionDaoImpl;
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
  private FileService fileService;

  public UploadDefinitionServiceImpl(UploadDefinitionDao uploadDefinitionDao) {
    this.uploadDefinitionDao = uploadDefinitionDao;
  }

  public UploadDefinitionServiceImpl(Vertx vertx, String tenantId) {
    this.vertx = vertx;
    uploadDefinitionDao = new UploadDefinitionDaoImpl(vertx, tenantId);
    fileService = new FileServiceImpl(vertx, tenantId);
  }

  public Future<List<UploadDefinition>> getUploadDefinitions(String query, int offset, int limit) {
    return uploadDefinitionDao.getUploadDefinitions(query, offset, limit);
  }

  @Override
  public Future<Optional<UploadDefinition>> getUploadDefinitionById(String id) {
    return uploadDefinitionDao.getUploadDefinitionById(id);
  }

  @Override
  public Future<String> addUploadDefinition(UploadDefinition uploadDefinition) {
    uploadDefinition.setId(UUID.randomUUID().toString());
    uploadDefinition.setStatus(UploadDefinition.Status.NEW);
    //TODO interact with source-record-manager and create job execution
    uploadDefinition.setMetaJobExecutionId(UUID.randomUUID().toString());
    uploadDefinition.setCreateDate(new Date());
    uploadDefinition.getFiles().forEach(f -> {
      f.withCreateDate(new Date())
        .withLoaded(false)
        //TODO interact with source-record-manager and create job execution
        .withJobExecutionId(UUID.randomUUID().toString())
        .withUploadDefinitionId(uploadDefinition.getId());
      fileService.addFile(f);
    });
    uploadDefinition.getFiles().clear();
    return uploadDefinitionDao.addUploadDefinition(uploadDefinition);
  }

  @Override
  public Future<Boolean> updateUploadDefinition(UploadDefinition uploadDefinition) {
    return getUploadDefinitionById(uploadDefinition.getId())
      .compose(optionalUploadDefinition -> optionalUploadDefinition
        .map(t -> uploadDefinitionDao.updateUploadDefinition(uploadDefinition))
        .orElse(Future.failedFuture(new NotFoundException(
          String.format("UploadDefinition with id '%s' not found", uploadDefinition.getId()))))
      );
  }

  @Override
  public Future<Boolean> deleteUploadDefinition(String id) {
    return uploadDefinitionDao.deleteUploadDefinition(id);
  }

  private Future<UploadDefinition> fetchFiles(UploadDefinition definition) {
    return fileService.getFileByUploadDefinitionId(definition.getId()).map(definition::withFiles);
  }

}
