package org.folio.service;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.folio.dao.FileDao;
import org.folio.dao.FileDaoImpl;
import org.folio.rest.jaxrs.model.DataImportUploadFileFileIdPostMultipartFormData;
import org.folio.rest.jaxrs.model.File;

import javax.ws.rs.NotFoundException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;


public class FileServiceImpl implements FileService {

  private Vertx vertx;
  private FileDao fileDao;

  public FileServiceImpl(FileDao fileDao) {
    this.fileDao = fileDao;
  }

  public FileServiceImpl(Vertx vertx, String tenantId) {
    this.vertx = vertx;
    fileDao = new FileDaoImpl(vertx, tenantId);
  }

  public Future<List<File>> getFiles(String query, int offset, int limit) {
    return fileDao.getFiles(query, offset, limit);
  }

  @Override
  public Future<Optional<File>> getFileById(String id) {
    return fileDao.getFileById(id);
  }

  @Override
  public Future<List<File>> getFileByUploadDefinitionId(String id) {
    return fileDao.getFileByUploadDefinitionId(id);
  }

  @Override
  public Future<String> addFile(File file) {
    file.setId(UUID.randomUUID().toString());
    return fileDao.addFile(file);
  }

  @Override
  public Future<String> uploadFile(String fileId, DataImportUploadFileFileIdPostMultipartFormData data) {
//    fileDao.getFileByUploadDefinitionId(file.getUploadDefinitionId()).map(files -> files.);
    return Future.succeededFuture(fileId);
  }

  @Override
  public Future<Boolean> updateFile(File file) {
    return getFileById(file.getId())
      .compose(optionalFile -> optionalFile
        .map(t -> fileDao.updateFile(file))
        .orElse(Future.failedFuture(new NotFoundException(
          String.format("File with id '%s' not found", file.getId()))))
      );
  }

  @Override
  public Future<Boolean> deleteFile(String id) {
    return fileDao.deleteFile(id);
  }

}
