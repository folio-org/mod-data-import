package org.folio.service.storage;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.FileDefinition;

import javax.ws.rs.BadRequestException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class LocalFileStorageService extends AbstractFileStorageService {

  private static final Logger LOGGER = LogManager.getLogger();

  private static final String FILE_STORAGE_PATH_CODE = "data.import.storage.path";

  public LocalFileStorageService(Vertx vertx, String tenantId) {
    super(vertx, tenantId);
  }

  @Override
  public String getServiceName() {
    return "LOCAL_STORAGE";
  }

  @Override
  public Future<FileDefinition> saveFile(byte[] data, FileDefinition fileDefinition, OkapiConnectionParams params) {
    Promise<FileDefinition> promise = Promise.promise();
    String fileId = fileDefinition.getId();
    getStoragePath(FILE_STORAGE_PATH_CODE, fileDefinition, params)
      .onComplete(pathReply -> {
        if (pathReply.succeeded()) {
          String path = pathReply.result();
          vertx.<Void>executeBlocking(b -> {
              try {
                if (!fs.existsBlocking(path)) {
                  fs.mkdirsBlocking(path.substring(0, path.indexOf(fileDefinition.getName()) - 1));
                }
                final Path pathToFile = Paths.get(path);
                Files.write(pathToFile, data, pathToFile.toFile().exists() ? StandardOpenOption.APPEND : StandardOpenOption.CREATE);
                fileDefinition.setSourcePath(path);
                b.complete();
              } catch (Exception e) {
                LOGGER.warn("saveFile:: Error during save file source data to the local system's storage. FileId: {}", fileId, e);
                b.fail(e);
              }
            },
            r -> {
              if (r.failed()) {
                LOGGER.warn("saveFile:: Error during calculating path for file save. FileId: {}", fileId, r.cause());
                promise.fail(r.cause());
              } else {
                LOGGER.warn("saveFile:: File part was saved to the storage. FileId: {}", fileId);
                promise.complete(fileDefinition);
              }
            });
        } else {
          LOGGER.warn("saveFile:: Error during calculating path for file save. FileId: {}", fileId, pathReply.cause());
          promise.fail(new BadRequestException(pathReply.cause()));
        }
      });
    return promise.future();
  }

  @Override
  public Future<Boolean> deleteFile(FileDefinition fileDefinition) {
    Promise<Boolean> promise = Promise.promise();
    try {
      String filePath = fileDefinition.getSourcePath();
      if (StringUtils.isNotBlank(filePath) && fs.existsBlocking(filePath)) {
        fs.deleteBlocking(filePath);
        promise.complete(true);
      } else {
        LOGGER.trace("deleteFile:: Couldn't detect the file with id {} in the storage", fileDefinition.getId());
        promise.complete(false);
      }
    } catch (Exception e) {
      LOGGER.warn("deleteFile:: Couldn't delete the file with id {} from the storage", fileDefinition.getId(), e);
      promise.complete(false);
    }
    return promise.future();
  }

  @Override
  protected Future<String> getStoragePath(String code, FileDefinition fileDefinition, OkapiConnectionParams params) {
    return fileDefinition.getSourcePath() != null ?
      Future.succeededFuture(fileDefinition.getSourcePath())
      : super.getStoragePath(code, fileDefinition, params)
      .compose(path -> Future.succeededFuture(path + "/" + fileDefinition.getName()));
  }
}
