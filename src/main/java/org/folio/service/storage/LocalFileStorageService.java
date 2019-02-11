package org.folio.service.storage;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.FileDefinition;

import javax.ws.rs.BadRequestException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class LocalFileStorageService extends AbstractFileStorageService {

  private static final String FILE_STORAGE_PATH_CODE = "data.import.storage.path";
  private static final Logger logger = LoggerFactory.getLogger(LocalFileStorageService.class);
  private String storagePath;


  public LocalFileStorageService(Vertx vertx, String tenantId) {
    super(vertx, tenantId);
  }

  @Override
  public String getServiceName() {
    return "LOCAL_STORAGE";
  }

  @Override
  public Future<FileDefinition> saveFile(byte[] data, FileDefinition fileDefinition, OkapiConnectionParams params) {
    Future<FileDefinition> future = Future.future();
    String fileId = fileDefinition.getId();
    getStoragePath(FILE_STORAGE_PATH_CODE, fileDefinition, params)
      .setHandler(pathReply -> {
        if (pathReply.succeeded()) {
          String path = pathReply.result();
          vertx.<Void>executeBlocking(b -> {
              try {
                if (!fs.existsBlocking(path)) {
                  fs.mkdirsBlocking(path.split(fileDefinition.getName())[0]);
                }
                final Path pathToFile = Paths.get(path);
                Files.write(pathToFile, data, pathToFile.toFile().exists() ? StandardOpenOption.APPEND : StandardOpenOption.CREATE);
                fileDefinition.setSourcePath(path);
                b.complete();
              } catch (Exception e) {
                logger.error("Error during save file source data to the local system's storage. FileId: {}", fileId, e);
                b.fail(e);
              }
            },
            r -> {
              if (r.failed()) {
                logger.error("Error during calculating path for file save. FileId: {}", fileId);
                future.fail(r.cause());
              } else {
                logger.info("File part was saved to the storage.");
                future.complete(fileDefinition);
              }
            });
        } else {
          logger.error("Error during calculating path for file save. FileId: {}", fileId);
          future.fail(new BadRequestException());
        }
      });
    return future;
  }

  @Override
  public Future<Boolean> deleteFile(FileDefinition fileDefinition) {
    Future<Boolean> future = Future.future();
    try {
      fs.deleteBlocking(fileDefinition.getSourcePath());
      future.complete(true);
    } catch (Exception e) {
      logger.error("Couldn't delete the file with id {} from the storage", fileDefinition.getId(), e);
      future.complete(false);
    }
    return future;
  }

  @Override
  protected Future<String> getStoragePath(String code, FileDefinition fileDefinition, OkapiConnectionParams params) {
    return fileDefinition.getSourcePath() != null ?
      Future.succeededFuture(fileDefinition.getSourcePath())
      : super.getStoragePath(code, fileDefinition, params)
        .compose(path -> Future.succeededFuture(path + "/" + fileDefinition.getName()));
  }
}
