package org.folio.service.storage;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.io.IOUtils;
import org.folio.dataImport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.FileDefinition;

import javax.ws.rs.BadRequestException;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class LocalFileStorageService extends AbstractFileStorageService {

  private static final String FILE_STORAGE_PATH_CODE = "data.import.storage.path";
  private static final Logger logger = LoggerFactory.getLogger(LocalFileStorageService.class);


  public LocalFileStorageService(Vertx vertx, String tenantId) {
    super(vertx, tenantId);
  }

  @Override
  public String getServiceName() {
    return "LOCAL_STORAGE";
  }

  @Override
  public Future<FileDefinition> saveFile(InputStream data, FileDefinition fileDefinition, OkapiConnectionParams params) {
    Future<FileDefinition> future = Future.future();
    String fileId = fileDefinition.getId();
    getStoragePath(FILE_STORAGE_PATH_CODE, fileDefinition.getId(), params)
      .setHandler(pathReply -> {
        if (pathReply.succeeded()) {
          String path = pathReply.result();
          vertx.<Long>executeBlocking(b -> {
              try {
                fs.mkdirsBlocking(path);
                String pathToFile = path + "/" + fileDefinition.getName();
                File targetFile = new File(pathToFile);
                Long bytes = Files.copy(
                  data,
                  targetFile.toPath(),
                  StandardCopyOption.REPLACE_EXISTING);
                IOUtils.closeQuietly(data);
                fileDefinition.setSourcePath(path + "/" + fileDefinition.getName());
                fileDefinition.setLoaded(true);
                b.complete(bytes);
              } catch (Exception e) {
                logger.error("Error during save file source data to the local system's storage. FileId: " + fileId, e);
                b.fail(e);
              }
            },
            r -> {
              if (r.failed()) {
                logger.error("Error during calculating path for file save. FileId: " + fileId);
                future.fail(r.cause());
              } else {
                logger.info("File was saved to the storage. File size " + (r.result() / 1024) / 1024 + " mb");
                future.complete(fileDefinition);
              }
            });
        } else {
          logger.error("Error during calculating path for file save. FileId: " + fileId);
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
      logger.error(String.format("Couldn't delete the file with id %s from the storage", fileDefinition.getId()), e);
      future.complete(false);
    }
    return future;
  }

}
