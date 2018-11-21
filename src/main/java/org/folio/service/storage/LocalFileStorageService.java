package org.folio.service.storage;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.util.OkapiConnectionParams;

import javax.ws.rs.BadRequestException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Scanner;

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
          try {
            String path = pathReply.result();
            fs.mkdirsBlocking(path);
            String pathToFile = path + "/" + fileDefinition.getName();
            Scanner scanner = new Scanner(data);
            AsyncFile file = null;
            while (scanner.hasNext()) {
              if (file == null) {
                fs.writeFileBlocking(pathToFile, Buffer.buffer(scanner.next()));
                file = fs.openBlocking(pathToFile, new OpenOptions().setAppend(true));
              } else {
                file.write(Buffer.buffer(scanner.next()));
              }
            }
            Objects.requireNonNull(file).close();
            fileDefinition.setSourcePath(path + "/" + fileDefinition.getName());
            fileDefinition.setLoaded(true);
            future.complete(fileDefinition);
          } catch (Exception e) {
            logger.error("Error during save file source data to the local system's storage. FileId: " + fileId, e);
            future.fail(e);
          }
        } else {
          logger.error("Error during calculating path for file save. FileId: " + fileId);
          future.fail(new BadRequestException());
        }
      });
    return future;
  }

}
