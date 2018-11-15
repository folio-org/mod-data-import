package org.folio.service.storage;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import org.apache.commons.io.IOUtils;
import org.folio.rest.jaxrs.model.FileDefinition;

import javax.ws.rs.BadRequestException;
import java.io.InputStream;
import java.util.Map;

public class LocalFileStorageService extends AbstractFileStorageService {

  public static final String FILE_STORAGE_PATH_CODE = "data.import.storage.path";

  public LocalFileStorageService(Vertx vertx, String tenantId) {
    super(vertx, tenantId);
  }

  @Override
  public String getServiceName() {
    return "LOCAL_STORAGE";
  }

  @Override
  public Future<FileDefinition> saveFile(InputStream data, FileDefinition fileDefinition, Map<String, String> okapiHeaders) {
    Future<FileDefinition> future = Future.future();
    getStoragePath(FILE_STORAGE_PATH_CODE, fileDefinition.getId(), okapiHeaders)
      .setHandler(pathReply -> {
        if (pathReply.succeeded()) {
          try {
            String path = pathReply.result();
            fs.mkdirsBlocking(path);
            fs.writeFileBlocking(path + "/" + fileDefinition.getName(), Buffer.buffer(IOUtils.toByteArray(data)));
            fileDefinition.setSourcePath(path + "/" + fileDefinition.getName());
            fileDefinition.setLoaded(true);
            future.complete(fileDefinition);
          } catch (Exception e) {
            future.fail(e);
          }
        } else {
          future.fail(new BadRequestException());
        }
      });
    return future;
  }

}
