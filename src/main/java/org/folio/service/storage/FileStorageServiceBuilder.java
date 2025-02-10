package org.folio.service.storage;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Async file storage service builder builder
 */
public class FileStorageServiceBuilder {

  private static final Logger LOGGER = LogManager.getLogger();

  private FileStorageServiceBuilder() {
  }

  /**
   * build a default LocalStorage Service
   *
   * @param vertx    - vertx object
   * @param tenantId - current tenant id
   * @return - new Service object
   */
  public static Future<FileStorageService> build(Vertx vertx, String tenantId) {
    LOGGER.info("build:: building LocalFileStorageService");
    return Future.succeededFuture(new LocalFileStorageService(vertx, tenantId));
  }

}
