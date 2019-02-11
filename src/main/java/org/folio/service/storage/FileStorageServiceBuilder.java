package org.folio.service.storage;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dataimport.util.ConfigurationUtil;
import org.folio.dataimport.util.OkapiConnectionParams;

import java.util.Collections;
import java.util.List;

/**
 * Async file storage service builder builder
 */
public class FileStorageServiceBuilder {

  private static final String SERVICE_STORAGE_PROPERTY_CODE = "data.import.storage.type";
  private static final Logger logger = LoggerFactory.getLogger(FileStorageServiceBuilder.class);

  private FileStorageServiceBuilder() {
  }

  /**
   * Build a service object by mod-configuration's values. If there are no properties build a default LocalStorage Service
   *
   * @param vertx    - vertx object
   * @param tenantId - current tenant id
   * @param params   - Wrapper for Okapi connection params
   * @return - new Service object
   */
  public static Future<FileStorageService> build(Vertx vertx, String tenantId, OkapiConnectionParams params) {
    Future<FileStorageService> future = Future.future();
    ConfigurationUtil.getPropertyByCode(SERVICE_STORAGE_PROPERTY_CODE, params).setHandler(result -> {
      if (result.failed() || result.result() == null || result.result().isEmpty()) {
        logger.warn("Request to mod-configuration was failed or property for lookup service is not define. Try to use default Local Storage!");
        future.complete(new LocalFileStorageService(vertx, tenantId));
        return;
      }
      String serviceCode = result.result();
      List<FileStorageService> availableServices = Collections.singletonList(new LocalFileStorageService(vertx, tenantId));
      future.complete(availableServices
        .stream()
        .filter(service -> service.getServiceName().equals(serviceCode))
        .findFirst()
        .orElse(new LocalFileStorageService(vertx, tenantId))
      );
    });
    return future;
  }

}
