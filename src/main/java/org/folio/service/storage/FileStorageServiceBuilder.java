package org.folio.service.storage;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.ConfigurationUtil;
import org.folio.dataimport.util.OkapiConnectionParams;

import java.util.Collections;
import java.util.List;

/**
 * Async file storage service builder builder
 */
public class FileStorageServiceBuilder {

  private static final Logger LOGGER = LogManager.getLogger();

  private static final String SERVICE_STORAGE_PROPERTY_CODE = "data.import.storage.type";

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
    Promise<FileStorageService> promise = Promise.promise();
    ConfigurationUtil.getPropertyByCode(SERVICE_STORAGE_PROPERTY_CODE, params).onComplete(result -> {
      if (result.failed() || result.result() == null || result.result().isEmpty()) {
        LOGGER.warn("Request to mod-configuration was failed or property for lookup service is not define. Try to use default Local Storage!");
        promise.complete(new LocalFileStorageService(vertx, tenantId));
        return;
      }
      String serviceCode = result.result();
      List<FileStorageService> availableServices = Collections.singletonList(new LocalFileStorageService(vertx, tenantId));
      promise.complete(availableServices
        .stream()
        .filter(service -> service.getServiceName().equals(serviceCode))
        .findFirst()
        .orElse(new LocalFileStorageService(vertx, tenantId))
      );
    });
    return promise.future();
  }

}
