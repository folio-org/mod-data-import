package org.folio.service.storage;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.folio.util.ConfigurationUtil;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Async file storage service builder builder
 */
public class FileStorageServiceBuilder {

  private static final String SERVICE_STORAGE_PROPERTY_CODE = "data.import.storage.type";

  private FileStorageServiceBuilder() {
  }

  /**
   * Build a service object by mod-configuration's values. If there are no properties build a default LocalStorage Service
   *
   * @param vertx        - vertx object
   * @param tenantId     - current tenant id
   * @param okapiHeaders - Map with headers and token
   * @return - new Service object
   */
  public static Future<FileStorageService> build(Vertx vertx, String tenantId, Map<String, String> okapiHeaders) {
    Future<FileStorageService> future = Future.future();
    ConfigurationUtil.getPropertyByCode(SERVICE_STORAGE_PROPERTY_CODE, okapiHeaders).setHandler(result -> {
      if (result.failed() || result.result() == null || result.result().isEmpty()) {
        future.complete(new LocalFileStorageService(vertx, tenantId));
        return;
      }
      String serviceCode = result.result();
      List<FileStorageService> availableServices = Arrays.asList(new LocalFileStorageService(vertx, tenantId));
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
