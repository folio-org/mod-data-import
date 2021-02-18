package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.ConfigurationUtil;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.FileExtensionCollection;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.service.cleanup.StorageCleanupService;
import org.folio.service.fileextension.FileExtensionService;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.core.Response;
import java.util.Map;

import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;

public class ModTenantAPI extends TenantAPI {

  private static final String DELAY_TIME_BETWEEN_CLEANUP_CODE = "data.import.cleanup.delay.time";
  private static final long DELAY_TIME_BETWEEN_CLEANUP_VALUE_MILLIS = 3600_000;

  private static final Logger LOGGER = LogManager.getLogger();

  @Autowired
  private FileExtensionService fileExtensionService;

  @Autowired
  private StorageCleanupService storageCleanupService;

  public ModTenantAPI() { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Override
  Future<Integer> loadData(TenantAttributes attributes, String tenantId, Map<String, String> headers, Context context) {
    return super.loadData(attributes, tenantId, headers, context)
      .compose(num -> {
        initStorageCleanupService(headers, context);
        return setupDefaultFileExtensions(headers).map(num);
      });
  }

  private Future<Boolean> setupDefaultFileExtensions(Map<String, String> headers) {
    try {
      String tenantId = TenantTool.calculateTenantId(headers.get(OKAPI_TENANT_HEADER));
      return
        fileExtensionService.getFileExtensions(null, 0, 1, tenantId)
          .compose(r -> createDefExtensionsIfNeeded(r, fileExtensionService, tenantId));
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  private Future<Boolean> createDefExtensionsIfNeeded(FileExtensionCollection collection, FileExtensionService service, String tenantId) {
    Promise<Boolean> promise = Promise.promise();
    if (collection.getTotalRecords() == 0) {
      return service.copyExtensionsFromDefault(tenantId)
        .map(r -> r.rowCount() > 0);
    } else {
      promise.complete(true);
    }
    return promise.future();
  }

  private void initStorageCleanupService(Map<String, String> headers, Context context) {
    Vertx vertx = context.owner();
    OkapiConnectionParams params = new OkapiConnectionParams(headers, vertx);

    ConfigurationUtil.getPropertyByCode(DELAY_TIME_BETWEEN_CLEANUP_CODE, params)
      .map(Long::parseLong)
      .otherwise(DELAY_TIME_BETWEEN_CLEANUP_VALUE_MILLIS)
      .onComplete(delayTimeAr -> vertx.setPeriodic(delayTimeAr.result(),
        e -> vertx.<Void>executeBlocking(b -> storageCleanupService.cleanStorage(params),
        cleanupAr -> {
          if (cleanupAr.failed()) {
            LOGGER.error("Error during cleaning file storage.", cleanupAr.cause());
          } else {
            LOGGER.info("File storage was successfully cleaned of unused files");
          }
        })));
  }

}
