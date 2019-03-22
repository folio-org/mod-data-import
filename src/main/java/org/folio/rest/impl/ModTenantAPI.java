package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.FileExtensionCollection;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.service.fileextension.FileExtensionService;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.core.Response;
import java.util.Map;

import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;

public class ModTenantAPI extends TenantAPI {

  @Autowired
  private FileExtensionService fileExtensionService;

  public ModTenantAPI() { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Validate
  @Override
  public void postTenant(TenantAttributes entity, Map<String, String> headers, Handler<AsyncResult<Response>> handlers, Context context) {
    super.postTenant(entity, headers, ar -> {
      if (ar.failed()) {
        handlers.handle(ar);
      } else {
         setupDefaultFileExtensions(headers)
          .setHandler(event -> handlers.handle(ar));
      }
    }, context);
  }

  private Future<Boolean> setupDefaultFileExtensions(Map<String, String> headers) {
    try {
      String tenantId = TenantTool.calculateTenantId((String) headers.get(OKAPI_TENANT_HEADER));
      return
        fileExtensionService.getFileExtensions(null, 0, 1, tenantId)
          .compose(r -> createDefExtensionsIfNeeded(r, fileExtensionService, tenantId));
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  private Future<Boolean> createDefExtensionsIfNeeded(FileExtensionCollection collection, FileExtensionService service, String tenantId) {
    Future<Boolean> future = Future.future();
    if (collection.getTotalRecords() == 0) {
      return service.copyExtensionsFromDefault(tenantId)
        .map(r -> r.getUpdated() > 0);
    } else {
      future.complete(true);
    }
    return future;
  }

}
