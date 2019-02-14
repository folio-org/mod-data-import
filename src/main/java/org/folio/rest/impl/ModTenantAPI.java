package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.FileExtensionCollection;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.service.fileextension.FileExtensionService;
import org.folio.service.fileextension.FileExtensionServiceImpl;

import javax.ws.rs.core.Response;
import java.util.Map;

public class ModTenantAPI extends TenantAPI {

  @Validate
  @Override
  public void postTenant(TenantAttributes entity, Map<String, String> headers, Handler<AsyncResult<Response>> handlers, Context context) {
    super.postTenant(entity, headers, ar -> {
      if (ar.failed()) {
        handlers.handle(ar);
      } else {
         setupDefaultFileExtensions(headers, context)
          .setHandler(event -> handlers.handle(ar));
      }
    }, context);
  }

  private Future<Boolean> setupDefaultFileExtensions(Map<String, String> headers, Context context) {
    try {
      FileExtensionService service = new FileExtensionServiceImpl(context.owner(), TenantTool.calculateTenantId(headers.get("x-okapi-tenant")));
      return
        service.getFileExtensions(null, 0, 1)
          .compose(r -> createDefExtensionsIfNeeded(r, service));
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  private Future<Boolean> createDefExtensionsIfNeeded(FileExtensionCollection collection, FileExtensionService service) {
    Future<Boolean> future = Future.future();
    if (collection.getTotalRecords() == 0) {
      return service.copyExtensionsFromDefault()
        .map(r -> r.getUpdated() > 0);
    } else {
      future.complete(true);
    }
    return future;
  }

}
