package org.folio.service.processing;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.serviceproxy.ServiceBinder;
import org.folio.rest.resource.interfaces.InitAPI;

/**
 * Performs components registration before the verticle is deployed.
 */
public class InitFileProcessorAPI implements InitAPI {
  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> handler) {
    new ServiceBinder(vertx)
      .setAddress(FileProcessor.FILE_PROCESSOR_ADDRESS)
      .register(FileProcessor.class, FileProcessor.create(vertx));
  }
}
