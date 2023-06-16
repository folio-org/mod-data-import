package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.serviceproxy.ServiceBinder;
import org.folio.config.ApplicationConfig;
import org.folio.kafka.KafkaConfig;
import org.folio.liquibase.LiquibaseUtil;
import org.folio.rest.resource.interfaces.InitAPI;
import org.folio.service.processing.FileProcessor;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

public class InitAPIImpl implements InitAPI {

  private static String MODULE_GLOBAL_SCHEMA = "dataImportGlobal";
  @Autowired
  private KafkaConfig kafkaConfig;

  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> handler) {

    try {
      SpringContextUtil.init(vertx, context, ApplicationConfig.class);
      SpringContextUtil.autowireDependencies(this, context);
      LiquibaseUtil.initializeSchemaForModule(vertx, MODULE_GLOBAL_SCHEMA);
      initFileProcessor(vertx);
      handler.handle(Future.succeededFuture(true));
    } catch (Exception e) {
      handler.handle(Future.failedFuture(e));
    }
  }

  private void initFileProcessor(Vertx vertx) {
    new ServiceBinder(vertx)
      .setAddress(FileProcessor.FILE_PROCESSOR_ADDRESS)
      .register(FileProcessor.class, FileProcessor.create(vertx, kafkaConfig));
  }
}
