package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.serviceproxy.ServiceBinder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.config.ApplicationConfig;
import org.folio.kafka.KafkaConfig;
import org.folio.liquibase.LiquibaseUtil;
import org.folio.rest.resource.interfaces.InitAPI;
import org.folio.service.processing.FileProcessor;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

public class InitAPIImpl implements InitAPI {

  private static final Logger LOGGER = LogManager.getLogger();

  private static final String MODULE_GLOBAL_SCHEMA = "data_import_global";

  @Autowired
  private KafkaConfig kafkaConfig;

  @Value("${SPLIT_FILES_ENABLED:false}")
  private boolean fileSplittingEnabled;

  @Override
  public void init(
    Vertx vertx,
    Context context,
    Handler<AsyncResult<Boolean>> handler
  ) {
    try {
      SpringContextUtil.init(vertx, context, ApplicationConfig.class);
      SpringContextUtil.autowireDependencies(this, context);

      LOGGER.info("Initializing Liquibase for module...");
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
