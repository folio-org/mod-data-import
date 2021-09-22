package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.serviceproxy.ServiceBinder;
import org.folio.config.ApplicationConfig;
import org.folio.rest.resource.interfaces.InitAPI;
import org.folio.service.processing.FileProcessor;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

public class InitAPIImpl implements InitAPI {

  @Value("${ENV:folio}")
  private String envId;
  @Autowired
  private KafkaTemplate kafkaTemplate;

  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> handler) {

    try {
      SpringContextUtil.init(vertx, context, ApplicationConfig.class);
      SpringContextUtil.autowireDependencies(this, context);
      initFileProcessor(vertx);
      handler.handle(Future.succeededFuture(true));
    } catch (Exception e) {
      handler.handle(Future.failedFuture(e));
    }
  }

  private void initFileProcessor(Vertx vertx) {
    new ServiceBinder(vertx)
      .setAddress(FileProcessor.FILE_PROCESSOR_ADDRESS)
      .register(FileProcessor.class, FileProcessor.create(vertx, kafkaTemplate, envId));
  }
}
