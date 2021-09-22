package org.folio.service.processing.kafka;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Optional;

/**
 * Implements Vertex write stream to be able to transfer large objects when fully not storing them in memory.
 */
public class SpringKafkaWriteStream implements SpringKafkaWriteStreamWrapper {

  private static final Logger LOGGER = LogManager.getLogger();

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final Context context;

  public SpringKafkaWriteStream(KafkaTemplate<String, String> kafkaTemplate, Context context) {
    this.kafkaTemplate = kafkaTemplate;
    this.context = context;
  }

  @Override
  public Future<Void> write(ProducerRecord<String, String> data) {
    sendToKafka(data, null);
    return Future.succeededFuture();
  }

  @Override
  public void write(ProducerRecord<String, String> data, Handler<AsyncResult<Void>> handler) {
    sendToKafka(data, handler);
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    runSucceedHandler(handler);
  }

  private ListenableFuture<SendResult<String, String>> sendToKafka(ProducerRecord<String, String> data, Handler<AsyncResult<Void>> handler) {
    ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(data);
    future.addCallback(new ListenableFutureCallback<>() {

      @Override
      public void onSuccess(SendResult<String, String> result) {
        Headers headers = result.getProducerRecord().headers();
        LOGGER.info("Next chunk has been written: correlationId: {} chunkNumber: {}",
          getHeaderValue(headers, "correlationId"),
          getHeaderValue(headers, "chunkNumber"));
        runSucceedHandler(handler);
      }

      @Override
      public void onFailure(Throwable ex) {
        LOGGER.error("Next chunk has failed with errors", ex);
      }
    });
    return future;
  }

  @Override
  public boolean writeQueueFull() {
    return false; //always return false for Spring Kafka
  }

  private void runSucceedHandler(Handler<AsyncResult<Void>> handler) {
    if (handler != null) {
      context.runOnContext(v -> handler.handle(Future.succeededFuture()));
    }
  }

  private String getHeaderValue(Headers headers, String headerName) {
    return Optional.ofNullable(headers.lastHeader(headerName)).map(header -> new String(header.value())).orElse(null);
  }
}
