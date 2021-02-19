package org.folio.service.processing.kafka;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.vertx.core.streams.WriteStream;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.util.List;

public class WriteStreamWrapper implements WriteStream<KafkaProducerRecord<String, String>> {

  private static final Logger LOGGER = LogManager.getLogger();

  private final KafkaProducer<String, String> producer;

  public WriteStreamWrapper(KafkaProducer<String, String> producer) {
    this.producer = producer;
  }

  @Override
  public WriteStream<KafkaProducerRecord<String, String>> exceptionHandler(Handler<Throwable> handler) {
    producer.exceptionHandler(handler);
    return this;
  }

  @Override
  public Future<Void> write(KafkaProducerRecord<String, String> data) {
    return Future.future(e -> producer.write(data, ar -> logChunkProcessingResult(data.headers(), ar)));
  }

  @Override
  public void write(KafkaProducerRecord<String, String> data, Handler<AsyncResult<Void>> handler) {
    producer.write(data, ar -> {
      logChunkProcessingResult(data.headers(), ar);
      handler.handle(ar);
    });
  }

  @Override
  public Future<Void> end() {
    return Future.future( e ->producer.end());
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    producer.end(handler);
  }

  @Override
  public WriteStream<KafkaProducerRecord<String, String>> setWriteQueueMaxSize(int maxSize) {
    producer.setWriteQueueMaxSize(maxSize);
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    return producer.writeQueueFull();
  }

  @Override
  public WriteStream<KafkaProducerRecord<String, String>> drainHandler(@Nullable Handler<Void> handler) {
    producer.drainHandler(handler);
    return this;
  }

  private void logChunkProcessingResult(List<KafkaHeader> headers, AsyncResult<Void> ar) {
    String correlationId = null;
    String chunkNumber = null;
    for (KafkaHeader h : headers) {
      if ("correlationId".equals(h.key())) {
        correlationId = h.value().toString();
      } else if ("chunkNumber".equals(h.key())) {
        chunkNumber = h.value().toString();
      }
    }
    if (ar.succeeded()) {
      LOGGER.debug("Next chunk has been written: correlationId: {} chunkNumber: {}", correlationId, chunkNumber);
    } else {
      LOGGER.error("Next chunk has failed with errors correlationId: {} chunkNumber: {}", correlationId, chunkNumber, ar.cause());
    }
  }
}
