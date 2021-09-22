package org.folio.service.processing.kafka;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.Handler;
import io.vertx.core.streams.WriteStream;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * WriteStream is package from Vertex and this wrapper designed to separate features that not supported by Spring Kafka
 * when implementing Vertex streams, would be replaced after full migration to Spring.
 */
public interface SpringKafkaWriteStreamWrapper extends WriteStream<ProducerRecord<String, String>> {
  @Override
  default WriteStream<ProducerRecord<String, String>> exceptionHandler(Handler<Throwable> handler) {
    throw new UnsupportedOperationException("Setting exceptionHandler is not supported for Spring Kafka");
  }

  @Override
  default WriteStream<ProducerRecord<String, String>> setWriteQueueMaxSize(int maxSize) {
    throw new UnsupportedOperationException("Setting setWriteQueueMaxSize is not supported for Spring Kafka");
  }

  @Override
  default WriteStream<ProducerRecord<String, String>> drainHandler(@Nullable Handler<Void> handler) {
    throw new UnsupportedOperationException("Setting exceptionHandler is not supported for Spring Kafka");
  }
}
