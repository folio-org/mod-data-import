package org.folio.service.processing.kafka;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.impl.InboundBuffer;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.RecordsMetadata;
import org.folio.service.processing.reader.SourceReader;
import org.folio.util.pubsub.PubSubClientUtils;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_MARC_BIB_RECORDS_CHUNK_READ;

public class SourceReaderReadStreamWrapper implements ReadStream<KafkaProducerRecord<String, String>> {
  private static final Logger LOGGER = LogManager.getLogger();

  private static final String END_SENTINEL = "EOF";

  private final Vertx vertx;
  private final SourceReader reader;
  private final String jobExecutionId;
  private final int totalRecordsInFile;
  private final List<KafkaHeader> kafkaHeaders;
  private final String tenantId;
  private final int maxDistributionNum;
  private final String topicName;

  private final InboundBuffer<KafkaProducerRecord<String, String>> queue;

  private int recordsCounter = 0;
  private int messageCounter = 0;

  private Handler<Throwable> exceptionHandler;
  private Handler<Void> endHandler;
  private Handler<KafkaProducerRecord<String, String>> handler;

  private boolean closed;

  public SourceReaderReadStreamWrapper(Vertx vertx, SourceReader reader, String jobExecutionId, int totalRecordsInFile,
                                       OkapiConnectionParams okapiConnectionParams, int maxDistributionNum, String topicName) {
    this.vertx = vertx;
    this.reader = reader;
    this.jobExecutionId = jobExecutionId;
    this.totalRecordsInFile = totalRecordsInFile;
    this.maxDistributionNum = maxDistributionNum;
    this.tenantId = okapiConnectionParams.getTenantId();
    this.kafkaHeaders = okapiConnectionParams
      .getHeaders()
      .entries()
      .stream()
      .map(e -> KafkaHeader.header(e.getKey(), e.getValue()))
      .collect(Collectors.toList());

    this.queue = new InboundBuffer<>(vertx.getOrCreateContext(), 0);
    this.topicName = topicName;
    queue.handler(record -> {
      handleNextChunk(record);

      if (record.headers().stream().anyMatch(h -> END_SENTINEL.equals(h.key()))) {
        handleEnd();
      }
    });

    queue.drainHandler(v -> doRead());

    LOGGER.debug("SourceReaderReadStreamWrapper has been created");
  }

  @Override
  public ReadStream<KafkaProducerRecord<String, String>> exceptionHandler(Handler<Throwable> exceptionHandler) {
    check();
    this.exceptionHandler = exceptionHandler;
    return this;
  }

  @Override
  public ReadStream<KafkaProducerRecord<String, String>> handler(@Nullable Handler<KafkaProducerRecord<String, String>> handler) {
    check();
    if (closed) {
      return this;
    }
    this.handler = handler;
    if (handler != null) {
      doRead();
    } else {
      queue.clear();
    }
    return this;
  }

  @Override
  public ReadStream<KafkaProducerRecord<String, String>> pause() {
    check();
    queue.pause();
    return this;
  }

  @Override
  public ReadStream<KafkaProducerRecord<String, String>> resume() {
    check();
    if (!closed) {
      queue.resume();
    }
    return this;
  }

  @Override
  public ReadStream<KafkaProducerRecord<String, String>> fetch(long amount) {
    queue.fetch(amount);
    return this;
  }

  @Override
  public ReadStream<KafkaProducerRecord<String, String>> endHandler(@Nullable Handler<Void> endHandler) {
    check();
    this.endHandler = endHandler;
    return this;
  }

  private void doRead() {
    doReadInternal();
  }

  private void doReadInternal() {
    vertx.runOnContext(v -> {
      try {
        RawRecordsDto chunk;
        boolean notEof = reader.hasNext();

        if (notEof) {
          List<InitialRecord> records = reader.next();
          recordsCounter += records.size();

          chunk = new RawRecordsDto()
            .withId(UUID.randomUUID().toString())
            .withInitialRecords(records)
            .withRecordsMetadata(new RecordsMetadata()
              .withContentType(reader.getContentType())
              .withCounter(recordsCounter)
              .withLast(false)
              .withTotal(totalRecordsInFile));

        } else {
          chunk = new RawRecordsDto()
            .withRecordsMetadata(new RecordsMetadata()
              .withContentType(reader.getContentType())
              .withCounter(recordsCounter)
              .withLast(true)
              .withTotal(totalRecordsInFile));
        }

        KafkaProducerRecord<String, String> kafkaProducerRecord = createKafkaProducerRecord(chunk);
        boolean canWrite = queue.write(kafkaProducerRecord);
        LOGGER.debug("Next chunk has been written to the queue. Key: {}", kafkaProducerRecord.key());
        if (canWrite && notEof && !closed) {
          doReadInternal();
        }
      } catch (Exception e) {
        LOGGER.error(e);
        handleException(e);
      }
    });
  }

  private KafkaProducerRecord<String, String> createKafkaProducerRecord(RawRecordsDto chunk) throws IOException {
    String correlationId = UUID.randomUUID().toString();
    Event event = new Event()
      .withId(correlationId)
      .withEventType(DI_RAW_MARC_BIB_RECORDS_CHUNK_READ.value())
      .withEventPayload(ZIPArchiver.zip(Json.encode(chunk)))
      .withEventMetadata(new EventMetadata()
        .withTenantId(tenantId)
        .withEventTTL(1)
        .withPublishedBy(PubSubClientUtils.constructModuleName()));

    int chunkNumber = ++messageCounter;
    String key = String.valueOf(chunkNumber % maxDistributionNum);

    KafkaProducerRecord<String, String> record =
      KafkaProducerRecord.create(topicName, key, Json.encode(event));

    record.addHeaders(kafkaHeaders);

    record.addHeader("jobExecutionId", jobExecutionId);
    record.addHeader("correlationId", correlationId);
    record.addHeader("chunkNumber", String.valueOf(chunkNumber));

    if (chunk.getRecordsMetadata().getLast()) {
      record.addHeader(END_SENTINEL, "true");
    }

    LOGGER.debug("Next chunk has been created: correlationId: {} chunkNumber: {}", correlationId, chunkNumber);
    return record;
  }

  private void check() {
    checkClosed();
  }

  private void checkClosed() {
    if (closed) {
      throw new IllegalStateException("Reader is closed");
    }
  }

  private void handleNextChunk(KafkaProducerRecord<String, String> packedChunk) {
    Handler<KafkaProducerRecord<String, String>> handler;
    synchronized (this) {
      handler = this.handler;
    }
    if (handler != null) {
      handler.handle(packedChunk);
    }
  }

  private void handleEnd() {
    closed = true;

    LOGGER.debug("End handler. Processing completed: {}", this);

    Handler<Void> endHandler;
    synchronized (this) {
      handler = null;
      endHandler = this.endHandler;
    }
    if (endHandler != null) {
      endHandler.handle(null);
    }
  }

  private void handleException(Throwable t) {
    if (exceptionHandler != null && t instanceof Exception) {
      exceptionHandler.handle(t);
    } else {
      LOGGER.error("Unhandled exception:", t);
    }
  }

}
