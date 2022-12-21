package org.folio.service.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.events.utils.PomReaderUtil;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.tools.utils.ModuleName;

import java.util.List;
import java.util.UUID;

public final class EventHandlingUtil {

  private static final Logger LOGGER = LogManager.getLogger();

  private EventHandlingUtil(){}

  public static String constructModuleName() {
    return PomReaderUtil.INSTANCE.constructModuleVersionAndVersion(ModuleName.getModuleName(),
      ModuleName.getModuleVersion());
  }

  /**
   * Prepares and sends event with payload to kafka
   *
   * @param tenantId tenant id
   * @param eventPayload eventPayload in String representation
   * @param eventType eventType
   * @param kafkaHeaders kafka headers
   * @param kafkaConfig kafka config
   * @return completed future with true if event was sent successfully
   */
  public static Future<Boolean> sendEventToKafka(
      String tenantId,
      String eventPayload,
      String eventType,
      List<KafkaHeader> kafkaHeaders,
      KafkaConfig kafkaConfig,
      String key,
      Vertx vertx) {
    LOGGER.debug("sendEventToKafka:: Starting to send event to Kafka for eventType: {}", eventType);
    Event event = createEvent(eventPayload, eventType, tenantId);

    String topicName = createTopicName(eventType, tenantId, kafkaConfig);

    KafkaProducerRecord<String, String> record = createProducerRecord(event, key, topicName, kafkaHeaders);

    Promise<Boolean> promise = Promise.promise();

    String chunkId = extractHeader(kafkaHeaders, "chunkId");
    String recordId = extractHeader(kafkaHeaders, "recordId");

    String producerName = eventType + "_Producer";
    KafkaProducer<String, String> producer =
      KafkaProducer.createShared(vertx, producerName, kafkaConfig.getProducerProps());
    producer.write(record)
      .<Void>mapEmpty()
      .eventually(x -> producer.flush())
      .eventually(x -> producer.close())
      .onComplete(war -> {
        if (war.succeeded()) {
          logSendingSucceeded(eventType, chunkId, recordId);
          promise.complete(true);
        } else {
          Throwable cause = war.cause();
          LOGGER.warn("{} write error for event {}:", producerName, eventType, cause);
          promise.fail(cause);
        }
      });
    return promise.future();
  }

  private static void logSendingSucceeded(String eventType, String chunkId, String recordId) {
    if (StringUtils.isBlank(recordId)) {
      LOGGER.info("logSendingSucceeded:: Event with type: {} and chunkId: {} was sent to kafka", eventType, chunkId);
    } else {
      LOGGER.info("logSendingSucceeded:: Event with type: {} and recordId: {} was sent to kafka", eventType, recordId);
    }
  }

  private static String extractHeader(List<KafkaHeader> kafkaHeaders, String headerName) {
    return kafkaHeaders.stream()
      .filter(header -> header.key().equals(headerName))
      .findFirst()
      .map(header -> header.value().toString())
      .orElse(null);
  }

  public static KafkaProducerRecord<String, String> createProducerRecord(Event event, String key, String topicName, List<KafkaHeader> kafkaHeaders) {
    KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(topicName, key, Json.encode(event));
    record.addHeaders(kafkaHeaders);
    return record;
  }

  public static String createTopicName(String eventType, String tenantId, KafkaConfig kafkaConfig) {
    return KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), KafkaTopicNameHelper.getDefaultNameSpace(),
      tenantId, eventType);
  }

  public static Event createEvent(String eventPayload, String eventType, String tenantId) {
    return new Event()
      .withId(UUID.randomUUID().toString())
      .withEventType(eventType)
      .withEventPayload(eventPayload)
      .withEventMetadata(new EventMetadata()
        .withTenantId(tenantId)
        .withEventTTL(1)
        .withPublishedBy(constructModuleName()));
  }
}
