package org.folio.rest.impl.util;

import org.apache.commons.lang3.StringUtils;
import org.folio.kafka.services.KafkaTopic;

public enum DataImportKafkaTopic implements KafkaTopic {
  DI_INITIALIZATION_STARTED("DI_INITIALIZATION_STARTED"),
  DI_RAW_RECORDS_CHUNK_READ("DI_RAW_RECORDS_CHUNK_READ");

  private final String topic;

  DataImportKafkaTopic(String topic) { this.topic = topic; }

  @Override
  public String moduleName() {
    return "data-import";
  }

  @Override
  public String topicName() {
    return topic;
  }

  @Override
  public int numPartitions() {
    return Integer.parseInt(StringUtils.firstNonBlank(
      System.getenv("KAFKA_DOMAIN_TOPIC_NUM_PARTITIONS"),
      System.getProperty("KAFKA_DOMAIN_TOPIC_NUM_PARTITIONS"),
      System.getProperty("kafka-domain-topic-num-partitions"), "50"));
  }
}
