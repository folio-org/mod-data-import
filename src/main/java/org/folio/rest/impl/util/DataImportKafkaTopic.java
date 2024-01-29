package org.folio.rest.impl.util;

import org.folio.kafka.services.KafkaTopic;

public class DataImportKafkaTopic implements KafkaTopic {

  private final String topic;
  private final int numPartitions;

  public DataImportKafkaTopic(String topic, int numPartitions) {
    this.topic = topic;
    this.numPartitions = numPartitions;
  }

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
    return numPartitions;
  }
}
