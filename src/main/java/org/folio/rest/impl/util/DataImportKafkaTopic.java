package org.folio.rest.impl.util;

import org.folio.kafka.services.KafkaTopic;

public enum DataImportKafkaTopic implements KafkaTopic {
  DI_ERROR("DI_ERROR"),
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
}
