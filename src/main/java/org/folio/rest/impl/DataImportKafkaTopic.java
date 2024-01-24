package org.folio.rest.impl;

import org.folio.kafka.services.KafkaTopic;

public enum DataImportKafkaTopic implements KafkaTopic {
  DI_COMPLETED("di-completed"),
  DI_EDIFACT_RECORD_CREATED("di-edifact-record-created"),
  DI_ERROR("di-error"),
  DI_INITIALIZATION_STARTED("di-initialization-started"),
  DI_INVENTORY_AUTHORITY_CREATED_READY_FOR_POST_PROCESSING("di-inventory-authority-created-ready-for-post-processing"),
  DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING("di-inventory-instance-created-ready-for-post-processing"),
  DI_LOG_SRS_MARC_AUTHORITY_RECORD_CREATED("di-log-srs-marc-authority-record-created"),
  DI_LOG_SRS_MARC_BIB_RECORD_CREATED("di-log-srs-marc-bib-record-created"),
  DI_MARC_BIB_FOR_ORDER_CREATED("di-marc-bib-for-order-created"),
  DI_MARC_FOR_UPDATE_RECEIVED("di-marc-for-update-received"),
  DI_PARSED_RECORDS_CHUNK_SAVED("di-parsed-records-chunk-saved"),
  DI_RAW_RECORDS_CHUNK_PARSED("di-raw-records-chunk-parsed"),
  DI_RAW_RECORDS_CHUNK_READ("di-raw-records-chunk-read"),
  DI_SRS_MARC_AUTHORITY_RECORD_CREATED("di-srs-marc-authority-record-created"),
  DI_SRS_MARC_AUTHORITY_RECORD_NOT_MATCHED("di-srs-marc-authority-record-not-matched"),
  DI_SRS_MARC_BIB_INSTANCE_HRID_SET("di-srs-marc-bib-instance-hrid-set"),
  DI_SRS_MARC_BIB_RECORD_CREATED("di-srs-marc-bib-record-created"),
  DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING("di-srs-marc-bib-record-modified-ready-for-post-processing");

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
