package org.folio.rest.impl.util;

import org.folio.kafka.services.KafkaTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

@Service
@PropertySource(value = "kafka.properties")
public class DIKafkaTopicService {

  @Value("${di_initialization_started.partitions}")
  private Integer diInitNumPartitions;

  @Value("${di_raw_records_chunk_read.partitions}")
  private Integer diRawRecordsChunkReadPartitions;

  public KafkaTopic[] createTopicObjects() {
    var initTopic = new DataImportKafkaTopic("DI_INITIALIZATION_STARTED", diInitNumPartitions);
    var rawRecordsChunkReadTopic = new DataImportKafkaTopic("DI_RAW_RECORDS_CHUNK_READ", diRawRecordsChunkReadPartitions);
    return new DataImportKafkaTopic[] {initTopic, rawRecordsChunkReadTopic};
  }
}
