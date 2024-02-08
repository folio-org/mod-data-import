package org.folio.service.kafka;

import org.folio.kafka.services.KafkaTopic;
import org.folio.service.kafka.support.DataImportKafkaTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

@Service
@PropertySource(value = "kafka.properties")
public class DIKafkaTopicService {

  @Value("${di_initialization_started.partitions}")
  private Integer diInitNumPartitions;

  @Value("${di_raw_records_chunk_read.partitions}")
  private Integer diRawRecordsChunkReadNumPartitions;

  public KafkaTopic[] createTopicObjects() {
    return new DataImportKafkaTopic[] {
      new DataImportKafkaTopic("DI_INITIALIZATION_STARTED", diInitNumPartitions),
      new DataImportKafkaTopic("DI_RAW_RECORDS_CHUNK_READ", diRawRecordsChunkReadNumPartitions)};
  }
}
