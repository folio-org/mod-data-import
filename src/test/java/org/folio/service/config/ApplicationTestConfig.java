package org.folio.service.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.KafkaConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(
  basePackages = {
    "org.folio.dao",
    "org.folio.rest.impl",
    "org.folio.service.auth",
    "org.folio.service.cleanup",
    "org.folio.service.file",
    "org.folio.service.fileextension",
    "org.folio.service.processing",
    "org.folio.service.processing.ranking",
    "org.folio.service.processing.split",
    "org.folio.service.s3storage",
    "org.folio.service.upload",
  }
)
public class ApplicationTestConfig {

  private static final Logger LOGGER = LogManager.getLogger();

  @Value("${KAFKA_HOST:kafka}")
  private String kafkaHost;
  @Value("${KAFKA_PORT:9092}")
  private String kafkaPort;
  @Value("${OKAPI_URL:http://okapi:9130}")
  private String okapiUrl;
  @Value("${REPLICATION_FACTOR:1}")
  private int replicationFactor;
  @Value("${ENV:folio}")
  private String envId;

  @Bean(name = "newKafkaConfig")
  public KafkaConfig kafkaConfigBean() {
    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .envId(envId)
      .kafkaHost(kafkaHost)
      .kafkaPort(kafkaPort)
      .okapiUrl(okapiUrl)
      .replicationFactor(replicationFactor)
      .build();
    LOGGER.debug("kafkaConfigBean:: kafkaConfig: " + kafkaConfig);

    return kafkaConfig;
  }
}
