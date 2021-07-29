package org.folio.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.KafkaConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {
  "org.folio.rest.impl",
  "org.folio.dao",
  "org.folio.service.file",
  "org.folio.service.fileextension",
  "org.folio.service.upload",
  "org.folio.service.cleanup"})
public class ApplicationConfig {
  private static final Logger LOGGER = LogManager.getLogger();

  @Value("${KAFKA_HOST:kafka}")
  private String kafkaHost;
  @Value("${KAFKA_PORT:9092}")
  private String kafkaPort;
  @Value("${OKAPI_URL:http://okapi:9130}")
  private String okapiUrl;
  @Value("${REPLICATION_FACTOR:1}")
  private int replicationFactor;
  @Value("${MAX_REQUEST_SIZE:1048576}")
  private int maxRequestSize;
  @Value("${ENV:folio}")
  private String envId;

  @Bean(name = "newKafkaConfig")
  public KafkaConfig kafkaConfigBean() {
    LOGGER.info("ENV variable value is: {}", System.getenv("ENV"));
    LOGGER.info("EnvId resolved by Spring: {}", envId);

    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .envId(envId)
      .kafkaHost(kafkaHost)
      .kafkaPort(kafkaPort)
      .okapiUrl(okapiUrl)
      .replicationFactor(replicationFactor)
      .maxRequestSize(maxRequestSize)
      .build();
    LOGGER.info("kafkaConfig: {}", kafkaConfig);

    return kafkaConfig;
  }

}
