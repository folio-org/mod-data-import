package org.folio.service.processing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INITIALIZATION_STARTED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_READ;
import static org.folio.support.TestUtil.TENANT_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.json.Json;
import io.vertx.junit5.VertxTestContext;
import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.DataImportInitConfig;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.service.storage.FileStorageService;
import org.folio.support.AbstractRestTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ParallelFileChunkingProcessorTest extends AbstractRestTest {

  private static final String KAFKA_ENV = "test-env";
  private static final String TENANT_ID_TEST_MARC_RAW = "diku_marc_raw";
  private static final String TENANT_ID_TEST_MARC_JSON = "diku_marc_json";
  private static final String TENANT_ID_TEST_MARC_XML = "diku_marc_xml";
  private static final String TENANT_ID_TEST_EDI_RAW = "diku_edifact_raw";

  private static final String SOURCE_PATH_1 = "src/test/resources/CornellFOLIOExemplars.mrc";
  private static final String SOURCE_PATH_2 = "src/test/resources/ChalmersFOLIOExamples.json";
  private static final String SOURCE_PATH_3 = "src/test/resources/invalidJsonExample.json";
  private static final String SOURCE_PATH_4 = "src/test/resources/UChicago_SampleBibs.xml";
  private static final String SOURCE_PATH_5 = "src/test/resources/invalidUChicago_SampleBibs.xml";
  private static final String SOURCE_PATH_6 = "src/test/resources/invalidMarcFile.mrc";
  private static final String SOURCE_PATH_7 = "src/test/resources/edifact/274812_WSHEIN_STO.txt";
  private static final String CONTENT_TYPE_RAW = "MARC_RAW";
  private static final String EDI_CONTENT_TYPE_RAW = "EDIFACT_RAW";
  private static final String CONTENT_TYPE_JSON = "MARC_JSON";
  private static final String CONTENT_TYPE_XML = "MARC_XML";
  private static final String MARC_TYPE_JOB_PROFILE = "marcJobProfile";
  private static final String EDI_FACT_JOB_PROFILE = "ediFactJobProfile";
  private static final String EMPTY_TYPE_JOB_PROFILE = "emptyTypeJobProfile";
  private static final String JOB_PROFILE_NAME = "MARC profile";
  private static final String KAFKA_HOST_PROP_NAME = "KAFKA_HOST";
  private static final String KAFKA_PORT_PROP_NAME = "KAFKA_PORT";
  private static final String KAFKA_MAX_REQUEST_SIZE = "MAX_REQUEST_SIZE";

  private static final int RECORDS_NUMBER = 62;

  private final Map<String, String> headers = new HashMap<>();
  private ParallelFileChunkingProcessor fileProcessor;
  private KafkaConfig kafkaConfig;
  private Map<String, JobProfileInfo> jobProfiles;

  @BeforeEach
  void setUpProcessor() {
    headers.put(XOkapiHeaders.URL, mockServerUrl());
    headers.put(XOkapiHeaders.TENANT, TENANT_ID);
    headers.put(XOkapiHeaders.TOKEN, TOKEN);

    kafkaConfig = KafkaConfig.builder()
      .kafkaHost(System.getProperty(KAFKA_HOST_PROP_NAME))
      .kafkaPort(System.getProperty(KAFKA_PORT_PROP_NAME))
      .envId(KAFKA_ENV)
      .maxRequestSize(Integer.parseInt(System.getProperty(KAFKA_MAX_REQUEST_SIZE, "4000000")))
      .okapiUrl(mockServerUrl())
      .build();

    jobProfiles = createJobProfilesMap();
    fileProcessor = new ParallelFileChunkingProcessor(vertx, kafkaConfig);
  }

  @DisplayName("should read MARC bib file and send all chunks to Kafka")
  @Test
  void shouldReadMarcBibAndSendAllChunks(VertxTestContext testContext) {
    headers.put(XOkapiHeaders.TENANT, TENANT_ID_TEST_MARC_RAW);

    FileDefinition fileDefinition = createFileDefinition();
    FileStorageService fileStorageService = createFileStorageServiceMock(SOURCE_PATH_1);

    fileProcessor.processFile(
      fileStorageService.getFile(fileDefinition.getSourcePath()),
      fileDefinition.getJobExecutionId(),
      jobProfiles.get(MARC_TYPE_JOB_PROFILE),
      new ConnectionParams(headers)
    ).onComplete(testContext.succeeding(v -> testContext.verify(() -> {
      assertInitializationDataFromKafka(fileDefinition.getJobExecutionId(), TENANT_ID_TEST_MARC_RAW, RECORDS_NUMBER);
      assertRawChunkDataFromKafka(fileStorageService, CONTENT_TYPE_RAW, TENANT_ID_TEST_MARC_RAW);
      testContext.completeNow();
    })));
  }

  @DisplayName("should return error when job profile is null")
  @Test
  void shouldReturnError_whenJobProfileIsNull(VertxTestContext testContext) {
    FileDefinition fileDefinition = createFileDefinition();
    FileStorageService fileStorageService = createFileStorageServiceMock(SOURCE_PATH_1);

    fileProcessor.processFile(
      fileStorageService.getFile(fileDefinition.getSourcePath()),
      fileDefinition.getJobExecutionId(),
      null,
      new ConnectionParams(headers)
    ).onComplete(testContext.failingThenComplete());
  }

  @DisplayName("should return error when job profile has no data type")
  @Test
  void shouldReturnError_whenJobProfileHasNoDataType(VertxTestContext testContext) {
    FileDefinition fileDefinition = createFileDefinition();
    FileStorageService fileStorageService = createFileStorageServiceMock(SOURCE_PATH_1);

    fileProcessor.processFile(
      fileStorageService.getFile(fileDefinition.getSourcePath()),
      fileDefinition.getJobExecutionId(),
      jobProfiles.get(EMPTY_TYPE_JOB_PROFILE),
      new ConnectionParams(headers)
    ).onComplete(testContext.failingThenComplete());
  }

  @DisplayName("should return error when EDIFACT job profile is used with MARC data type")
  @Test
  void shouldReturnError_whenEdifactJobProfileUsedWithMarcDataType(VertxTestContext testContext) {
    FileDefinition fileDefinition = createFileDefinition();
    FileStorageService fileStorageService = createFileStorageServiceMock(SOURCE_PATH_1);

    fileProcessor.processFile(
      fileStorageService.getFile(fileDefinition.getSourcePath()),
      fileDefinition.getJobExecutionId(),
      jobProfiles.get(EDI_FACT_JOB_PROFILE),
      new ConnectionParams(headers)
    ).onComplete(testContext.failingThenComplete());
  }

  @DisplayName("should read JSON array file and send all chunks to Kafka")
  @Test
  void shouldReadJsonArrayFileAndSendAllChunks(VertxTestContext testContext) {
    headers.put(XOkapiHeaders.TENANT, TENANT_ID_TEST_MARC_JSON);

    FileDefinition fileDefinition = createFileDefinition();
    FileStorageService fileStorageService = createFileStorageServiceMock(SOURCE_PATH_2);

    fileProcessor.processFile(
      fileStorageService.getFile(fileDefinition.getSourcePath()),
      fileDefinition.getJobExecutionId(),
      jobProfiles.get(MARC_TYPE_JOB_PROFILE),
      new ConnectionParams(headers)
    ).onComplete(testContext.succeeding(v -> testContext.verify(() -> {
      assertInitializationDataFromKafka(fileDefinition.getJobExecutionId(), TENANT_ID_TEST_MARC_JSON, RECORDS_NUMBER);
      assertRawChunkDataFromKafka(fileStorageService, CONTENT_TYPE_JSON, TENANT_ID_TEST_MARC_JSON);
      testContext.completeNow();
    })));
  }

  @DisplayName("should read XML array file and send all chunks to Kafka")
  @Test
  void shouldReadXmlArrayFileAndSendAllChunks(VertxTestContext testContext) {
    headers.put(XOkapiHeaders.TENANT, TENANT_ID_TEST_MARC_XML);

    FileDefinition fileDefinition = createFileDefinition();
    FileStorageService fileStorageService = createFileStorageServiceMock(SOURCE_PATH_4);

    fileProcessor.processFile(
      fileStorageService.getFile(fileDefinition.getSourcePath()),
      fileDefinition.getJobExecutionId(),
      jobProfiles.get(MARC_TYPE_JOB_PROFILE),
      new ConnectionParams(headers)
    ).onComplete(testContext.succeeding(v -> testContext.verify(() -> {
      assertInitializationDataFromKafka(fileDefinition.getJobExecutionId(), TENANT_ID_TEST_MARC_XML, RECORDS_NUMBER);
      assertRawChunkDataFromKafka(fileStorageService, CONTENT_TYPE_XML, TENANT_ID_TEST_MARC_XML);
      testContext.completeNow();
    })));
  }

  @DisplayName("should return error on malformed JSON file")
  @Test
  void shouldReturnError_onMalformedJsonFile(VertxTestContext testContext) {
    FileDefinition fileDefinition = createFileDefinition();
    FileStorageService fileStorageService = createFileStorageServiceMock(SOURCE_PATH_3);

    fileProcessor.processFile(
      fileStorageService.getFile(fileDefinition.getSourcePath()),
      fileDefinition.getJobExecutionId(),
      jobProfiles.get(MARC_TYPE_JOB_PROFILE),
      new ConnectionParams(headers)
    ).onComplete(testContext.failingThenComplete());
  }

  @DisplayName("should return error on malformed XML file")
  @Test
  void shouldReturnError_onMalformedXmlFile(VertxTestContext testContext) {
    FileDefinition fileDefinition = createFileDefinition();
    FileStorageService fileStorageService = createFileStorageServiceMock(SOURCE_PATH_5);

    fileProcessor.processFile(
      fileStorageService.getFile(fileDefinition.getSourcePath()),
      fileDefinition.getJobExecutionId(),
      jobProfiles.get(MARC_TYPE_JOB_PROFILE),
      new ConnectionParams(headers)
    ).onComplete(testContext.failingThenComplete());
  }

  @DisplayName("should return error on invalid MRC file")
  @Test
  void shouldReturnError_onInvalidMrcFile(VertxTestContext testContext) {
    FileDefinition fileDefinition = createFileDefinition();
    FileStorageService fileStorageService = createFileStorageServiceMock(SOURCE_PATH_6);

    fileProcessor.processFile(
      fileStorageService.getFile(fileDefinition.getSourcePath()),
      fileDefinition.getJobExecutionId(),
      jobProfiles.get(MARC_TYPE_JOB_PROFILE),
      new ConnectionParams(headers)
    ).onComplete(testContext.failingThenComplete());
  }

  @DisplayName("should read TXT EDIFACT file and send all chunks to Kafka")
  @Test
  void shouldReadTxtEdifactFileAndSendAllChunks(VertxTestContext testContext) {
    headers.put(XOkapiHeaders.TENANT, TENANT_ID_TEST_EDI_RAW);

    FileDefinition fileDefinition = createFileDefinition();
    FileStorageService fileStorageService = createFileStorageServiceMock(SOURCE_PATH_7);

    fileProcessor.processFile(
      fileStorageService.getFile(fileDefinition.getSourcePath()),
      fileDefinition.getJobExecutionId(),
      jobProfiles.get(EDI_FACT_JOB_PROFILE),
      new ConnectionParams(headers)
    ).onComplete(testContext.succeeding(v -> testContext.verify(() -> {
      assertInitializationDataFromKafka(fileDefinition.getJobExecutionId(), TENANT_ID_TEST_EDI_RAW, 1);
      assertRawChunkDataFromKafka(fileStorageService, EDI_CONTENT_TYPE_RAW, TENANT_ID_TEST_EDI_RAW, 1);
      testContext.completeNow();
    })));
  }

  @DisplayName("should return error when reading EDIFACT TXT file with MARC job profile")
  @Test
  void shouldReturnError_whenReadingEdifactTxtFileWithMarcJobProfile(VertxTestContext testContext) {
    headers.put(XOkapiHeaders.TENANT, TENANT_ID_TEST_EDI_RAW);

    FileDefinition fileDefinition = createFileDefinition();
    FileStorageService fileStorageService = createFileStorageServiceMock(SOURCE_PATH_7);

    fileProcessor.processFile(
      fileStorageService.getFile(fileDefinition.getSourcePath()),
      fileDefinition.getJobExecutionId(),
      jobProfiles.get(MARC_TYPE_JOB_PROFILE),
      new ConnectionParams(headers)
    ).onComplete(testContext.failing(ar -> testContext.verify(() -> {
      assertErrorFromKafka(fileStorageService, TENANT_ID_TEST_EDI_RAW, "Can not initialize reader");
      testContext.completeNow();
    })));
  }

  @DisplayName("should return zero records when file or profile is null")
  @Test
  void shouldReturnZeroRecords_whenFileOrProfileIsNull() {
    assertThat(ParallelFileChunkingProcessor.countTotalRecordsInFile(null, new JobProfileInfo())).isZero();
    assertThat(ParallelFileChunkingProcessor.countTotalRecordsInFile(new File(SOURCE_PATH_1), null)).isZero();
  }

  private FileDefinition createFileDefinition() {
    return new FileDefinition()
      .withSourcePath(StringUtils.EMPTY)
      .withJobExecutionId(UUID.randomUUID().toString());
  }

  private FileStorageService createFileStorageServiceMock(String filePath) {
    FileStorageService fileStorageService = mock(FileStorageService.class);
    when(fileStorageService.getFile(anyString())).thenReturn(new File(filePath));
    return fileStorageService;
  }

  private Map<String, JobProfileInfo> createJobProfilesMap() {
    Map<String, JobProfileInfo> profiles = new HashMap<>();
    profiles.put(MARC_TYPE_JOB_PROFILE, new JobProfileInfo()
      .withId(UUID.randomUUID().toString()).withDataType(JobProfileInfo.DataType.MARC).withName(JOB_PROFILE_NAME));
    profiles.put(EDI_FACT_JOB_PROFILE, new JobProfileInfo()
      .withId(UUID.randomUUID().toString()).withDataType(JobProfileInfo.DataType.EDIFACT).withName(JOB_PROFILE_NAME));
    profiles.put(EMPTY_TYPE_JOB_PROFILE, new JobProfileInfo()
      .withId(UUID.randomUUID().toString()).withName(JOB_PROFILE_NAME));
    return profiles;
  }

  private Properties getConsumerProperties() {
    var properties = new Properties();
    kafkaConfig.getConsumerProps().forEach((key, value) -> {
      if (value != null) {
        properties.put(key, value);
      }
    });
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "ParallelFileChunkingProcessorUnitTest");
    return properties;
  }

  private String getEventPayload(String topicToObserve) {
    try (var kafkaConsumer = new KafkaConsumer<String, String>(getConsumerProperties())) {
      kafkaConsumer.subscribe(List.of(topicToObserve));
      var records = kafkaConsumer.poll(Duration.ofSeconds(60));
      if (records.isEmpty()) {
        throw new IllegalStateException("Expected Kafka event at " + topicToObserve + " but got none");
      }
      Event obtainedEvent = Json.decodeValue(records.iterator().next().value(), Event.class);
      return obtainedEvent.getEventPayload();
    }
  }

  @SneakyThrows
  private void assertInitializationDataFromKafka(String jobExecutionId, String tenantId, int recordNumber) {
    String topicToObserve = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(),
      KafkaTopicNameHelper.getDefaultNameSpace(), tenantId, DI_INITIALIZATION_STARTED.value());
    DataImportInitConfig initConfig = Json.decodeValue(getEventPayload(topicToObserve), DataImportInitConfig.class);

    assertThat(initConfig).isNotNull();
    assertThat(initConfig.getTotalRecords()).isEqualTo(recordNumber);
    assertThat(initConfig.getJobExecutionId()).isEqualTo(jobExecutionId);
  }

  private void assertRawChunkDataFromKafka(FileStorageService fileStorageService, String contentType, String tenantId) {
    assertRawChunkDataFromKafka(fileStorageService, contentType, tenantId, RECORDS_NUMBER);
  }

  @SneakyThrows
  private void assertRawChunkDataFromKafka(FileStorageService fileStorageService, String contentType, String tenantId,
                                           int recordNumber) {
    String topicToObserve = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(),
      KafkaTopicNameHelper.getDefaultNameSpace(), tenantId, DI_RAW_RECORDS_CHUNK_READ.value());
    RawRecordsDto rawRecordsDto = Json.decodeValue(getEventPayload(topicToObserve), RawRecordsDto.class);

    verify(fileStorageService, times(1)).getFile(any());
    assertThat(rawRecordsDto).isNotNull();
    assertThat(rawRecordsDto.getRecordsMetadata().getTotal()).isEqualTo(recordNumber);
    assertThat(rawRecordsDto.getRecordsMetadata().getContentType().value()).isEqualTo(contentType);
  }

  @SneakyThrows
  private void assertErrorFromKafka(FileStorageService fileStorageService, String tenantId, String errorMessage) {
    String topicToObserve = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(),
      KafkaTopicNameHelper.getDefaultNameSpace(), tenantId, DI_ERROR.value());
    DataImportEventPayload dataImportEventPayload =
      Json.decodeValue(getEventPayload(topicToObserve), DataImportEventPayload.class);

    verify(fileStorageService, times(1)).getFile(any());
    assertThat(dataImportEventPayload).isNotNull();
    assertThat(dataImportEventPayload.getEventType()).isEqualTo(DI_ERROR.value());
    String error = dataImportEventPayload.getContext().get("ERROR");
    assertThat(error).isNotNull().contains(errorMessage);
  }
}
