package org.folio.service.processing;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import net.mguenther.kafka.junit.ObserveKeyValues;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.rest.AbstractRestTest;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.service.storage.FileStorageService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_TOKEN_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_READ;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Testing ParallelFileChunkingProcessor
 */
@RunWith(VertxUnitRunner.class)
public class ParallelFileChunkingProcessorUnitTest extends AbstractRestTest {
  private static final String TOKEN = "token";
  private static final String KAFKA_ENV = "test-env";
  private static final String TENANT_ID = "diku";
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
  private static final String LOCAL_HOST = "http://localhost:";
  private static final String KAFKA_HOST_PROP_NAME = "KAFKA_HOST";
  private static final String KAFKA_PORT_PROP_NAME = "KAFKA_PORT";
  private static final String KAFKA_MAX_REQUEST_SIZE = "MAX_REQUEST_SIZE";
  public static final String EXCEPTION_OCCURRED_WHILE_GETTING_RECORDERS_FROM_KAFKA = "Exception occurred while getting recorders from kafka {}";

  private static final int RECORDS_NUMBER = 62;
  private static final Logger LOGGER = LogManager.getLogger();
  private final Map<String, String> okapiHeaders = new HashMap<>();
  private final Vertx vertx = Vertx.vertx();
  private ParallelFileChunkingProcessor fileProcessor;
  private KafkaConfig kafkaConfig;
  private Map<String, JobProfileInfo> jobProfiles;

  @Before
  public void setUp() {
    int okapiPort = mockServer.port();
    okapiHeaders.put(OKAPI_URL_HEADER, LOCAL_HOST + okapiPort);
    okapiHeaders.put(OKAPI_TENANT_HEADER, TENANT_ID);
    okapiHeaders.put(OKAPI_TOKEN_HEADER, TOKEN);

    kafkaConfig = KafkaConfig.builder()
      .kafkaHost(System.getProperty(KAFKA_HOST_PROP_NAME))
      .kafkaPort(System.getProperty(KAFKA_PORT_PROP_NAME))
      .envId(KAFKA_ENV)
      .maxRequestSize(Integer.parseInt(System.getProperty(KAFKA_MAX_REQUEST_SIZE)))
      .okapiUrl(LOCAL_HOST + okapiPort)
      .build();

    jobProfiles = createJobProfilesMap();
    fileProcessor = new ParallelFileChunkingProcessor(Vertx.vertx(), kafkaConfig);
  }

  @Test
  public void shouldReadMarcBibAndSendAllChunks(TestContext context) {
    readAndSendAllChunks(context);
  }

  private void readAndSendAllChunks(TestContext context) {
    // given
    Async async = context.async();
    okapiHeaders.put(OKAPI_TENANT_HEADER, TENANT_ID_TEST_MARC_RAW);

    FileDefinition fileDefinition = createFileDefinition();
    JobProfileInfo jobProfile = jobProfiles.get(MARC_TYPE_JOB_PROFILE);
    FileStorageService fileStorageService = createFileStorageServiceMock(SOURCE_PATH_1);

    // when
    Future<Void> future = fileProcessor
      .processFile(fileDefinition, jobProfile, fileStorageService, new OkapiConnectionParams(okapiHeaders, vertx));

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      assertDataFromKafka(fileStorageService, CONTENT_TYPE_RAW, TENANT_ID_TEST_MARC_RAW);
      async.complete();
    });
  }

  @Test
  public void shouldErrorOnJobProfileAsNull(TestContext context) {
    // given
    Async async = context.async();
    FileDefinition fileDefinition = createFileDefinition();
    FileStorageService fileStorageService = createFileStorageServiceMock(SOURCE_PATH_1);
    // when
    Future<Void> future = fileProcessor
      .processFile(fileDefinition, null, fileStorageService, new OkapiConnectionParams(okapiHeaders, vertx));
    // then
    future.onComplete(ar -> {
      assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldErrorIfJobProfileInfoWithoutDataType(TestContext context) {
    // given
    Async async = context.async();
    FileDefinition fileDefinition = createFileDefinition();
    JobProfileInfo jobProfile = jobProfiles.get(EMPTY_TYPE_JOB_PROFILE);
    FileStorageService fileStorageService = createFileStorageServiceMock(SOURCE_PATH_1);

    // when
    Future<Void> future = fileProcessor
      .processFile(fileDefinition, jobProfile, fileStorageService, new OkapiConnectionParams(okapiHeaders, vertx));

    // then
    future.onComplete(ar -> {
      assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldErrorIfJobProfileInfoWithoutDataType2(TestContext context) {
    // given
    Async async = context.async();
    FileDefinition fileDefinition = createFileDefinition();
    JobProfileInfo jobProfile = jobProfiles.get(EDI_FACT_JOB_PROFILE);
    FileStorageService fileStorageService = createFileStorageServiceMock(SOURCE_PATH_1);

    // when
    Future<Void> future = fileProcessor
      .processFile(fileDefinition, jobProfile, fileStorageService, new OkapiConnectionParams(okapiHeaders, vertx));

    // then
    future.onComplete(ar -> {
      assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldReadJsonArrayFileAndSendAllChunks(TestContext context) {
    // given
    Async async = context.async();
    okapiHeaders.put(OKAPI_TENANT_HEADER, TENANT_ID_TEST_MARC_JSON);

    FileDefinition fileDefinition = createFileDefinition();
    JobProfileInfo jobProfile = jobProfiles.get(MARC_TYPE_JOB_PROFILE);
    FileStorageService fileStorageService = createFileStorageServiceMock(SOURCE_PATH_2);

    // when
    Future<Void> future = fileProcessor
      .processFile(fileDefinition, jobProfile, fileStorageService, new OkapiConnectionParams(okapiHeaders, vertx));

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      assertDataFromKafka(fileStorageService, CONTENT_TYPE_JSON, TENANT_ID_TEST_MARC_JSON);
      async.complete();
    });
  }

  @Test
  public void shouldReadXmlArrayFileAndSendAllChunks(TestContext context) {
    // given
    Async async = context.async();
    okapiHeaders.put(OKAPI_TENANT_HEADER, TENANT_ID_TEST_MARC_XML);

    FileDefinition fileDefinition = createFileDefinition();
    JobProfileInfo jobProfile = jobProfiles.get(MARC_TYPE_JOB_PROFILE);
    FileStorageService fileStorageService = createFileStorageServiceMock(SOURCE_PATH_4);

    // when
    Future<Void> future = fileProcessor
      .processFile(fileDefinition, jobProfile, fileStorageService, new OkapiConnectionParams(okapiHeaders, vertx));

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      assertDataFromKafka(fileStorageService, CONTENT_TYPE_XML, TENANT_ID_TEST_MARC_XML);
      async.complete();
    });
  }

  @Test
  public void shouldReturnErrorOnMalformedFile(TestContext context) {
    // given
    Async async = context.async();
    FileDefinition fileDefinition = createFileDefinition();
    JobProfileInfo jobProfile = jobProfiles.get(MARC_TYPE_JOB_PROFILE);
    FileStorageService fileStorageService = createFileStorageServiceMock(SOURCE_PATH_3);

    // when
    Future<Void> future = fileProcessor
      .processFile(fileDefinition, jobProfile, fileStorageService, new OkapiConnectionParams(okapiHeaders, vertx));

    // then
    future.onComplete(ar -> {
      assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldReturnErrorOnMalformedXmlFile(TestContext context) {
    // given
    Async async = context.async();
    FileDefinition fileDefinition = createFileDefinition();
    JobProfileInfo jobProfile = jobProfiles.get(MARC_TYPE_JOB_PROFILE);
    FileStorageService fileStorageService = createFileStorageServiceMock(SOURCE_PATH_5);

    // when
    Future<Void> future = fileProcessor
      .processFile(fileDefinition, jobProfile, fileStorageService, new OkapiConnectionParams(okapiHeaders, vertx));

    // then
    future.onComplete(ar -> {
      assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldReturnErrorOnInvalidMrcFile(TestContext context) {
    // given
    Async async = context.async();
    FileDefinition fileDefinition = createFileDefinition();
    JobProfileInfo jobProfile = jobProfiles.get(MARC_TYPE_JOB_PROFILE);
    FileStorageService fileStorageService = createFileStorageServiceMock(SOURCE_PATH_6);

    // when
    Future<Void> future = fileProcessor
      .processFile(fileDefinition, jobProfile, fileStorageService, new OkapiConnectionParams(okapiHeaders, vertx));

    // then
    future.onComplete(ar -> {
      assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void readTXTfileWithEDIFACTJobProfile(TestContext context) {
    // given
    Async async = context.async();
    okapiHeaders.put(OKAPI_TENANT_HEADER, TENANT_ID_TEST_EDI_RAW);

    FileDefinition fileDefinition = createFileDefinition();
    JobProfileInfo jobProfile = jobProfiles.get(EDI_FACT_JOB_PROFILE);
    FileStorageService fileStorageService = createFileStorageServiceMock(SOURCE_PATH_7);

    // when
    Future<Void> future = fileProcessor
      .processFile(fileDefinition, jobProfile, fileStorageService, new OkapiConnectionParams(okapiHeaders, vertx));

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      assertDataFromKafka(fileStorageService, EDI_CONTENT_TYPE_RAW, TENANT_ID_TEST_EDI_RAW, 1);
      async.complete();
    });
  }

  @Test
  public void shouldReturnErrorWhenReadEdifactTXTfileWithMARCJobProfile(TestContext context) {
    // given
    Async async = context.async();
    okapiHeaders.put(OKAPI_TENANT_HEADER, TENANT_ID_TEST_EDI_RAW);

    FileDefinition fileDefinition = createFileDefinition();
    JobProfileInfo jobProfile = jobProfiles.get(MARC_TYPE_JOB_PROFILE);
    FileStorageService fileStorageService = createFileStorageServiceMock(SOURCE_PATH_7);

    // when
    Future<Void> future = fileProcessor
      .processFile(fileDefinition, jobProfile, fileStorageService, new OkapiConnectionParams(okapiHeaders, vertx));

    // then
    future.onComplete(ar -> {
      context.assertFalse(ar.succeeded());
      assertErrorFromKafka(fileStorageService, TENANT_ID_TEST_EDI_RAW, "Can not initialize reader");
      async.complete();
    });
  }

  private FileDefinition createFileDefinition() {
    String stubSourcePath = StringUtils.EMPTY;
    String jobExecutionId = UUID.randomUUID().toString();
    return new FileDefinition()
      .withSourcePath(stubSourcePath)
      .withJobExecutionId(jobExecutionId);
  }

  private FileStorageService createFileStorageServiceMock(String filePath) {
    FileStorageService fileStorageService = Mockito.mock(FileStorageService.class);
    when(fileStorageService.getFile(anyString())).thenReturn(new File(filePath));
    return fileStorageService;
  }

  private Map<String, JobProfileInfo> createJobProfilesMap() {
    Map<String, JobProfileInfo> profiles = new HashMap<>();

    JobProfileInfo marcJobProfileValue = new JobProfileInfo()
      .withId(UUID.randomUUID().toString())
      .withDataType(JobProfileInfo.DataType.MARC)
      .withName(JOB_PROFILE_NAME);
    JobProfileInfo ediFactJobProfileValue = new JobProfileInfo()
      .withId(UUID.randomUUID().toString())
      .withDataType(JobProfileInfo.DataType.EDIFACT)
      .withName(JOB_PROFILE_NAME);
    JobProfileInfo emptyTypeJobProfileValue = new JobProfileInfo()
      .withId(UUID.randomUUID().toString())
      .withName(JOB_PROFILE_NAME);
    profiles.put(MARC_TYPE_JOB_PROFILE, marcJobProfileValue);
    profiles.put(EDI_FACT_JOB_PROFILE, ediFactJobProfileValue);
    profiles.put(EMPTY_TYPE_JOB_PROFILE, emptyTypeJobProfileValue);

    return profiles;
  }

  private void assertDataFromKafka(FileStorageService fileStorageService, String contentType, String tenantId) {
    assertDataFromKafka(fileStorageService, contentType, tenantId, RECORDS_NUMBER);
  }

  private void assertDataFromKafka(FileStorageService fileStorageService, String contentType, String tenantId, int recordNumber) {
    String topicToObserve = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(),
      KafkaTopicNameHelper.getDefaultNameSpace(), tenantId, DI_RAW_RECORDS_CHUNK_READ.value());
    List<String> observedValues = null;
    try {
      observedValues = kafkaCluster.observeValues(ObserveKeyValues.on(topicToObserve, 1)
        .observeFor(60, TimeUnit.SECONDS)
        .build());
    } catch (InterruptedException e) {
      LOGGER.error(EXCEPTION_OCCURRED_WHILE_GETTING_RECORDERS_FROM_KAFKA, e.getMessage());
    }
    Event obtainedEvent = Json.decodeValue(observedValues.get(0), Event.class);
    RawRecordsDto rawRecordsDto = Json.decodeValue(obtainedEvent.getEventPayload(), RawRecordsDto.class);
    verify(fileStorageService, times(1)).getFile(any());

    Assert.assertNotNull(rawRecordsDto);
    Assert.assertEquals(Integer.valueOf(recordNumber), rawRecordsDto.getRecordsMetadata().getTotal());
    Assert.assertEquals(contentType, rawRecordsDto.getRecordsMetadata().getContentType().value());
  }

  private void assertErrorFromKafka(FileStorageService fileStorageService, String tenantId, String errorMessage) {
    String topicToObserve = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(),
      KafkaTopicNameHelper.getDefaultNameSpace(), tenantId, DI_ERROR.value());
    List<String> observedValues = null;
    try {
      observedValues = kafkaCluster.observeValues(ObserveKeyValues.on(topicToObserve, 1)
        .observeFor(60, TimeUnit.SECONDS)
        .build());
    } catch (InterruptedException e) {
      LOGGER.error(EXCEPTION_OCCURRED_WHILE_GETTING_RECORDERS_FROM_KAFKA, e.getMessage());
    }
    Event obtainedEvent = Json.decodeValue(observedValues.get(0), Event.class);
    DataImportEventPayload dataImportEventPayload = Json.decodeValue(obtainedEvent.getEventPayload(), DataImportEventPayload.class);

    verify(fileStorageService, times(1)).getFile(any());

    Assert.assertNotNull(dataImportEventPayload);
    Assert.assertEquals(DI_ERROR.value(), dataImportEventPayload.getEventType());
    String error = dataImportEventPayload.getContext().get("ERROR");
    Assert.assertNotNull(error);
    Assert.assertTrue(error.contains(errorMessage));
  }
}
