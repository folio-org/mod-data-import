package org.folio.service.processing;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.drools.core.util.StringUtils;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.service.storage.FileStorageService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.Spy;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_TOKEN_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.jaxrs.model.RecordsMetadata.ContentType.MARC_JSON;
import static org.folio.rest.jaxrs.model.RecordsMetadata.ContentType.MARC_RAW;
import static org.folio.rest.jaxrs.model.RecordsMetadata.ContentType.MARC_XML;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Testing ParallelFileChunkingProcessor
 */
@RunWith(VertxUnitRunner.class)
public class ParallelFileChunkingProcessorUnitTest {

  private static final String TENANT = "diku";
  private static final String TOKEN = "token";

  private static final String RAW_RECORDS_SERVICE_URL = "/change-manager/jobExecutions/%s/records";
  private static final String SOURCE_PATH = "src/test/resources/CornellFOLIOExemplars.mrc";
  private static final String SOURCE_PATH_3 = "src/test/resources/ChalmersFOLIOExamples.json";
  private static final String SOURCE_PATH_4 = "src/test/resources/invalidJsonExample.json";
  private static final String SOURCE_PATH_5 = "src/test/resources/UChicago_SampleBibs.xml";
  private static final String SOURCE_PATH_6 = "src/test/resources/invalidUChicago_SampleBibs.xml";

  private static final int RECORDS_NUMBER = 62;
  private static final int CHUNKS_NUMBER = 2;

  @Spy
  private HttpClient httpClient = Vertx.vertx().createHttpClient();
  @InjectMocks
  private ParallelFileChunkingProcessor fileProcessor = new ParallelFileChunkingProcessor();
  private Map<String, String> headers = new HashMap<>();
  private Vertx vertx = Vertx.vertx();

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  @Before
  public void setUp() {
    headers.put(OKAPI_URL_HEADER, "http://localhost:" + mockServer.port());
    headers.put(OKAPI_TENANT_HEADER, TENANT);
    headers.put(OKAPI_TOKEN_HEADER, TOKEN);
  }

  @Test
  public void shouldReadAndSendAllChunks(TestContext context) {
    /* given */
    Async async = context.async();
    String stubSourcePath = StringUtils.EMPTY;

    String jobExecutionId = UUID.randomUUID().toString();
    WireMock.stubFor(WireMock.post(String.format(RAW_RECORDS_SERVICE_URL, jobExecutionId))
      .willReturn(WireMock.noContent()));

    FileDefinition fileDefinition = new FileDefinition()
      .withSourcePath(stubSourcePath)
      .withJobExecutionId(jobExecutionId);
    JobProfileInfo jobProfile = new JobProfileInfo()
      .withId(UUID.randomUUID().toString())
      .withDataType(JobProfileInfo.DataType.MARC)
      .withName("MARC profile");
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(headers, vertx);

    FileStorageService fileStorageService = Mockito.mock(FileStorageService.class);
    when(fileStorageService.getFile(anyString())).thenReturn(new File(SOURCE_PATH));

    // We need +1 additional request to post the last chunk with total records number
    int expectedRequestsNumber = CHUNKS_NUMBER + 1;

    /* when */
    Future<Void> future = fileProcessor.processFile(fileDefinition, jobProfile, fileStorageService, okapiConnectionParams);

    /* then */
    future.setHandler(ar -> {
      assertTrue(ar.succeeded());
      List<LoggedRequest> requests = WireMock.findAll(RequestPatternBuilder.allRequests());
      Assert.assertEquals(expectedRequestsNumber, requests.size());

      int actualTotalRecordsNumber = 0;
      int actualLastChunkRecordsCounter = 0;
      for (LoggedRequest loggedRequest : requests) {
        RawRecordsDto rawRecordsDto = new JsonObject(loggedRequest.getBodyAsString()).mapTo(RawRecordsDto.class);
        assertSame(MARC_RAW, rawRecordsDto.getRecordsMetadata().getContentType());
        actualTotalRecordsNumber += rawRecordsDto.getRecords().size();
        if (rawRecordsDto.getRecordsMetadata().getLast()) {
          actualLastChunkRecordsCounter = rawRecordsDto.getRecordsMetadata().getCounter();
        }
      }
      Assert.assertEquals(expectedRequestsNumber, requests.size());
      Assert.assertEquals(RECORDS_NUMBER, actualLastChunkRecordsCounter);
      Assert.assertEquals(RECORDS_NUMBER, actualTotalRecordsNumber);
      async.complete();
    });
  }

  @Test
  public void shouldReadAndStopSendingChunksOnServerError(TestContext context) {
    /* given */
    Async async = context.async();
    String stubSourcePath = StringUtils.EMPTY;
    String jobExecutionId = UUID.randomUUID().toString();
    WireMock.stubFor(WireMock.post(String.format(RAW_RECORDS_SERVICE_URL, jobExecutionId))
      .willReturn(WireMock.serverError()));
    FileDefinition fileDefinition = new FileDefinition()
      .withSourcePath(stubSourcePath)
      .withJobExecutionId(jobExecutionId);
    JobProfileInfo jobProfile = new JobProfileInfo()
      .withId(UUID.randomUUID().toString())
      .withDataType(JobProfileInfo.DataType.MARC)
      .withName("MARC profile");
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(headers, vertx);
    FileStorageService fileStorageService = Mockito.mock(FileStorageService.class);
    when(fileStorageService.getFile(anyString())).thenReturn(new File(SOURCE_PATH));
    /* when */
    Future<Void> future = fileProcessor.processFile(fileDefinition, jobProfile, fileStorageService, okapiConnectionParams);
    /* then */
    future.setHandler(ar -> {
      assertTrue(ar.failed());
      List<LoggedRequest> requests = WireMock.findAll(RequestPatternBuilder.allRequests());
      assertFalse(requests.isEmpty());
      async.complete();
    });
  }

  @Test
  public void shouldErrorOnNull(TestContext context) {
    /* given */
    Async async = context.async();
    String stubSourcePath = StringUtils.EMPTY;
    String jobExecutionId = UUID.randomUUID().toString();
    WireMock.stubFor(WireMock.post(String.format(RAW_RECORDS_SERVICE_URL, jobExecutionId))
      .willReturn(WireMock.serverError()));
    FileDefinition fileDefinition = new FileDefinition()
      .withSourcePath(stubSourcePath)
      .withJobExecutionId(jobExecutionId);
    FileStorageService fileStorageService = Mockito.mock(FileStorageService.class);
    when(fileStorageService.getFile(anyString())).thenReturn(new File(SOURCE_PATH));
    /* when */
    Future<Void> future = fileProcessor.processFile(fileDefinition, null, null, null);
    /* then */
    future.setHandler(ar -> {
      assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldErrorOnNull2(TestContext context) {
    /* given */
    Async async = context.async();
    String stubSourcePath = StringUtils.EMPTY;
    String jobExecutionId = UUID.randomUUID().toString();
    WireMock.stubFor(WireMock.post(String.format(RAW_RECORDS_SERVICE_URL, jobExecutionId))
      .willReturn(WireMock.serverError()));
    FileDefinition fileDefinition = new FileDefinition()
      .withSourcePath(stubSourcePath)
      .withJobExecutionId(jobExecutionId);
    JobProfileInfo jobProfile = new JobProfileInfo()
      .withId(UUID.randomUUID().toString())
      .withDataType(JobProfileInfo.DataType.MARC)
      .withName("MARC profile");
    FileStorageService fileStorageService = Mockito.mock(FileStorageService.class);
    when(fileStorageService.getFile(anyString())).thenReturn(new File(SOURCE_PATH));
    /* when */
    Future<Void> future = fileProcessor.processFile(fileDefinition, jobProfile, fileStorageService, null);
    /* then */
    future.setHandler(ar -> {
      assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldErrorOnNull3(TestContext context) {
    /* given */
    Async async = context.async();
    String stubSourcePath = StringUtils.EMPTY;
    String jobExecutionId = UUID.randomUUID().toString();
    WireMock.stubFor(WireMock.post(String.format(RAW_RECORDS_SERVICE_URL, jobExecutionId))
      .willReturn(WireMock.serverError()));
    FileDefinition fileDefinition = new FileDefinition()
      .withSourcePath(stubSourcePath)
      .withJobExecutionId(jobExecutionId);
    JobProfileInfo jobProfile = new JobProfileInfo()
      .withId(UUID.randomUUID().toString())
      .withDataType(JobProfileInfo.DataType.MARC)
      .withName("MARC profile");
    /* when */
    Future<Void> future = fileProcessor.processFile(fileDefinition, jobProfile, null, null);

    /* then */
    future.setHandler(ar -> {
      assertTrue(ar.failed());
      async.complete();
    });
  }


  @Test
  public void shouldReadAndStopSendingChunksOnServerError2(TestContext context) {
    /* given */
    Async async = context.async();
    String stubSourcePath = StringUtils.EMPTY;

    String jobExecutionId = UUID.randomUUID().toString();
    WireMock.stubFor(WireMock.post(String.format(RAW_RECORDS_SERVICE_URL, jobExecutionId))
      .willReturn(WireMock.notFound()));

    FileDefinition fileDefinition = new FileDefinition()
      .withSourcePath(stubSourcePath)
      .withJobExecutionId(jobExecutionId);
    JobProfileInfo jobProfile = new JobProfileInfo()
      .withId(UUID.randomUUID().toString())
      .withDataType(JobProfileInfo.DataType.MARC)
      .withName("MARC profile");
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(headers, vertx);

    FileStorageService fileStorageService = Mockito.mock(FileStorageService.class);
    when(fileStorageService.getFile(anyString())).thenReturn(new File(SOURCE_PATH));

    /* when */
    Future<Void> future = fileProcessor.processFile(fileDefinition, jobProfile, fileStorageService, okapiConnectionParams);

    /* then */
    future.setHandler(ar -> {
      assertTrue(ar.failed());
      List<LoggedRequest> requests = WireMock.findAll(RequestPatternBuilder.allRequests());
      assertFalse(requests.isEmpty());
      async.complete();
    });
  }

  @Test
  public void shouldReadAndError(TestContext context) {
    /* given */
    Async async = context.async();
    String stubSourcePath = StringUtils.EMPTY;

    String jobExecutionId = UUID.randomUUID().toString();
    WireMock.stubFor(WireMock.post(String.format(RAW_RECORDS_SERVICE_URL, jobExecutionId))
      .willReturn(WireMock.serverError()));

    FileDefinition fileDefinition = new FileDefinition()
      .withSourcePath(stubSourcePath)
      .withJobExecutionId(jobExecutionId);
    JobProfileInfo jobProfile = new JobProfileInfo()
      .withId(UUID.randomUUID().toString())
      .withDataType(JobProfileInfo.DataType.MARC)
      .withName("MARC profile");
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(headers, vertx);

    FileStorageService fileStorageService = Mockito.mock(FileStorageService.class);
    when(fileStorageService.getFile(anyString())).thenReturn(new File(SOURCE_PATH));

    /* when */
    Future<Void> future = fileProcessor.processFile(fileDefinition, jobProfile, fileStorageService, okapiConnectionParams);

    /* then */
    future.setHandler(ar -> {
      assertTrue(ar.failed());
      List<LoggedRequest> requests = WireMock.findAll(RequestPatternBuilder.allRequests());
      assertFalse(requests.isEmpty());
      async.complete();
    });
  }

  @Test
  public void shouldErrorIfJobProfileInfoWithoutDataType(TestContext context) {
    /* given */
    Async async = context.async();
    String stubSourcePath = StringUtils.EMPTY;

    String jobExecutionId = UUID.randomUUID().toString();
    WireMock.stubFor(WireMock.post(RAW_RECORDS_SERVICE_URL + jobExecutionId)
      .willReturn(WireMock.serverError()));

    FileDefinition fileDefinition = new FileDefinition()
      .withSourcePath(stubSourcePath)
      .withJobExecutionId(jobExecutionId);
    JobProfileInfo jobProfile = new JobProfileInfo()
      .withId(UUID.randomUUID().toString())
      .withName("MARC profile");
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(headers, vertx);

    FileStorageService fileStorageService = Mockito.mock(FileStorageService.class);
    when(fileStorageService.getFile(anyString())).thenReturn(new File(SOURCE_PATH));

    /* when */
    Future<Void> future = fileProcessor.processFile(fileDefinition, jobProfile, fileStorageService, okapiConnectionParams);

    /* then */
    future.setHandler(ar -> {
      assertTrue(ar.failed());
      List<LoggedRequest> requests = WireMock.findAll(RequestPatternBuilder.allRequests());
      assertTrue(requests.isEmpty());
      async.complete();
    });
  }

  @Test
  public void shouldErrorIfJobProfileInfoWithoutDataType2(TestContext context) {
    /* given */
    Async async = context.async();
    String stubSourcePath = StringUtils.EMPTY;

    String jobExecutionId = UUID.randomUUID().toString();
    WireMock.stubFor(WireMock.post(RAW_RECORDS_SERVICE_URL + jobExecutionId)
      .willReturn(WireMock.serverError()));

    FileDefinition fileDefinition = new FileDefinition()
      .withSourcePath(stubSourcePath)
      .withJobExecutionId(jobExecutionId);
    JobProfileInfo jobProfile = new JobProfileInfo()
      .withId(UUID.randomUUID().toString())
      .withDataType(JobProfileInfo.DataType.EDIFACT)
      .withName("MARC profile");
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(headers, vertx);

    FileStorageService fileStorageService = Mockito.mock(FileStorageService.class);
    when(fileStorageService.getFile(anyString())).thenReturn(new File(SOURCE_PATH));

    /* when */
    Future<Void> future = fileProcessor.processFile(fileDefinition, jobProfile, fileStorageService, okapiConnectionParams);

    /* then */
    future.setHandler(ar -> {
      assertTrue(ar.failed());
      List<LoggedRequest> requests = WireMock.findAll(RequestPatternBuilder.allRequests());
      assertTrue(requests.isEmpty());
      async.complete();
    });
  }

  @Test
  public void shouldReadJsonArrayFileAndSendAllChunks(TestContext context) {
    /* given */
    Async async = context.async();
    String stubSourcePath = StringUtils.EMPTY;

    String jobExecutionId = UUID.randomUUID().toString();
    WireMock.stubFor(WireMock.post(String.format(RAW_RECORDS_SERVICE_URL, jobExecutionId))
      .willReturn(WireMock.noContent()));

    FileDefinition fileDefinition = new FileDefinition()
      .withSourcePath(stubSourcePath)
      .withJobExecutionId(jobExecutionId);
    JobProfileInfo jobProfile = new JobProfileInfo()
      .withId(UUID.randomUUID().toString())
      .withDataType(JobProfileInfo.DataType.MARC)
      .withName("MARC profile");
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(headers, vertx);

    FileStorageService fileStorageService = Mockito.mock(FileStorageService.class);
    when(fileStorageService.getFile(anyString())).thenReturn(new File(SOURCE_PATH_3));

    // We need +1 additional request to post the last chunk with total records number
    int expectedRequestsNumber = CHUNKS_NUMBER + 1;

    /* when */
    Future<Void> future = fileProcessor.processFile(fileDefinition, jobProfile, fileStorageService, okapiConnectionParams);

    /* then */
    future.setHandler(ar -> {
      assertTrue(ar.succeeded());
      List<LoggedRequest> requests = WireMock.findAll(RequestPatternBuilder.allRequests());
      Assert.assertEquals(expectedRequestsNumber, requests.size());

      int actualTotalRecordsNumber = 0;
      int actualLastChunkRecordsCounter = 0;
      for (LoggedRequest loggedRequest : requests) {
        RawRecordsDto rawRecordsDto = new JsonObject(loggedRequest.getBodyAsString()).mapTo(RawRecordsDto.class);
        assertSame(MARC_JSON, rawRecordsDto.getRecordsMetadata().getContentType());
        actualTotalRecordsNumber += rawRecordsDto.getRecords().size();
        if (rawRecordsDto.getRecordsMetadata().getLast()) {
          actualLastChunkRecordsCounter = rawRecordsDto.getRecordsMetadata().getCounter();
        }
      }
      Assert.assertEquals(expectedRequestsNumber, requests.size());
      Assert.assertEquals(RECORDS_NUMBER, actualLastChunkRecordsCounter);
      Assert.assertEquals(RECORDS_NUMBER, actualTotalRecordsNumber);
      async.complete();
    });
  }

  @Test
  public void shouldReturnErrorOnMalformedFile(TestContext context) {
    /* given */
    Async async = context.async();
    String stubSourcePath = StringUtils.EMPTY;

    String jobExecutionId = UUID.randomUUID().toString();
    WireMock.stubFor(WireMock.post(String.format(RAW_RECORDS_SERVICE_URL, jobExecutionId))
      .willReturn(WireMock.noContent()));

    FileDefinition fileDefinition = new FileDefinition()
      .withSourcePath(stubSourcePath)
      .withJobExecutionId(jobExecutionId);
    JobProfileInfo jobProfile = new JobProfileInfo()
      .withId(UUID.randomUUID().toString())
      .withDataType(JobProfileInfo.DataType.MARC)
      .withName("MARC profile");
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(headers, vertx);

    FileStorageService fileStorageService = Mockito.mock(FileStorageService.class);
    when(fileStorageService.getFile(anyString())).thenReturn(new File(SOURCE_PATH_4));

    /* when */
    Future<Void> future = fileProcessor.processFile(fileDefinition, jobProfile, fileStorageService, okapiConnectionParams);

    /* then */
    future.setHandler(ar -> {
      assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldReadXmlArrayFileAndSendAllChunks(TestContext context) {
    /* given */
    Async async = context.async();
    String stubSourcePath = StringUtils.EMPTY;

    String jobExecutionId = UUID.randomUUID().toString();
    WireMock.stubFor(WireMock.post(String.format(RAW_RECORDS_SERVICE_URL, jobExecutionId))
      .willReturn(WireMock.noContent()));

    FileDefinition fileDefinition = new FileDefinition()
      .withSourcePath(stubSourcePath)
      .withJobExecutionId(jobExecutionId);
    JobProfileInfo jobProfile = new JobProfileInfo()
      .withId(UUID.randomUUID().toString())
      .withDataType(JobProfileInfo.DataType.MARC)
      .withName("MARC profile");
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(headers, vertx);

    FileStorageService fileStorageService = Mockito.mock(FileStorageService.class);
    when(fileStorageService.getFile(anyString())).thenReturn(new File(SOURCE_PATH_5));

    // We need +1 additional request to post the last chunk with total records number
    int expectedRequestsNumber = CHUNKS_NUMBER + 1;

    /* when */
    Future<Void> future = fileProcessor.processFile(fileDefinition, jobProfile, fileStorageService, okapiConnectionParams);

    /* then */
    future.setHandler(ar -> {
      assertTrue(ar.succeeded());
      List<LoggedRequest> requests = WireMock.findAll(RequestPatternBuilder.allRequests());
      Assert.assertEquals(expectedRequestsNumber, requests.size());

      int actualTotalRecordsNumber = 0;
      int actualLastChunkRecordsCounter = 0;
      for (LoggedRequest loggedRequest : requests) {
        RawRecordsDto rawRecordsDto = new JsonObject(loggedRequest.getBodyAsString()).mapTo(RawRecordsDto.class);
        assertSame(MARC_XML, rawRecordsDto.getRecordsMetadata().getContentType());
        actualTotalRecordsNumber += rawRecordsDto.getRecords().size();
        if (rawRecordsDto.getRecordsMetadata().getLast()) {
          actualLastChunkRecordsCounter = rawRecordsDto.getRecordsMetadata().getCounter();
        }
      }
      Assert.assertEquals(expectedRequestsNumber, requests.size());
      Assert.assertEquals(RECORDS_NUMBER, actualLastChunkRecordsCounter);
      Assert.assertEquals(RECORDS_NUMBER, actualTotalRecordsNumber);
      async.complete();
    });
  }

  @Test
  public void shouldReturnErrorOnMalformedXmlFile(TestContext context) {
    /* given */
    Async async = context.async();
    String stubSourcePath = StringUtils.EMPTY;

    String jobExecutionId = UUID.randomUUID().toString();
    WireMock.stubFor(WireMock.post(String.format(RAW_RECORDS_SERVICE_URL, jobExecutionId))
      .willReturn(WireMock.noContent()));

    FileDefinition fileDefinition = new FileDefinition()
      .withSourcePath(stubSourcePath)
      .withJobExecutionId(jobExecutionId);
    JobProfileInfo jobProfile = new JobProfileInfo()
      .withId(UUID.randomUUID().toString())
      .withDataType(JobProfileInfo.DataType.MARC)
      .withName("MARC profile");
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(headers, vertx);

    FileStorageService fileStorageService = Mockito.mock(FileStorageService.class);
    when(fileStorageService.getFile(anyString())).thenReturn(new File(SOURCE_PATH_6));

    /* when */
    Future<Void> future = fileProcessor.processFile(fileDefinition, jobProfile, fileStorageService, okapiConnectionParams);

    /* then */
    future.setHandler(ar -> {
      assertTrue(ar.failed());
      async.complete();
    });
  }

}
