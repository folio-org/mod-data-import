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
import org.folio.rest.jaxrs.model.JobProfile;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Testing ParallelFileChunkingProcessor
 */
@RunWith(VertxUnitRunner.class)
public class ParallelFileChunkingProcessorUnitTest {

  private static final String TENANT = "diku";
  private static final String TOKEN = "token";

  private static final String RAW_RECORDS_SERVICE_URL = "/change-manager/records/";
  private static final String SOURCE_PATH = "src/test/resources/CornellFOLIOExemplars.mrc";
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
    WireMock.stubFor(WireMock.post(RAW_RECORDS_SERVICE_URL + jobExecutionId)
      .willReturn(WireMock.ok()));

    FileDefinition fileDefinition = new FileDefinition()
      .withSourcePath(stubSourcePath)
      .withJobExecutionId(jobExecutionId);
    JobProfile jobProfile = new JobProfile();
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(headers, vertx);

    FileStorageService fileStorageService = Mockito.mock(FileStorageService.class);
    when(fileStorageService.getFile(anyString())).thenReturn(new File(SOURCE_PATH));

    // We need +1 additional request to post the last chunk with total records number
    int expectedRequestsNumber = CHUNKS_NUMBER + 1;

    /* when */
    Future<Void> future = fileProcessor.processFile(fileDefinition, jobProfile, fileStorageService, okapiConnectionParams);

    /* then */
    future.setHandler(ar -> {
      Assert.assertTrue(ar.succeeded());
      List<LoggedRequest> requests = WireMock.findAll(RequestPatternBuilder.allRequests());
      Assert.assertEquals(expectedRequestsNumber, requests.size());

      int actualTotalRecordsNumber = 0;
      int actualLastChunkRecordsCounter = 0;
      for (LoggedRequest loggedRequest : requests) {
        RawRecordsDto rawRecordsDto = new JsonObject(loggedRequest.getBodyAsString()).mapTo(RawRecordsDto.class);
        actualTotalRecordsNumber += rawRecordsDto.getRecords().size();
        if (rawRecordsDto.getLast()) {
          actualLastChunkRecordsCounter = rawRecordsDto.getCounter();
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
    WireMock.stubFor(WireMock.post(RAW_RECORDS_SERVICE_URL + jobExecutionId)
      .willReturn(WireMock.serverError()));

    FileDefinition fileDefinition = new FileDefinition()
      .withSourcePath(stubSourcePath)
      .withJobExecutionId(jobExecutionId);
    JobProfile jobProfile = new JobProfile();
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(headers, vertx);

    FileStorageService fileStorageService = Mockito.mock(FileStorageService.class);
    when(fileStorageService.getFile(anyString())).thenReturn(new File(SOURCE_PATH));

    /* when */
    Future<Void> future = fileProcessor.processFile(fileDefinition, jobProfile, fileStorageService, okapiConnectionParams);

    /* then */
    future.setHandler(ar -> {
      Assert.assertTrue(ar.failed());
      List<LoggedRequest> requests = WireMock.findAll(RequestPatternBuilder.allRequests());
      Assert.assertTrue(!requests.isEmpty());
      async.complete();
    });
  }
}
