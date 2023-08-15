package org.folio.service.file;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.folio.dao.DataImportQueueItemDao;
import org.folio.dao.DataImportQueueItemDaoImpl;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.AbstractRestTest;
import org.folio.rest.client.ChangeManagerClient;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.JobExecutionDtoCollection;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.helpers.LocalRowSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;

@RunWith(VertxUnitRunner.class)
public class SplitFileProcessingServiceTest extends AbstractRestTest {

  private static final String PARENT_UPLOAD_DEFINITION_ID =
    "parent-upload-definition-id";
  private static final UploadDefinition PARENT_UPLOAD_DEFINITION = new UploadDefinition()
    .withId(PARENT_UPLOAD_DEFINITION_ID);
  private static final UploadDefinition PARENT_UPLOAD_DEFINITION_WITH_USER = PARENT_UPLOAD_DEFINITION.withMetadata(
    new Metadata().withCreatedByUserId("user")
  );

  private static final String PARENT_JOB_EXECUTION_ID =
    "parent-job-execution-id";
  private static final JobExecution PARENT_JOB_EXECUTION = new JobExecution()
    .withId(PARENT_JOB_EXECUTION_ID);

  @Mock
  PostgresClientFactory pgClientFactory;

  @Mock
  PostgresClient postgresClient;

  ChangeManagerClient changeManagerClient;
  DataImportQueueItemDao queueItemDao;
  SplitFileProcessingService service;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    this.changeManagerClient =
      spy(
        new ChangeManagerClient(
          "http://localhost:" + mockServer.port(),
          this.TENANT_ID,
          this.TOKEN
        )
      );

    when(pgClientFactory.getInstance()).thenReturn(postgresClient);

    // handle inserts into the queue
    doAnswer((InvocationOnMock invocation) -> {
        Promise<RowSet<Row>> promise = invocation.getArgument(2);
        promise.complete(new LocalRowSet(1));
        return null;
      })
      .when(postgresClient)
      .execute(
        anyString(),
        any(Tuple.class),
        ArgumentMatchers.<Promise<RowSet<Row>>>any()
      );

    this.queueItemDao = spy(new DataImportQueueItemDaoImpl(pgClientFactory));

    this.service = new SplitFileProcessingService(queueItemDao);
  }

  @Test
  public void testNoSplitRegistration(TestContext context) {
    service
      .registerSplitFiles(
        null,
        null,
        changeManagerClient,
        0,
        TENANT_ID,
        Arrays.asList()
      )
      .onComplete(
        context.asyncAssertSuccess(result -> {
          assertThat(result.list(), is(empty()));
          verifyNoInteractions(changeManagerClient);
          verifyNoInteractions(queueItemDao);
        })
      );
  }

  @Test
  public void testSingleSplitRegistration(TestContext context) {
    WireMock.stubFor(
      WireMock
        .post("/change-manager/jobExecutions")
        .willReturn(
          WireMock
            .created()
            .withBody(
              JsonObject
                .mapFrom(
                  new JobExecutionDtoCollection()
                    .withJobExecutions(
                      Arrays.asList(
                        new JobExecutionDto().withId("test-execution-id")
                      )
                    )
                )
                .encode()
            )
        )
    );

    service
      .registerSplitFiles(
        PARENT_UPLOAD_DEFINITION_WITH_USER,
        PARENT_JOB_EXECUTION,
        changeManagerClient,
        123,
        TENANT_ID,
        Arrays.asList("key1")
      )
      .onComplete(
        context.asyncAssertSuccess(result -> {
          assertThat(result.succeeded(), is(true));
          assertThat(result.list(), hasSize(1));

          JobExecutionDto execution = (JobExecutionDto) result.list().get(0);
          assertThat(execution.getId(), is("test-execution-id"));

          WireMock.verify(
            WireMock.exactly(1),
            WireMock.anyRequestedFor(
              WireMock.urlMatching("/change-manager/jobExecutions")
            )
          );

          verify(changeManagerClient, times(1))
            .postChangeManagerJobExecutions(any(), any());

          verify(queueItemDao, times(1)).addQueueItem(any());
          verifyNoMoreInteractions(queueItemDao);
        })
      );
  }

  @Test
  public void testMultipleSplitRegistration(TestContext context) {
    WireMock.stubFor(
      WireMock
        .post("/change-manager/jobExecutions")
        .inScenario("multiple")
        .whenScenarioStateIs(Scenario.STARTED)
        .willReturn(
          WireMock
            .created()
            .withBody(
              JsonObject
                .mapFrom(
                  new JobExecutionDtoCollection()
                    .withJobExecutions(
                      Arrays.asList(
                        new JobExecutionDto().withId("test-execution-id-1")
                      )
                    )
                )
                .encode()
            )
        )
        .willSetStateTo("success1")
    );

    WireMock.stubFor(
      WireMock
        .post("/change-manager/jobExecutions")
        .inScenario("multiple")
        .whenScenarioStateIs("success1")
        .willReturn(
          WireMock
            .created()
            .withBody(
              JsonObject
                .mapFrom(
                  new JobExecutionDtoCollection()
                    .withJobExecutions(
                      Arrays.asList(
                        new JobExecutionDto().withId("test-execution-id-2")
                      )
                    )
                )
                .encode()
            )
        )
        .willSetStateTo("success2")
    );

    WireMock.stubFor(
      WireMock
        .post("/change-manager/jobExecutions")
        .inScenario("multiple")
        .whenScenarioStateIs("success2")
        .willReturn(
          WireMock
            .created()
            .withBody(
              JsonObject
                .mapFrom(
                  new JobExecutionDtoCollection()
                    .withJobExecutions(
                      Arrays.asList(
                        new JobExecutionDto().withId("test-execution-id-3")
                      )
                    )
                )
                .encode()
            )
        )
        .willSetStateTo("success3")
    );

    service
      .registerSplitFiles(
        PARENT_UPLOAD_DEFINITION_WITH_USER,
        PARENT_JOB_EXECUTION,
        changeManagerClient,
        123,
        TENANT_ID,
        Arrays.asList("key1", "key2", "key3")
      )
      .onComplete(
        context.asyncAssertSuccess(result -> {
          assertThat(result.succeeded(), is(true));
          assertThat(result.list(), hasSize(3));

          assertThat(
            result
              .list()
              .stream()
              .map(JobExecutionDto.class::cast)
              .map(exec -> exec.getId())
              .collect(Collectors.toList()),
            containsInAnyOrder(
              "test-execution-id-1",
              "test-execution-id-2",
              "test-execution-id-3"
            )
          );

          WireMock.verify(
            WireMock.exactly(3),
            WireMock.anyRequestedFor(
              WireMock.urlMatching("/change-manager/jobExecutions")
            )
          );

          verify(changeManagerClient, times(3))
            .postChangeManagerJobExecutions(any(), any());

          verify(queueItemDao, times(3)).addQueueItem(any());
          verifyNoMoreInteractions(queueItemDao);
        })
      );
  }

  @Test
  public void testNoResponseAtAll(TestContext context) {
    service
      .registerSplitFiles(
        PARENT_UPLOAD_DEFINITION_WITH_USER,
        PARENT_JOB_EXECUTION,
        changeManagerClient,
        123,
        TENANT_ID,
        Arrays.asList("key1")
      )
      .onComplete(
        context.asyncAssertSuccess(result -> {
          assertThat(result.succeeded(), is(false));
          assertThat(result.causes(), hasSize(1));

          WireMock.verify(
            WireMock.exactly(1),
            WireMock.anyRequestedFor(
              WireMock.urlMatching("/change-manager/jobExecutions")
            )
          );

          verify(changeManagerClient, times(1))
            .postChangeManagerJobExecutions(any(), any());

          verifyNoMoreInteractions(queueItemDao);
        })
      );
  }

  @Test
  public void testBadResponse(TestContext context) {
    WireMock.stubFor(
      WireMock
        .post("/change-manager/jobExecutions")
        .willReturn(WireMock.serverError())
    );

    service
      .registerSplitFiles(
        PARENT_UPLOAD_DEFINITION_WITH_USER,
        PARENT_JOB_EXECUTION,
        changeManagerClient,
        123,
        TENANT_ID,
        Arrays.asList("key1")
      )
      .onComplete(
        context.asyncAssertSuccess(result -> {
          assertThat(result.succeeded(), is(false));
          assertThat(result.causes(), hasSize(1));

          WireMock.verify(
            WireMock.exactly(1),
            WireMock.anyRequestedFor(
              WireMock.urlMatching("/change-manager/jobExecutions")
            )
          );

          verify(changeManagerClient, times(1))
            .postChangeManagerJobExecutions(any(), any());

          verifyNoMoreInteractions(queueItemDao);
        })
      );
  }
}
