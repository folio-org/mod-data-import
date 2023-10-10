package org.folio.service.file;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.Arrays;
import java.util.Map;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class SplitFileProcessingServiceRegistrationTest
  extends SplitFileProcessingServiceAbstractTest {

  @Test
  public void testNoSplitRegistration(TestContext context) {
    service
      .registerSplitFileParts(
        null,
        null,
        new JobProfileInfo().withDataType(JobProfileInfo.DataType.MARC),
        changeManagerClient,
        0,
        new OkapiConnectionParams(
          Map.of(RestUtil.OKAPI_TENANT_HEADER, TENANT_ID),
          null
        ),
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
                  new InitJobExecutionsRsDto()
                    .withJobExecutions(
                      Arrays.asList(
                        new JobExecution().withId("test-execution-id")
                      )
                    )
                )
                .encode()
            )
        )
    );

    service
      .registerSplitFileParts(
        PARENT_UPLOAD_DEFINITION_WITH_USER,
        PARENT_JOB_EXECUTION,
        new JobProfileInfo().withDataType(JobProfileInfo.DataType.MARC),
        changeManagerClient,
        123,
        new OkapiConnectionParams(
          Map.of(RestUtil.OKAPI_TENANT_HEADER, TENANT_ID),
          null
        ),
        Arrays.asList("key1")
      )
      .onComplete(
        context.asyncAssertSuccess(result -> {
          assertThat(result.succeeded(), is(true));
          assertThat(result.list(), hasSize(1));

          JobExecution execution = (JobExecution) result.list().get(0);
          assertThat(execution.getId(), is("test-execution-id"));

          WireMock.verify(
            WireMock.exactly(1),
            WireMock.anyRequestedFor(
              WireMock.urlMatching("/change-manager/jobExecutions")
            )
          );

          verify(changeManagerClient, times(1))
            .postChangeManagerJobExecutions(any(), any());

          verifyNoInteractions(queueItemDao);
        })
      );
  }

  @Test
  public void testMultipleSplitRegistration(TestContext context) {
    WireMock.stubFor(
      WireMock
        .post("/change-manager/jobExecutions")
        .willReturn(
          WireMock
            .created()
            .withBody(
              JsonObject
                .mapFrom(
                  new InitJobExecutionsRsDto()
                    .withJobExecutions(
                      Arrays.asList(
                        new JobExecution().withId("test-execution-id")
                      )
                    )
                )
                .encode()
            )
        )
    );

    service
      .registerSplitFileParts(
        PARENT_UPLOAD_DEFINITION,
        PARENT_JOB_EXECUTION,
        new JobProfileInfo().withDataType(JobProfileInfo.DataType.MARC),
        changeManagerClient,
        123,
        new OkapiConnectionParams(
          Map.of(RestUtil.OKAPI_TENANT_HEADER, TENANT_ID),
          null
        ),
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
              .map(JobExecution.class::cast)
              .map(exec -> exec.getId())
              .toList(),
            containsInAnyOrder(
              "test-execution-id",
              "test-execution-id",
              "test-execution-id"
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

          verifyNoInteractions(queueItemDao);
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
      .registerSplitFileParts(
        PARENT_UPLOAD_DEFINITION,
        PARENT_JOB_EXECUTION,
        new JobProfileInfo().withDataType(JobProfileInfo.DataType.MARC),
        changeManagerClient,
        123,
        new OkapiConnectionParams(
          Map.of(RestUtil.OKAPI_TENANT_HEADER, TENANT_ID),
          null
        ),
        Arrays.asList("key1")
      )
      .onComplete(
        context.asyncAssertFailure(result -> {
          WireMock.verify(
            WireMock.exactly(1),
            WireMock.anyRequestedFor(
              WireMock.urlMatching("/change-manager/jobExecutions")
            )
          );

          verify(changeManagerClient, times(1))
            .postChangeManagerJobExecutions(any(), any());

          verifyNoInteractions(queueItemDao);
        })
      );
  }

  @Test
  public void testGetKey(TestContext context) {
    when(uploadDefinitionService.getJobExecutionById(anyString(), any()))
      .thenReturn(
        Future.succeededFuture(new JobExecution().withSourcePath("key"))
      );

    service
      .getKey("id", null)
      .onComplete(
        context.asyncAssertSuccess(result -> {
          assertThat(result, is("key"));

          verify(uploadDefinitionService, times(1))
            .getJobExecutionById("id", null);
          verifyNoMoreInteractions(uploadDefinitionService);
        })
      );
  }
}
