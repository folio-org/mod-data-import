package org.folio.service.file;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.support.TestUtil.TENANT_ID;
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
import io.vertx.junit5.VertxTestContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SplitFileProcessingServiceRegistrationTest extends SplitFileProcessingServiceAbstractTest {

  @DisplayName("should return empty list when no keys to register")
  @Test
  void shouldReturnEmptyList_whenNoKeysToRegister(VertxTestContext testContext) {
    service.registerSplitFileParts(
      null, null,
      new JobProfileInfo().withDataType(JobProfileInfo.DataType.MARC),
      changeManagerClient, 0,
      new ConnectionParams(Map.of(XOkapiHeaders.TENANT, TENANT_ID), null),
      List.of()
    ).onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertThat(result.list()).isEmpty();
      verifyNoInteractions(changeManagerClient);
      verifyNoInteractions(queueItemDao);
      testContext.completeNow();
    })));
  }

  @DisplayName("should register single split file part successfully")
  @Test
  void shouldRegisterSingleSplitFilePart_successfully(VertxTestContext testContext) {
    WIRE_MOCK.stubFor(
      WireMock.post("/change-manager/jobExecutions")
        .willReturn(WireMock.created().withBody(
          JsonObject.mapFrom(new InitJobExecutionsRsDto()
              .withJobExecutions(Collections.singletonList(new JobExecution().withId("test-execution-id"))))
            .encode()))
    );

    service.registerSplitFileParts(
      PARENT_UPLOAD_DEFINITION_WITH_USER, PARENT_JOB_EXECUTION,
      new JobProfileInfo().withDataType(JobProfileInfo.DataType.MARC),
      changeManagerClient, 123,
      new ConnectionParams(Map.of(XOkapiHeaders.TENANT, TENANT_ID), null),
      List.of("key1")
    ).onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertThat(result.succeeded()).isTrue();
      assertThat(result.list()).hasSize(1);

      JobExecution execution = (JobExecution) result.list().getFirst();
      assertThat(execution.getId()).isEqualTo("test-execution-id");

      WIRE_MOCK.verify(WireMock.exactly(1),
        WireMock.anyRequestedFor(WireMock.urlMatching("/change-manager/jobExecutions")));
      verify(changeManagerClient, times(1)).postChangeManagerJobExecutions(any(), any());
      verifyNoInteractions(queueItemDao);

      testContext.completeNow();
    })));
  }

  @DisplayName("should register multiple split file parts successfully")
  @Test
  void shouldRegisterMultipleSplitFileParts_successfully(VertxTestContext testContext) {
    WIRE_MOCK.stubFor(
      WireMock.post("/change-manager/jobExecutions")
        .willReturn(WireMock.created().withBody(
          JsonObject.mapFrom(new InitJobExecutionsRsDto()
              .withJobExecutions(Collections.singletonList(new JobExecution().withId("test-execution-id"))))
            .encode()))
    );

    service.registerSplitFileParts(
      PARENT_UPLOAD_DEFINITION, PARENT_JOB_EXECUTION,
      new JobProfileInfo().withDataType(JobProfileInfo.DataType.MARC),
      changeManagerClient, 123,
      new ConnectionParams(Map.of(XOkapiHeaders.TENANT, TENANT_ID), null),
      Arrays.asList("key1", "key2", "key3")
    ).onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertThat(result.succeeded()).isTrue();
      assertThat(result.list()).hasSize(3);
      assertThat(result.list().stream().map(JobExecution.class::cast).map(JobExecution::getId).toList())
        .containsExactlyInAnyOrder("test-execution-id", "test-execution-id", "test-execution-id");

      WIRE_MOCK.verify(WireMock.exactly(3),
        WireMock.anyRequestedFor(WireMock.urlMatching("/change-manager/jobExecutions")));
      verify(changeManagerClient, times(3)).postChangeManagerJobExecutions(any(), any());
      verifyNoInteractions(queueItemDao);

      testContext.completeNow();
    })));
  }

  @DisplayName("should fail registration when server returns error response")
  @Test
  void shouldFailRegistration_whenServerReturnsError(VertxTestContext testContext) {
    WIRE_MOCK.stubFor(
      WireMock.post("/change-manager/jobExecutions").willReturn(WireMock.serverError())
    );

    service.registerSplitFileParts(
      PARENT_UPLOAD_DEFINITION, PARENT_JOB_EXECUTION,
      new JobProfileInfo().withDataType(JobProfileInfo.DataType.MARC),
      changeManagerClient, 123,
      new ConnectionParams(Map.of(XOkapiHeaders.TENANT, TENANT_ID), null),
      List.of("key1")
    ).onComplete(testContext.failing(result -> testContext.verify(() -> {
      WIRE_MOCK.verify(WireMock.exactly(1),
        WireMock.anyRequestedFor(WireMock.urlMatching("/change-manager/jobExecutions")));
      verify(changeManagerClient, times(1)).postChangeManagerJobExecutions(any(), any());
      verifyNoInteractions(queueItemDao);
      testContext.completeNow();
    })));
  }

  @DisplayName("should return source path as key from job execution")
  @Test
  void shouldReturnSourcePathAsKey_fromJobExecution(VertxTestContext testContext) {
    when(uploadDefinitionService.getJobExecutionById(anyString(), any()))
      .thenReturn(Future.succeededFuture(new JobExecution().withSourcePath("key")));

    service.getKey("id", null).onComplete(
      testContext.succeeding(result -> testContext.verify(() -> {
        assertThat(result).isEqualTo("key");
        verify(uploadDefinitionService, times(1)).getJobExecutionById("id", null);
        verifyNoMoreInteractions(uploadDefinitionService);
        testContext.completeNow();
      }))
    );
  }
}
