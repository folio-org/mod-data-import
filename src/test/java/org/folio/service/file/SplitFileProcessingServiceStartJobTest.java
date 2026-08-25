package org.folio.service.file;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.junit5.VertxTestContext;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.ProcessFilesRqDto;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.service.file.SplitFileProcessingService.SplitFileInformation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

class SplitFileProcessingServiceStartJobTest extends SplitFileProcessingServiceAbstractTest {

  private static final Resource TEST_FILE = new ClassPathResource("10.mrc");
  private static final Resource TEST_EDIFACT_FILE = new ClassPathResource("edifact/CornAuxAm.1605541205.edi");

  @DisplayName("should create parent job executions for each file definition")
  @Test
  void shouldCreateParentJobExecutions_forEachFileDefinition(VertxTestContext testContext) {
    Map<String, JobExecution> executionByFileName = Map.of(
      FILE_DEFINITION_1.getSourcePath(), JOB_EXECUTION_1,
      FILE_DEFINITION_2.getSourcePath(), JOB_EXECUTION_2,
      FILE_DEFINITION_3.getSourcePath(), JOB_EXECUTION_3
    );
    doAnswer(invocation -> {
      InitJobExecutionsRqDto request = invocation.getArgument(0);
      assertThat(request.getFiles()).hasSize(1);
      assertThat(request.getJobProfileInfo()).isEqualTo(JOB_PROFILE_INFO);
      assertThat(request.getUserId()).isEqualTo("created-user-id");
      String fileName = request.getFiles().getFirst().getName();
      JobExecution jobExecution = executionByFileName.get(fileName);
      assertThat(jobExecution).as("Unexpected file name: %s", fileName).isNotNull();
      Handler<AsyncResult<HttpResponse<Buffer>>> responseHandler = invocation.getArgument(1);
      responseHandler.handle(getSuccessArBuffer(new InitJobExecutionsRsDto().withJobExecutions(List.of(jobExecution))));
      return null;
    }).when(changeManagerClient).postChangeManagerJobExecutions(any(), any());

    service.createParentJobExecutions(
      new ProcessFilesRqDto()
        .withJobProfileInfo(JOB_PROFILE_INFO)
        .withUploadDefinition(new UploadDefinition()
          .withFileDefinitions(Arrays.asList(FILE_DEFINITION_1, FILE_DEFINITION_2, FILE_DEFINITION_3))
          .withMetadata(METADATA)),
      changeManagerClient
    ).onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertThat(result)
        .hasSize(3)
        .containsEntry("key/file-1-key", JOB_EXECUTION_1)
        .containsEntry("key/file-2-key", JOB_EXECUTION_2)
        .containsEntry("key/file-3-key", JOB_EXECUTION_3);
      verify(changeManagerClient, times(3)).postChangeManagerJobExecutions(any(), any());
      testContext.completeNow();
    })));
  }

  @DisplayName("should fail when creating parent job executions fails")
  @Test
  void shouldFail_whenCreatingParentJobExecutionsFails(VertxTestContext testContext) {
    doAnswer(invocation -> {
      invocation.<Handler<AsyncResult<HttpResponse<Buffer>>>>getArgument(1).handle(Future.failedFuture("test error"));
      return null;
    }).when(changeManagerClient).postChangeManagerJobExecutions(any(), any());

    service.createParentJobExecutions(
      new ProcessFilesRqDto()
        .withJobProfileInfo(JOB_PROFILE_INFO)
        .withUploadDefinition(new UploadDefinition()
          .withFileDefinitions(Arrays.asList(FILE_DEFINITION_1, FILE_DEFINITION_2, FILE_DEFINITION_3))
          .withMetadata(METADATA)),
      changeManagerClient
    ).onComplete(testContext.failingThenComplete());
  }

  @DisplayName("should split MARC file and return split keys and total records")
  @Test
  void shouldSplitMarcFile_andReturnSplitKeysAndTotalRecords(VertxTestContext testContext) throws IOException {
    when(minioStorageService.readFile("test-key"))
      .thenReturn(Future.succeededFuture(TEST_FILE.getInputStream()));
    when(fileSplitService.splitFileFromS3(any(), any()))
      .thenReturn(Future.succeededFuture(Arrays.asList("result1", "result2", "result3")));

    service.splitFile("test-key", JOB_PROFILE_MARC).onComplete(
      testContext.succeeding(result -> testContext.verify(() -> {
        assertThat(result.getKey()).isEqualTo("test-key");
        assertThat(result.getSplitKeys()).containsExactly("result1", "result2", "result3");
        assertThat(result.getTotalRecords()).isEqualTo(10);
        testContext.completeNow();
      }))
    );
  }

  @DisplayName("should not split non-MARC file and return original key")
  @Test
  void shouldNotSplitNonMarcFile_andReturnOriginalKey(VertxTestContext testContext) throws IOException {
    when(minioStorageService.readFile("test-key"))
      .thenReturn(Future.succeededFuture(TEST_EDIFACT_FILE.getInputStream()));

    service.splitFile("test-key", JOB_PROFILE_EDIFACT).onComplete(
      testContext.succeeding(result -> testContext.verify(() -> {
        assertThat(result.getKey()).isEqualTo("test-key");
        assertThat(result.getSplitKeys()).containsExactly("test-key");
        assertThat(result.getTotalRecords()).isEqualTo(1);
        testContext.completeNow();
      }))
    );
  }

  @DisplayName("should fail when file split encounters I/O error")
  @Test
  void shouldFail_whenFileSplitEncountersIoError(VertxTestContext testContext) {
    when(minioStorageService.readFile("test-key"))
      .thenReturn(Future.succeededFuture(new InputStream() {
        @Override
        public int read() throws IOException {
          throw new IOException();
        }
      }));

    service.splitFile("test-key", JOB_PROFILE_MARC).onComplete(testContext.failingThenComplete());
  }

  @DisplayName("should initialize job for all file definitions and return split info map")
  @Test
  void shouldInitializeJob_forAllFileDefinitions(VertxTestContext testContext) {
    stubSplitFileSequence();
    stubJobExecutionCreation();
    stubRecordCountUpdate();

    service.initializeJob(
      new ProcessFilesRqDto()
        .withJobProfileInfo(JOB_PROFILE_INFO)
        .withUploadDefinition(new UploadDefinition()
          .withFileDefinitions(Arrays.asList(FILE_DEFINITION_1, FILE_DEFINITION_2, FILE_DEFINITION_3))),
      changeManagerClient
    ).onComplete(testContext.succeeding(map -> testContext.verify(() -> {
      assertThat(map).hasSize(3);

      assertThat(map.get("key/file-1-key").getKey()).isEqualTo("key/file-1-key");
      assertThat(map.get("key/file-1-key").getJobExecution()).isEqualTo(JOB_EXECUTION_1.withTotalRecordsInFile(10));
      assertThat(map.get("key/file-1-key").getSplitKeys()).containsExactly("a1", "a2", "a3");
      assertThat(map.get("key/file-1-key").getTotalRecords()).isEqualTo(10);

      assertThat(map.get("key/file-2-key").getKey()).isEqualTo("key/file-2-key");
      assertThat(map.get("key/file-2-key").getJobExecution()).isEqualTo(JOB_EXECUTION_2.withTotalRecordsInFile(10));
      assertThat(map.get("key/file-2-key").getSplitKeys()).containsExactly("b1");
      assertThat(map.get("key/file-2-key").getTotalRecords()).isEqualTo(10);

      assertThat(map.get("key/file-3-key").getKey()).isEqualTo("key/file-3-key");
      assertThat(map.get("key/file-3-key").getJobExecution()).isEqualTo(JOB_EXECUTION_3.withTotalRecordsInFile(10));
      assertThat(map.get("key/file-3-key").getSplitKeys()).containsExactly("c1", "c2");
      assertThat(map.get("key/file-3-key").getTotalRecords()).isEqualTo(10);

      verify(service, times(3)).splitFile(any(), any());
      verify(changeManagerClient, times(3)).postChangeManagerJobExecutions(any(), any());
      testContext.completeNow();
    })));
  }

  @DisplayName("should initialize children and update job profile for each file")
  @Test
  void shouldInitializeChildren_andUpdateJobProfile(VertxTestContext testContext) {
    doReturn(Future.all(Future.succeededFuture(JOB_EXECUTION_2), Future.succeededFuture(JOB_EXECUTION_3)))
      .when(service).registerSplitFileParts(any(), eq(JOB_EXECUTION_1), eq(JOB_PROFILE_INFO),
        eq(changeManagerClient), eq(10), any(), anyList());

    when(fileProcessor.updateJobsProfile(any(), eq(JOB_PROFILE_INFO), any()))
      .thenAnswer(invocation -> {
        assertThat(invocation.<List<JobExecutionDto>>getArgument(0).stream()
          .map(JobExecutionDto::getId).toList())
          .containsExactlyInAnyOrder(JOB_EXECUTION_2.getId(), JOB_EXECUTION_3.getId());
        return Future.succeededFuture();
      })
      .thenAnswer(invocation -> {
        assertThat(invocation.<List<JobExecutionDto>>getArgument(0).stream()
          .map(JobExecutionDto::getId).toList())
          .containsExactlyInAnyOrder(JOB_EXECUTION_1.getId());
        return Future.succeededFuture();
      });

    when(uploadDefinitionService.updateJobExecutionStatus(
      eq(JOB_EXECUTION_1.getId()),
      eq(new StatusDto().withStatus(StatusDto.Status.COMMIT_IN_PROGRESS)), any()))
      .thenReturn(Future.succeededFuture(true));

    when(this.queueItemDao.addQueueItem(any())).thenReturn(Future.succeededFuture("new-id"));

    service.initializeChildren(
      new ProcessFilesRqDto()
        .withJobProfileInfo(JOB_PROFILE_INFO)
        .withUploadDefinition(new UploadDefinition()
          .withFileDefinitions(Arrays.asList(FILE_DEFINITION_1, FILE_DEFINITION_2, FILE_DEFINITION_3))
          .withMetadata(METADATA)),
      changeManagerClient,
      new ConnectionParams(Map.of(XOkapiHeaders.TENANT, "tenant"), null),
      SplitFileInformation.builder()
        .key("key/file-1-key").jobExecution(JOB_EXECUTION_1).totalRecords(10)
        .splitKeys(Arrays.asList("a1", "a2")).build()
    ).onComplete(testContext.succeeding(v -> testContext.verify(() -> {
      verify(service, times(1)).registerSplitFileParts(
        any(), eq(JOB_EXECUTION_1), eq(JOB_PROFILE_INFO), eq(changeManagerClient), eq(10), any(), anyList());
      verify(fileProcessor, times(2)).updateJobsProfile(any(), eq(JOB_PROFILE_INFO), any());
      verify(uploadDefinitionService, times(1)).updateJobExecutionStatus(
        eq(JOB_EXECUTION_1.getId()),
        eq(new StatusDto().withStatus(StatusDto.Status.COMMIT_IN_PROGRESS)), any());
      verifyNoMoreInteractions(fileProcessor);
      verifyNoMoreInteractions(uploadDefinitionService);
      verify(queueItemDao, times(2)).addQueueItem(any());
      verifyNoMoreInteractions(queueItemDao);
      testContext.completeNow();
    })));
  }

  @DisplayName("should fail to initialize children when update job execution status returns false")
  @Test
  void shouldFailInitializeChildren_whenUpdateJobExecutionStatusReturnsFalse(VertxTestContext testContext) {
    doReturn(Future.all(new ArrayList<>())).when(service)
      .registerSplitFileParts(any(), any(), any(), any(), anyInt(), any(), anyList());

    when(fileProcessor.updateJobsProfile(any(), eq(JOB_PROFILE_INFO), any()))
      .thenReturn(Future.succeededFuture());

    when(uploadDefinitionService.updateJobExecutionStatus(any(), any(), any()))
      .thenReturn(Future.succeededFuture(false));

    service.initializeChildren(
      new ProcessFilesRqDto()
        .withJobProfileInfo(JOB_PROFILE_INFO)
        .withUploadDefinition(new UploadDefinition()
          .withFileDefinitions(Arrays.asList(FILE_DEFINITION_1, FILE_DEFINITION_2, FILE_DEFINITION_3))
          .withMetadata(METADATA)),
      changeManagerClient,
      new ConnectionParams(Map.of(XOkapiHeaders.TENANT, "tenant"), null),
      SplitFileInformation.builder()
        .key("key/file-1-key").jobExecution(JOB_EXECUTION_1).totalRecords(10)
        .splitKeys(Arrays.asList("a1", "a2")).build()
    ).onComplete(testContext.failing(v -> testContext.verify(() -> {
      verifyNoInteractions(queueItemDao);
      testContext.completeNow();
    })));
  }

  private void stubSplitFileSequence() {
    doAnswer(invocation ->
      Future.succeededFuture(SplitFileInformation.builder()
        .key(invocation.getArgument(0)).splitKeys(Arrays.asList("a1", "a2", "a3")).totalRecords(10).build()))
      .doAnswer(invocation ->
        Future.succeededFuture(SplitFileInformation.builder()
          .key(invocation.getArgument(0)).splitKeys(List.of("b1")).totalRecords(10).build()))
      .doAnswer(invocation ->
        Future.succeededFuture(SplitFileInformation.builder()
          .key(invocation.getArgument(0)).splitKeys(Arrays.asList("c1", "c2")).totalRecords(10).build()))
      .when(service).splitFile(any(), any());
  }

  private void stubJobExecutionCreation() {
    Map<String, JobExecution> executionByFileName = Map.of(
      FILE_DEFINITION_1.getSourcePath(), JOB_EXECUTION_1,
      FILE_DEFINITION_2.getSourcePath(), JOB_EXECUTION_2,
      FILE_DEFINITION_3.getSourcePath(), JOB_EXECUTION_3
    );
    doAnswer(invocation -> {
      InitJobExecutionsRqDto request = invocation.getArgument(0);
      assertThat(request.getFiles()).hasSize(1);
      assertThat(request.getJobProfileInfo()).isEqualTo(JOB_PROFILE_INFO);
      String fileName = request.getFiles().getFirst().getName();
      JobExecution jobExecution = executionByFileName.get(fileName);
      assertThat(jobExecution).as("Unexpected file name: %s", fileName).isNotNull();
      Handler<AsyncResult<HttpResponse<Buffer>>> responseHandler = invocation.getArgument(1);
      responseHandler.handle(getSuccessArBuffer(new InitJobExecutionsRsDto().withJobExecutions(List.of(jobExecution))));
      return null;
    }).when(changeManagerClient).postChangeManagerJobExecutions(any(), any());
  }

  private void stubRecordCountUpdate() {
    when(changeManagerClient.putChangeManagerJobExecutionsById(any(), any()))
      .thenAnswer(invocation -> {
        JobExecution jobExecution = invocation.getArgument(1);
        assertThat(jobExecution).isNotNull();
        assertThat(jobExecution.getTotalRecordsInFile()).isEqualTo(10);
        return getSuccessArBuffer(null);
      });
  }

  @DisplayName("should start job and update upload definition status to COMPLETED")
  @Test
  void shouldStartJob_andUpdateUploadDefinitionStatusToCompleted() {
    doReturn(Future.succeededFuture(Map.of(
      "key/file-1-key", SplitFileInformation.builder()
        .key("key/file-1-key").splitKeys(Arrays.asList("a1", "a2", "a3")).totalRecords(10)
        .jobExecution(JOB_EXECUTION_1).build(),
      "key/file-2-key", SplitFileInformation.builder()
        .key("key/file-2-key").splitKeys(List.of("b1")).totalRecords(10)
        .jobExecution(JOB_EXECUTION_2).build()
    ))).when(service).initializeJob(any(), eq(changeManagerClient));

    doReturn(Future.succeededFuture()).when(service).initializeChildren(any(), eq(changeManagerClient), any(), any());

    when(uploadDefinitionService.updateBlocking(any(), any(), any()))
      .thenAnswer(v -> {
        assertThat(v.<Function<UploadDefinition, UploadDefinition>>getArgument(1)
          .apply(new UploadDefinition()).getStatus())
          .isEqualTo(UploadDefinition.Status.COMPLETED);
        return Future.succeededFuture();
      });

    service.startJob(
      new ProcessFilesRqDto()
        .withJobProfileInfo(JOB_PROFILE_INFO)
        .withUploadDefinition(new UploadDefinition()
          .withFileDefinitions(Arrays.asList(FILE_DEFINITION_1, FILE_DEFINITION_2, FILE_DEFINITION_3))),
      changeManagerClient,
      new ConnectionParams(Map.of(XOkapiHeaders.TENANT, "tenant"), null)
    );
  }
}
