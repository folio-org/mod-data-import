package org.folio.service.file;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.file.FileSystem;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.commons.io.FileUtils;
import org.folio.dao.DataImportQueueItemDao;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.service.file.S3JobRunningVerticle.QueueJob;
import org.folio.service.processing.ParallelFileChunkingProcessor;
import org.folio.service.processing.ranking.ScoreService;
import org.folio.service.s3storage.MinioStorageService;
import org.folio.service.upload.UploadDefinitionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class S3JobRunningVerticleTest {

  private static final int POLL_INTERVAL = 100;
  private static final Vertx VERTX = Vertx.vertx();

  private @Mock Vertx mockVertx;
  private @Mock FileSystem fileSystem;
  private @Mock DataImportQueueItemDao queueItemDao;
  private @Mock MinioStorageService minioStorageService;
  private @Mock ScoreService scoreService;
  private @Mock UploadDefinitionService uploadDefinitionService;
  private @Mock ParallelFileChunkingProcessor fileProcessor;

  @TempDir
  private Path tempDir;

  private S3JobRunningVerticle verticle;

  @BeforeEach
  void setUp() {
    S3JobRunningVerticle.WORKERS_IN_USE.set(0);

    lenient().when(mockVertx.fileSystem()).thenReturn(fileSystem);
    lenient().doAnswer(invocation -> {
      new File(invocation.getArgument(0, String.class)).delete();
      return Future.succeededFuture();
    }).when(fileSystem).delete(anyString());
    lenient().doAnswer(invocation -> {
      invocation.<Handler<Void>>getArgument(0).handle(null);
      return null;
    }).when(mockVertx).runOnContext(any());

    this.verticle = spy(
      new S3JobRunningVerticle(
        mockVertx, queueItemDao, minioStorageService, scoreService,
        uploadDefinitionService, fileProcessor, POLL_INTERVAL, 10
      )
    );
  }

  @DisplayName("should build connection params from queue item fields")
  @Test
  void shouldBuildConnectionParams_fromQueueItemFields() {
    ConnectionParams params = verticle.getConnectionParams(
      new DataImportQueueItem()
        .withTenant("tenant")
        .withOkapiUrl("okapi-url")
        .withOkapiToken("token")
        .withOkapiPermissions("permissions")
        .withOkapiRequestId("request-id")
    );

    assertThat(params.getTenantId()).isEqualTo("tenant");
    assertThat(params.getConnectionUrl()).isEqualTo("okapi-url");
    assertThat(params.getToken()).isEqualTo("token");
    assertThat(params.getHeaders()).containsEntry(XOkapiHeaders.PERMISSIONS, "permissions");
    assertThat(params.getHeaders()).containsEntry(XOkapiHeaders.REQUEST_ID, "request-id");
  }

  @DisplayName("should include user ID header when building connection params with user ID")
  @Test
  void shouldIncludeUserIdHeader_whenBuildingConnectionParamsWithUserId() {
    ConnectionParams params = verticle.getConnectionParams(
      new DataImportQueueItem()
        .withTenant("tenant")
        .withOkapiUrl("okapi-url")
        .withOkapiToken("token")
        .withOkapiPermissions("permissions")
        .withOkapiRequestId("request-id"),
      "user-id"
    );

    assertThat(params.getTenantId()).isEqualTo("tenant");
    assertThat(params.getConnectionUrl()).isEqualTo("okapi-url");
    assertThat(params.getToken()).isEqualTo("token");
    assertThat(params.getHeaders()).containsEntry(XOkapiHeaders.PERMISSIONS, "permissions");
    assertThat(params.getHeaders()).containsEntry(XOkapiHeaders.REQUEST_ID, "request-id");
    assertThat(params.getHeaders()).containsEntry(XOkapiHeaders.USER_ID, "user-id");
  }

  @DisplayName("should download 5 bytes from S3 and verify S3 client interactions")
  @Test
  void shouldDownload5Bytes_andVerifyS3Interactions(VertxTestContext testContext) throws IOException {
    InputStream inputStream = spy(new ByteArrayInputStream(new byte[5]));
    when(minioStorageService.readFile("test-key")).thenReturn(Future.succeededFuture(inputStream));

    File destFile = Files.createTempFile(tempDir, "", "").toFile();

    verticle
      .downloadFromS3(
        new QueueJob()
          .withFile(destFile)
          .withJobExecution(new JobExecution().withSourcePath("test-key"))
      )
      .onComplete(
        testContext.succeeding(result -> {
          try (InputStream reader = new FileInputStream(destFile)) {
            assertThat(reader.readAllBytes()).hasSize(5);

            verify(inputStream, times(1)).close();
            verify(inputStream, times(1)).transferTo(any());
            verify(minioStorageService, times(1)).readFile("test-key");
            verifyNoMoreInteractions(minioStorageService);
            verifyNoMoreInteractions(inputStream);

            testContext.completeNow();
          } catch (IOException e) {
            testContext.failNow(e);
          }
        })
      );
  }

  @DisplayName("should fail download when S3 stream transferTo throws IOException")
  @Test
  void shouldFailDownload_whenS3StreamTransferToThrowsIoException(VertxTestContext testContext)
    throws IOException {
    InputStream inputStream = mock(ByteArrayInputStream.class);
    when(inputStream.transferTo(any())).thenThrow(new IOException("test error"));
    when(minioStorageService.readFile("test-key")).thenReturn(Future.succeededFuture(inputStream));

    File destFile = Files.createTempFile(tempDir, "", "").toFile();

    verticle
      .downloadFromS3(
        new QueueJob()
          .withFile(destFile)
          .withJobExecution(new JobExecution().withSourcePath("test-key"))
      )
      .onComplete(
        testContext.failing(v ->
          testContext.verify(() -> {
            verify(minioStorageService, times(1)).readFile("test-key");
            verifyNoMoreInteractions(minioStorageService);

            testContext.completeNow();
          })
        )
      );
  }

  @DisplayName("should create local temp file for queue item file path")
  @Test
  void shouldCreateLocalTempFile_forQueueItemFilePath(VertxTestContext testContext) {
    File testResult = new File("result");

    when(fileSystem.createTempFile(anyString(), anyString(), anyString()))
      .thenReturn(Future.succeededFuture(testResult.toString()));

    verticle
      .createLocalFile(new DataImportQueueItem().withFilePath("path/test-file"))
      .onComplete(
        testContext.succeeding(r ->
          testContext.verify(() -> {
            assertThat(r).hasToString(testResult.toString());
            testContext.completeNow();
          })
        )
      );
  }

  @DisplayName("should update job execution status successfully and verify service interaction")
  @Test
  void shouldUpdateJobExecutionStatus_successfully(VertxTestContext testContext) {
    when(uploadDefinitionService.updateJobExecutionStatus(eq("exec-id"), any(), any()))
      .thenReturn(Future.succeededFuture(true));

    verticle
      .updateJobExecutionStatusSafely("exec-id", new StatusDto(), null)
      .onComplete(
        testContext.succeeding(v ->
          testContext.verify(() -> {
            verify(uploadDefinitionService, times(1)).updateJobExecutionStatus(eq("exec-id"), any(), any());
            verifyNoMoreInteractions(uploadDefinitionService);
            testContext.completeNow();
          })
        )
      );
  }

  @DisplayName("should fail when updateJobExecutionStatus returns false")
  @Test
  void shouldFail_whenUpdateJobExecutionStatusReturnsFalse(VertxTestContext testContext) {
    when(uploadDefinitionService.updateJobExecutionStatus(eq("exec-id"), any(), any()))
      .thenReturn(Future.succeededFuture(false));

    verticle
      .updateJobExecutionStatusSafely("exec-id", new StatusDto(), null)
      .onComplete(
        testContext.failing(v ->
          testContext.verify(() -> {
            verify(uploadDefinitionService, times(1)).updateJobExecutionStatus(eq("exec-id"), any(), any());
            verifyNoMoreInteractions(uploadDefinitionService);
            testContext.completeNow();
          })
        )
      );
  }

  @DisplayName("should not request queue items when worker pool is at capacity")
  @Test
  void shouldNotRequestQueueItems_whenWorkerPoolIsAtCapacity() {
    S3JobRunningVerticle.WORKERS_IN_USE.set(10);

    verticle.pollForJobs();

    verifyNoInteractions(scoreService);
  }

  @DisplayName("should request one item and not request more when worker pool is almost at capacity")
  @Test
  void shouldRequestOneItem_andNotRequestMore_whenWorkerPoolIsAlmostAtCapacity(VertxTestContext testContext) {
    S3JobRunningVerticle.WORKERS_IN_USE.set(9);

    when(scoreService.getBestQueueItemAndMarkInProgress())
      .thenReturn(Future.succeededFuture(Optional.of(new DataImportQueueItem())));

    // never-completing promise keeps workersInUse at 10 so the recursive guard never fires
    doAnswer(invocation -> Promise.promise())
      .when(verticle)
      .processQueueItem(any());

    verticle.pollForJobs();

    VERTX.setTimer(
      50L,
      v ->
        testContext.verify(() -> {
          verify(scoreService, times(1)).getBestQueueItemAndMarkInProgress();
          verifyNoMoreInteractions(scoreService);
          verify(verticle, times(1)).pollForJobs();
          assertThat(S3JobRunningVerticle.WORKERS_IN_USE.get()).isEqualTo(10);
          testContext.completeNow();
        })
    );
  }

  @DisplayName("should not process any item when no queue items are available")
  @Test
  void shouldNotProcessAnyItem_whenNoQueueItemsAvailable(VertxTestContext testContext) {
    S3JobRunningVerticle.WORKERS_IN_USE.set(0);

    when(scoreService.getBestQueueItemAndMarkInProgress())
      .thenReturn(Future.succeededFuture(Optional.empty()));

    verticle.pollForJobs();

    VERTX.setTimer(
      50L,
      v ->
        testContext.verify(() -> {
          verify(scoreService, times(1)).getBestQueueItemAndMarkInProgress();
          verifyNoMoreInteractions(scoreService);
          verify(verticle, times(1)).pollForJobs();
          verify(verticle, never()).processQueueItem(any());
          assertThat(S3JobRunningVerticle.WORKERS_IN_USE.get()).isZero();
          testContext.completeNow();
        })
    );
  }

  @DisplayName("should process multiple queue items and poll three times when two items are available")
  @Test
  void shouldProcessMultipleQueueItems_andPollThreeTimes_whenTwoItemsAvailable(VertxTestContext testContext) {
    S3JobRunningVerticle.WORKERS_IN_USE.set(0);

    when(scoreService.getBestQueueItemAndMarkInProgress())
      .thenReturn(Future.succeededFuture(Optional.of(new DataImportQueueItem())))
      .thenReturn(Future.succeededFuture(Optional.of(new DataImportQueueItem())))
      .thenReturn(Future.succeededFuture(Optional.empty()));

    doReturn(Future.succeededFuture()).when(verticle).processQueueItem(any());

    verticle.pollForJobs();

    VERTX.setTimer(
      50L,
      v ->
        testContext.verify(() -> {
          verify(scoreService, times(3)).getBestQueueItemAndMarkInProgress();
          verifyNoMoreInteractions(scoreService);
          verify(verticle, times(3)).pollForJobs();
          assertThat(S3JobRunningVerticle.WORKERS_IN_USE.get()).isZero();
          testContext.completeNow();
        })
    );
  }

  @DisplayName("should process queue item successfully and delete temp file asynchronously")
  @Test
  void shouldProcessQueueItem_successfully_andDeleteTempFileAsync(VertxTestContext testContext)
    throws IOException {
    File tempFile = Files.createTempFile(tempDir, "", "").toFile();

    DataImportQueueItem queueItem = new DataImportQueueItem()
      .withId("queue-id")
      .withJobExecutionId("job-exec-id")
      .withDataType("MARC")
      .withTenant("tenant")
      .withOkapiUrl("okapi-url")
      .withOkapiToken("token")
      .withOkapiPermissions("permissions")
      .withOkapiRequestId("request-id");

    doReturn(Future.succeededFuture(tempFile)).when(verticle).createLocalFile(queueItem);
    when(uploadDefinitionService.getJobExecutionById(eq("job-exec-id"), any()))
      .thenReturn(Future.succeededFuture(new JobExecution().withId("job-exec-id").withUserId("user-id")));
    doReturn(Future.succeededFuture()).when(verticle).updateJobExecutionStatusSafely(any(), any(), any());
    doAnswer(invocation -> Future.succeededFuture(invocation.getArgument(0))).when(verticle).downloadFromS3(any());
    when(fileProcessor.processFile(eq(tempFile), eq("job-exec-id"), any(), any()))
      .thenReturn(Future.succeededFuture());

    verticle
      .processQueueItem(queueItem)
      .onComplete(
        testContext.succeeding(v -> {
          verify(verticle, times(1)).createLocalFile(queueItem);
          verify(uploadDefinitionService, times(1)).getJobExecutionById(eq("job-exec-id"), any());
          verify(verticle, times(1)).updateJobExecutionStatusSafely(eq("job-exec-id"), any(), any());
          verify(verticle, times(1)).downloadFromS3(any());
          verify(fileProcessor, times(1)).processFile(eq(tempFile), eq("job-exec-id"), any(), any());
          verify(queueItemDao, times(1)).deleteQueueItemById("queue-id");
          verifyNoMoreInteractions(queueItemDao);
          verifyNoMoreInteractions(uploadDefinitionService);
          verifyNoMoreInteractions(fileProcessor);

          VERTX.setTimer(
            50L,
            vv ->
              testContext.verify(() -> {
                assertThat(tempFile).doesNotExist();
                testContext.completeNow();
              })
          );
        })
      );
  }

  @DisplayName("should fail and mark failure status then delete temp file when processFile throws")
  @Test
  void shouldFail_andMarkFailureStatus_thenDeleteTempFile_whenProcessFileThrows(VertxTestContext testContext)
    throws IOException {
    File tempFile = Files.createTempFile(tempDir, "", "").toFile();

    DataImportQueueItem queueItem = new DataImportQueueItem()
      .withId("queue-id")
      .withJobExecutionId("job-exec-id")
      .withDataType("MARC")
      .withTenant("tenant")
      .withOkapiUrl("okapi-url")
      .withOkapiToken("token")
      .withOkapiPermissions("permissions")
      .withOkapiRequestId("request-id");

    doReturn(Future.succeededFuture(tempFile)).when(verticle).createLocalFile(queueItem);
    when(uploadDefinitionService.getJobExecutionById(eq("job-exec-id"), any()))
      .thenReturn(Future.succeededFuture(new JobExecution().withId("job-exec-id").withUserId("user-id")));
    doReturn(Future.succeededFuture()).when(verticle).updateJobExecutionStatusSafely(any(), any(), any());
    doAnswer(invocation -> Future.succeededFuture(invocation.getArgument(0))).when(verticle).downloadFromS3(any());
    when(fileProcessor.processFile(eq(tempFile), eq("job-exec-id"), any(), any()))
      .thenThrow(new RuntimeException("test error"));

    verticle
      .processQueueItem(queueItem)
      .onComplete(
        testContext.failing(v -> {
          verify(verticle, times(1)).createLocalFile(queueItem);
          verify(uploadDefinitionService, times(1)).getJobExecutionById(eq("job-exec-id"), any());
          verify(verticle, times(2)).updateJobExecutionStatusSafely(eq("job-exec-id"), any(), any());
          verify(verticle, times(1)).downloadFromS3(any());
          verify(fileProcessor, times(1)).processFile(eq(tempFile), eq("job-exec-id"), any(), any());
          verify(queueItemDao, times(1)).deleteQueueItemById("queue-id");
          verifyNoMoreInteractions(queueItemDao);
          verifyNoMoreInteractions(uploadDefinitionService);
          verifyNoMoreInteractions(fileProcessor);

          VERTX.setTimer(
            50L,
            vv ->
              testContext.verify(() -> {
                assertThat(tempFile).doesNotExist();
                testContext.completeNow();
              })
          );
        })
      );
  }

  @DisplayName("should fail early and clean up queue item when createLocalFile throws UncheckedIOException")
  @Test
  void shouldFailEarly_andCleanUpQueueItem_whenCreateLocalFileThrows(VertxTestContext testContext) {
    try (
      MockedStatic<FileUtils> mock = Mockito.mockStatic(FileUtils.class, Mockito.CALLS_REAL_METHODS)
    ) {
      DataImportQueueItem queueItem = new DataImportQueueItem()
        .withId("queue-id")
        .withJobExecutionId("job-exec-id")
        .withDataType("MARC")
        .withTenant("tenant")
        .withOkapiUrl("okapi-url")
        .withOkapiToken("token")
        .withOkapiPermissions("permissions")
        .withOkapiRequestId("request-id");

      doThrow(new UncheckedIOException(new IOException())).when(verticle).createLocalFile(queueItem);
      doReturn(Future.succeededFuture()).when(verticle).updateJobExecutionStatusSafely(any(), any(), any());

      verticle
        .processQueueItem(queueItem)
        .onComplete(
          testContext.failing(v ->
            testContext.verify(() -> {
              verify(verticle, times(1)).createLocalFile(queueItem);
              verify(verticle, times(1)).updateJobExecutionStatusSafely(eq("job-exec-id"), any(), any());
              verify(queueItemDao, times(1)).deleteQueueItemById("queue-id");
              verifyNoMoreInteractions(queueItemDao);
              verifyNoMoreInteractions(uploadDefinitionService);
              verifyNoMoreInteractions(fileProcessor);
              mock.verifyNoInteractions();

              testContext.completeNow();
            })
          )
        );
    }
  }
}
