package org.folio.service.file;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.commons.io.FileUtils;
import org.folio.dao.DataImportQueueItemDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.service.auth.SystemUserAuthService;
import org.folio.service.file.S3JobRunningVerticle.QueueJob;
import org.folio.service.processing.ParallelFileChunkingProcessor;
import org.folio.service.processing.ranking.ScoreService;
import org.folio.service.s3storage.MinioStorageService;
import org.folio.service.upload.UploadDefinitionService;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class S3JobRunningVerticleUnitTest {

  private static final int POLL_INTERVAL = 100;

  private static final Vertx vertx = Vertx.vertx();

  @Mock
  Vertx mockVertx;

  @Mock
  DataImportQueueItemDao queueItemDao;

  @Mock
  MinioStorageService minioStorageService;

  @Mock
  ScoreService scoreService;

  @Mock
  SystemUserAuthService systemUserService;

  @Mock
  UploadDefinitionService uploadDefinitionService;

  @Mock
  ParallelFileChunkingProcessor fileProcessor;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  S3JobRunningVerticle verticle;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);

    this.verticle =
      spy(
        new S3JobRunningVerticle(
          mockVertx,
          queueItemDao,
          minioStorageService,
          scoreService,
          systemUserService,
          uploadDefinitionService,
          fileProcessor,
          POLL_INTERVAL,
          10
        )
      );
  }

  @Test
  public void testConnectionParams() {
    when(systemUserService.getAuthToken(any())).thenReturn("token");

    OkapiConnectionParams params = verticle.getConnectionParams(
      new DataImportQueueItem().withTenant("tenant").withOkapiUrl("okapi-url")
    );

    assertThat(params.getTenantId(), is("tenant"));
    assertThat(params.getOkapiUrl(), is("okapi-url"));
    assertThat(params.getToken(), is("token"));

    verify(systemUserService, times(1)).getAuthToken(any());

    verifyNoMoreInteractions(systemUserService);
  }

  @Test
  public void testDownloadFromS3Success(TestContext context)
    throws IOException {
    InputStream inputStream = spy(new ByteArrayInputStream(new byte[5]));
    when(minioStorageService.readFile("test-key"))
      .thenReturn(Future.succeededFuture(inputStream));

    File destFile = temporaryFolder.newFile();

    verticle
      .downloadFromS3(
        new QueueJob()
          .withFile(destFile)
          .withJobExecution(new JobExecution().withSourcePath("test-key"))
      )
      .onComplete(
        context.asyncAssertSuccess(result -> {
          try (InputStream reader = new FileInputStream(destFile)) {
            assertThat(reader.readAllBytes().length, is(5));

            // auto-closed by try-with-resources
            verify(inputStream, times(1)).close();
            verify(inputStream, times(1)).transferTo(any());
            verify(minioStorageService, times(1)).readFile("test-key");

            verifyNoMoreInteractions(minioStorageService);
            verifyNoMoreInteractions(inputStream);
          } catch (IOException e) {
            context.fail(e);
          }
        })
      );
  }

  @Test
  public void testDownloadFromS3Failure(TestContext context)
    throws IOException {
    InputStream inputStream = mock(ByteArrayInputStream.class);
    when(inputStream.transferTo(any()))
      .thenThrow(new IOException("test error"));

    when(minioStorageService.readFile("test-key"))
      .thenReturn(Future.succeededFuture(inputStream));

    File destFile = temporaryFolder.newFile();

    verticle
      .downloadFromS3(
        new QueueJob()
          .withFile(destFile)
          .withJobExecution(new JobExecution().withSourcePath("test-key"))
      )
      .onComplete(
        context.asyncAssertFailure(v -> {
          verify(minioStorageService, times(1)).readFile("test-key");

          verifyNoMoreInteractions(minioStorageService);
        })
      );
  }

  @Test
  public void testCreateLocalFileSuccess() {
    try (
      MockedStatic<Files> mock = Mockito.mockStatic(
        Files.class,
        Mockito.CALLS_REAL_METHODS
      )
    ) {
      File testResult = new File("result");
      mock
        .when(() -> Files.createTempFile(eq("di-tmp-"), eq("test-file"), any()))
        .thenReturn(testResult.toPath());

      File result = verticle.createLocalFile(
        new DataImportQueueItem().withFilePath("path/test-file")
      );
      assertThat(result, is(testResult));

      mock.verify(
        () -> Files.createTempFile(eq("di-tmp-"), eq("test-file"), any()),
        times(1)
      );
      mock.verifyNoMoreInteractions();
    }
  }

  @Test
  public void testCreateLocalFileFailure() {
    try (
      MockedStatic<Files> mock = Mockito.mockStatic(
        Files.class,
        Mockito.CALLS_REAL_METHODS
      )
    ) {
      mock
        .when(() -> Files.createTempFile(eq("di-tmp-"), eq("test-file"), any()))
        .thenThrow(new IOException("test exception"));

      DataImportQueueItem testItem = new DataImportQueueItem()
        .withFilePath("path/test-file");

      assertThrows(
        "Should fail with exception when underlying create file call fails",
        UncheckedIOException.class,
        () -> verticle.createLocalFile(testItem)
      );

      mock.verify(
        () -> Files.createTempFile(eq("di-tmp-"), eq("test-file"), any()),
        times(1)
      );
      mock.verifyNoMoreInteractions();
    }
  }

  @Test
  public void testUpdateJobExecutionStatusSuccessful(TestContext context) {
    when(
      uploadDefinitionService.updateJobExecutionStatus(
        eq("exec-id"),
        any(),
        any()
      )
    )
      .thenReturn(Future.succeededFuture(true));

    verticle
      .updateJobExecutionStatusSafely("exec-id", new StatusDto(), null)
      .onComplete(
        context.asyncAssertSuccess(v -> {
          verify(uploadDefinitionService, times(1))
            .updateJobExecutionStatus(eq("exec-id"), any(), any());

          verifyNoMoreInteractions(uploadDefinitionService);
        })
      );
  }

  @Test
  public void testUpdateJobExecutionStatusFailure(TestContext context) {
    when(
      uploadDefinitionService.updateJobExecutionStatus(
        eq("exec-id"),
        any(),
        any()
      )
    )
      .thenReturn(Future.succeededFuture(false));

    verticle
      .updateJobExecutionStatusSafely("exec-id", new StatusDto(), null)
      .onComplete(
        context.asyncAssertFailure(v -> {
          verify(uploadDefinitionService, times(1))
            .updateJobExecutionStatus(eq("exec-id"), any(), any());

          verifyNoMoreInteractions(uploadDefinitionService);
        })
      );
  }

  @Test
  @Ignore
  public void testPollWithAvailableAndSuccessful(TestContext context) {
    DataImportQueueItem queueItem = new DataImportQueueItem();

    when(scoreService.getBestQueueItemAndMarkInProgress())
      .thenReturn(Future.succeededFuture(Optional.of(queueItem)));

    doReturn(Future.succeededFuture())
      .when(verticle)
      .processQueueItem(queueItem);

    when(mockVertx.setTimer(anyLong(), any()))
      .thenAnswer(invocation -> {
        // override default after first call
        doNothing().when(verticle).pollForJobs2();

        // happens immediately, so below we can check that it was called twice
        // (for the initial run below and second go here)
        invocation.<Handler<Long>>getArgument(1).handle(0L);

        return null;
      });

    verticle.pollForJobs2();

    Async async = context.async();

    vertx.setTimer(
      100,
      v ->
        context.verify(vv -> {
          verify(mockVertx, times(1)).setTimer(eq(0L), any());
          verifyNoMoreInteractions(mockVertx);

          verify(scoreService, times(1)).getBestQueueItemAndMarkInProgress();
          verifyNoMoreInteractions(scoreService);

          verify(verticle, times(1)).processQueueItem(queueItem);
          // once for initial run, second from the setTimer mock answer
          verify(verticle, times(2)).pollForJobs2();
          verifyNoMoreInteractions(verticle);

          async.complete();
        })
    );
  }

  @Test
  @Ignore
  public void testPollWithAvailableAndNonSuccessful(TestContext context) {
    DataImportQueueItem queueItem = new DataImportQueueItem();

    when(scoreService.getBestQueueItemAndMarkInProgress())
      .thenReturn(Future.succeededFuture(Optional.of(queueItem)));

    doReturn(Future.failedFuture(new RuntimeException()))
      .when(verticle)
      .processQueueItem(queueItem);

    when(mockVertx.setTimer(anyLong(), any()))
      .thenAnswer(invocation -> {
        // override default after first call
        doNothing().when(verticle).pollForJobs2();

        // happens immediately, so below we can check that it was called twice
        // (for the initial run below and second go here)
        invocation.<Handler<Long>>getArgument(1).handle(0L);

        return null;
      });

    verticle.pollForJobs2();

    Async async = context.async();

    vertx.setTimer(
      100,
      v ->
        context.verify(vv -> {
          // sleep before retry upon failure
          verify(mockVertx, times(1)).setTimer(eq(POLL_INTERVAL), any());
          verifyNoMoreInteractions(mockVertx);

          verify(scoreService, times(1)).getBestQueueItemAndMarkInProgress();
          verifyNoMoreInteractions(scoreService);

          verify(verticle, times(1)).processQueueItem(queueItem);
          // once for initial run, second from the setTimer mock answer
          verify(verticle, times(2)).pollForJobs2();
          verifyNoMoreInteractions(verticle);

          async.complete();
        })
    );
  }

  @Test
  @Ignore
  public void testPollWithNoneAvailable(TestContext context) {
    when(scoreService.getBestQueueItemAndMarkInProgress())
      .thenReturn(Future.succeededFuture(Optional.empty()));

    when(mockVertx.setTimer(anyLong(), any()))
      .thenAnswer(invocation -> {
        // override default after first call
        doNothing().when(verticle).pollForJobs2();

        // happens immediately, so below we can check that it was called twice
        // (for the initial run below and second go here)
        invocation.<Handler<Long>>getArgument(1).handle(0L);

        return null;
      });

    verticle.pollForJobs2();

    Async async = context.async();

    vertx.setTimer(
      100,
      v ->
        context.verify(vv -> {
          // sleep before retry upon failure
          verify(mockVertx, times(1)).setTimer(eq(POLL_INTERVAL), any());
          verifyNoMoreInteractions(mockVertx);

          verify(scoreService, times(1)).getBestQueueItemAndMarkInProgress();
          verifyNoMoreInteractions(scoreService);

          verify(verticle, never()).processQueueItem(any());
          // once for initial run, second from the setTimer mock answer
          verify(verticle, times(2)).pollForJobs2();
          verifyNoMoreInteractions(verticle);

          async.complete();
        })
    );
  }

  @Test
  public void testProcessQueueItemSuccess(TestContext context)
    throws IOException {
    File tempFile = temporaryFolder.newFile();

    DataImportQueueItem queueItem = new DataImportQueueItem()
      .withId("queue-id")
      .withJobExecutionId("job-exec-id")
      .withDataType("MARC");

    doReturn(null).when(verticle).getConnectionParams(any());

    doReturn(tempFile).when(verticle).createLocalFile(queueItem);

    when(uploadDefinitionService.getJobExecutionById("job-exec-id", null))
      .thenReturn(
        Future.succeededFuture(new JobExecution().withId("job-exec-id"))
      );

    doReturn(Future.succeededFuture())
      .when(verticle)
      .updateJobExecutionStatusSafely(any(), any(), any());

    doAnswer(invocation -> Future.succeededFuture(invocation.getArgument(0)))
      .when(verticle)
      .downloadFromS3(any());

    when(
      fileProcessor.processFile(
        eq(tempFile),
        eq("job-exec-id"),
        any(),
        isNull()
      )
    )
      .thenReturn(Future.succeededFuture());

    verticle
      .processQueueItem(queueItem)
      .onComplete(
        context.asyncAssertSuccess(v -> {
          verify(verticle, times(1)).createLocalFile(queueItem);
          verify(uploadDefinitionService, times(1))
            .getJobExecutionById("job-exec-id", null);
          verify(verticle, times(1))
            .updateJobExecutionStatusSafely(eq("job-exec-id"), any(), isNull());
          verify(verticle, times(1)).downloadFromS3(any());
          verify(fileProcessor, times(1))
            .processFile(eq(tempFile), eq("job-exec-id"), any(), isNull());

          verify(queueItemDao, times(1)).deleteDataImportQueueItem("queue-id");

          verifyNoMoreInteractions(queueItemDao);
          verifyNoMoreInteractions(uploadDefinitionService);
          verifyNoMoreInteractions(fileProcessor);

          assertThat(tempFile.exists(), is(false));
        })
      );
  }

  @Test
  public void testProcessQueueItemFailure(TestContext context)
    throws IOException {
    // same as successful, but the call to process fails
    File tempFile = temporaryFolder.newFile();

    DataImportQueueItem queueItem = new DataImportQueueItem()
      .withId("queue-id")
      .withJobExecutionId("job-exec-id")
      .withDataType("MARC");

    doReturn(null).when(verticle).getConnectionParams(any());

    doReturn(tempFile).when(verticle).createLocalFile(queueItem);

    when(uploadDefinitionService.getJobExecutionById("job-exec-id", null))
      .thenReturn(
        Future.succeededFuture(new JobExecution().withId("job-exec-id"))
      );

    doReturn(Future.succeededFuture())
      .when(verticle)
      .updateJobExecutionStatusSafely(any(), any(), any());

    doAnswer(invocation -> Future.succeededFuture(invocation.getArgument(0)))
      .when(verticle)
      .downloadFromS3(any());

    when(
      fileProcessor.processFile(
        eq(tempFile),
        eq("job-exec-id"),
        any(),
        isNull()
      )
    )
      .thenThrow(new RuntimeException("test error"));

    verticle
      .processQueueItem(queueItem)
      .onComplete(
        context.asyncAssertFailure(v -> {
          verify(verticle, times(1)).createLocalFile(queueItem);
          verify(uploadDefinitionService, times(1))
            .getJobExecutionById("job-exec-id", null);
          // once to mark in progress, second to mark failure
          verify(verticle, times(2))
            .updateJobExecutionStatusSafely(eq("job-exec-id"), any(), isNull());
          verify(verticle, times(1)).downloadFromS3(any());
          verify(fileProcessor, times(1))
            .processFile(eq(tempFile), eq("job-exec-id"), any(), isNull());

          // should still cleanup
          verify(queueItemDao, times(1)).deleteDataImportQueueItem("queue-id");

          verifyNoMoreInteractions(queueItemDao);
          verifyNoMoreInteractions(uploadDefinitionService);
          verifyNoMoreInteractions(fileProcessor);

          // should still cleanup
          assertThat(tempFile.exists(), is(false));
        })
      );
  }

  @Test
  public void testProcessQueueItemCleanupFailure(TestContext context)
    throws IOException {
    // static mock for FileUtils.delete:
    try (
      MockedStatic<FileUtils> mock = Mockito.mockStatic(
        FileUtils.class,
        Mockito.CALLS_REAL_METHODS
      )
    ) {
      // fails to delete file, but the error should be swallowed
      mock
        .when(() -> FileUtils.delete(any(File.class)))
        .thenThrow(new IOException());

      File tempFile = temporaryFolder.newFile();

      DataImportQueueItem queueItem = new DataImportQueueItem()
        .withId("queue-id")
        .withJobExecutionId("job-exec-id")
        .withDataType("MARC");

      doReturn(null).when(verticle).getConnectionParams(any());

      doReturn(tempFile).when(verticle).createLocalFile(queueItem);

      when(uploadDefinitionService.getJobExecutionById("job-exec-id", null))
        .thenThrow(new RuntimeException("test exception"));

      doReturn(Future.succeededFuture())
        .when(verticle)
        .updateJobExecutionStatusSafely(any(), any(), any());

      verticle
        .processQueueItem(queueItem)
        .onComplete(
          context.asyncAssertFailure(v -> {
            verify(verticle, times(1)).createLocalFile(queueItem);
            verify(uploadDefinitionService, times(1))
              .getJobExecutionById("job-exec-id", null);
            verify(verticle, times(1))
              .updateJobExecutionStatusSafely(
                eq("job-exec-id"),
                any(),
                isNull()
              );

            // should still cleanup
            verify(queueItemDao, times(1))
              .deleteDataImportQueueItem("queue-id");

            verifyNoMoreInteractions(queueItemDao);
            verifyNoMoreInteractions(uploadDefinitionService);
            verifyNoMoreInteractions(fileProcessor);

            // should still exist despite attempt to cleanup
            mock.verify(() -> FileUtils.delete(any(File.class)), times(1));
            mock.verifyNoMoreInteractions();

            assertThat(tempFile.exists(), is(true));
          })
        );
    }
  }

  @Test
  public void testProcessQueueItemEarlyFailure(TestContext context)
    throws IOException {
    // fails to early to create a file
    try (
      MockedStatic<FileUtils> mock = Mockito.mockStatic(
        FileUtils.class,
        Mockito.CALLS_REAL_METHODS
      )
    ) {
      DataImportQueueItem queueItem = new DataImportQueueItem()
        .withId("queue-id")
        .withJobExecutionId("job-exec-id")
        .withDataType("MARC");

      doReturn(null).when(verticle).getConnectionParams(any());

      doThrow(new UncheckedIOException(new IOException()))
        .when(verticle)
        .createLocalFile(queueItem);

      doReturn(Future.succeededFuture())
        .when(verticle)
        .updateJobExecutionStatusSafely(any(), any(), any());

      verticle
        .processQueueItem(queueItem)
        .onComplete(
          context.asyncAssertFailure(v -> {
            verify(verticle, times(1)).createLocalFile(queueItem);
            verify(verticle, times(1))
              .updateJobExecutionStatusSafely(
                eq("job-exec-id"),
                any(),
                isNull()
              );

            // should still cleanup queue item
            verify(queueItemDao, times(1))
              .deleteDataImportQueueItem("queue-id");

            verifyNoMoreInteractions(queueItemDao);
            verifyNoMoreInteractions(uploadDefinitionService);
            verifyNoMoreInteractions(fileProcessor);

            // no calls to delete, though, since we had no file
            mock.verifyNoInteractions();
          })
        );
    }
  }
}
