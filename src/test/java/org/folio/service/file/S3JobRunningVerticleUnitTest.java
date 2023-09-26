package org.folio.service.file;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
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
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Optional;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

@RunWith(VertxUnitRunner.class)
public class S3JobRunningVerticleUnitTest {

  private static final int POLL_INTERVAL = 100;

  private static final Vertx vertx = Vertx.vertx();

  @Mock
  Vertx mockVertx;

  @Mock
  FileSystem fileSystem;

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

    when(mockVertx.fileSystem()).thenReturn(fileSystem);
    doAnswer(invocation -> {
        invocation.<Handler<Void>>getArgument(0).handle(null);
        return null;
      })
      .when(mockVertx)
      .runOnContext(any());

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
  public void testCreateLocalFileSuccess(TestContext context) {
    File testResult = new File("result");

    when(fileSystem.createTempFile(anyString(), anyString(), anyString()))
      .thenReturn(Future.succeededFuture(testResult.toString()));

    verticle
      .createLocalFile(new DataImportQueueItem().withFilePath("path/test-file"))
      .onComplete(
        context.asyncAssertSuccess(r ->
          assertThat(r.toString(), is(testResult.toString()))
        )
      );
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
  public void testWorkersAtCapacity(TestContext context) {
    S3JobRunningVerticle.workersInUse.set(10);

    // at capacity; we should not request new queue items
    verticle.pollForJobs();

    verifyNoInteractions(scoreService);
  }

  @Test
  public void testWorkersAlmostAtCapacityRunning(TestContext context) {
    S3JobRunningVerticle.workersInUse.set(9);

    when(scoreService.getBestQueueItemAndMarkInProgress())
      .thenReturn(
        Future.succeededFuture(Optional.of(new DataImportQueueItem()))
      );

    doAnswer(invocation -> {
        Promise<QueueJob> promise = Promise.promise();

        // ensure we don't immediately complete it, to simulate realistic situations
        vertx.setTimer(1L, v -> promise.complete(null));

        return promise;
      })
      .when(verticle)
      .processQueueItem(any());

    // should request item
    verticle.pollForJobs();

    vertx.setTimer(
      50L,
      v ->
        context.verify(vv -> {
          // after "running", should not request any additional items
          // (since worker pool will then be at capacity)
          verify(scoreService, times(1)).getBestQueueItemAndMarkInProgress();
          verifyNoMoreInteractions(scoreService);

          // no more requests made
          verify(verticle, times(1)).pollForJobs();

          // back to 9/10 in progress, since our started one here should have finished
          assertThat(S3JobRunningVerticle.workersInUse.get(), is(9));
        })
    );
  }

  @Test
  public void testWorkersNoneToRun(TestContext context) {
    S3JobRunningVerticle.workersInUse.set(0);

    when(scoreService.getBestQueueItemAndMarkInProgress())
      .thenReturn(Future.succeededFuture(Optional.empty()));

    verticle.pollForJobs();

    vertx.setTimer(
      50L,
      v ->
        context.verify(vv -> {
          // after "running", should not request any additional items
          // (since worker pool will then be at capacity)
          verify(scoreService, times(1)).getBestQueueItemAndMarkInProgress();
          verifyNoMoreInteractions(scoreService);

          // no more requests made
          verify(verticle, times(1)).pollForJobs();

          // no processing done
          verify(verticle, never()).processQueueItem(any());

          // back to 0/10 in progress, since our started one here should have finished
          assertThat(S3JobRunningVerticle.workersInUse.get(), is(9));
        })
    );
  }

  @Test
  public void testWorkersRunningMultiple(TestContext context) {
    S3JobRunningVerticle.workersInUse.set(0);

    // return two jobs
    when(scoreService.getBestQueueItemAndMarkInProgress())
      .thenReturn(
        Future.succeededFuture(Optional.of(new DataImportQueueItem()))
      )
      .thenReturn(
        Future.succeededFuture(Optional.of(new DataImportQueueItem()))
      )
      .thenReturn(Future.succeededFuture(Optional.empty()));

    doReturn(Future.succeededFuture()).when(verticle).processQueueItem(any());

    // should request item
    verticle.pollForJobs();

    vertx.setTimer(
      50L,
      v ->
        context.verify(vv -> {
          // after "running", should not request any additional items
          // (since worker pool will then be at capacity)
          verify(scoreService, times(3)).getBestQueueItemAndMarkInProgress();
          verifyNoMoreInteractions(scoreService);

          // called 3x total
          verify(verticle, times(3)).pollForJobs();

          // our "jobs" should have finished
          assertThat(S3JobRunningVerticle.workersInUse.get(), is(0));
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

    doReturn(Future.succeededFuture(tempFile))
      .when(verticle)
      .createLocalFile(queueItem);

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

          // should still cleanup, but cleanup is async so
          vertx.setTimer(
            50L,
            vv ->
              context.verify(vvv -> assertThat(tempFile.exists(), is(false)))
          );
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

    doReturn(Future.succeededFuture(tempFile))
      .when(verticle)
      .createLocalFile(queueItem);

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

          // should still cleanup, but cleanup is async so
          vertx.setTimer(
            50L,
            vv ->
              context.verify(vvv -> assertThat(tempFile.exists(), is(false)))
          );
        })
      );
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
