package org.folio.service.file;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.DataImportQueueItemDao;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.StatusDto.ErrorStatus;
import org.folio.service.processing.ParallelFileChunkingProcessor;
import org.folio.service.processing.ranking.ScoreService;
import org.folio.service.processing.split.FileSplitUtilities;
import org.folio.service.s3storage.MinioStorageService;
import org.folio.service.upload.UploadDefinitionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Worker verticle to handle running jobs from S3 storage.
 *
 * <p>
 * This is configured as a verticle to enable asynchronous processing apart from all normal HTTP/API threads
 */
@Component
public class S3JobRunningVerticle extends AbstractVerticle {

  protected static final AtomicInteger WORKERS_IN_USE = new AtomicInteger(0);

  private static final Logger LOGGER = LogManager.getLogger();

  private final DataImportQueueItemDao queueItemDao;
  private final MinioStorageService minioStorageService;
  private final ScoreService scoreService;
  private final UploadDefinitionService uploadDefinitionService;

  private final ParallelFileChunkingProcessor fileProcessor;

  private final int pollInterval;

  private final int maxWorkersCount;

  // constructs the processor automatically as it is a @Component
  @Autowired
  public S3JobRunningVerticle(
    Vertx vertx,
    DataImportQueueItemDao queueItemDao,
    MinioStorageService minioStorageService,
    ScoreService scoreService,
    UploadDefinitionService uploadDefinitionService,
    ParallelFileChunkingProcessor fileProcessor,
    @Value("${ASYNC_PROCESSOR_POLL_INTERVAL_MS:5000}") int pollInterval,
    @Value("${ASYNC_PROCESSOR_MAX_WORKERS_COUNT:1}") int maxWorkersCount
  ) {
    this.vertx = vertx;

    this.queueItemDao = queueItemDao;

    this.minioStorageService = minioStorageService;
    this.scoreService = scoreService;
    this.uploadDefinitionService = uploadDefinitionService;
    this.fileProcessor = fileProcessor;
    this.pollInterval = pollInterval;
    this.maxWorkersCount = maxWorkersCount;
  }

  @Override
  public void start() {
    LOGGER.info("Running S3JobRunningVerticle");
    vertx.setPeriodic(this.pollInterval, v -> this.pollForJobs());
  }

  @Override
  public void stop() {
    LOGGER.info("Stopping S3JobRunningVerticle");
  }

  protected void pollForJobs() {
    int currentWorkersInUse = WORKERS_IN_USE.get();
    LOGGER.info(
      "Checking for items available to run. Worker usage: {}/{}",
      WORKERS_IN_USE,
      maxWorkersCount
    );

    if (currentWorkersInUse < maxWorkersCount) {
      this.scoreService.getBestQueueItemAndMarkInProgress()
        .onSuccess(opt ->
          opt.ifPresentOrElse(
            (DataImportQueueItem item) -> {
              LOGGER.info("Running item: {}", item);

              WORKERS_IN_USE.incrementAndGet();

              long startTimeStamp = System.currentTimeMillis();

              vertx.runOnContext(v ->
                processQueueItem(item)
                  .onComplete((AsyncResult<QueueJob> vv) -> {
                    int workersLeft = WORKERS_IN_USE.decrementAndGet();
                    LOGGER.info(
                      "Competed running item: {}; Time spent (in ms): {}; Active workers left: {}",
                      item,
                      System.currentTimeMillis() - startTimeStamp,
                      workersLeft
                    );
                  })
              );

              // do it one more time in hope that there more items in the queue
              if (WORKERS_IN_USE.get() < maxWorkersCount) {
                pollForJobs();
              }
            },
            () -> LOGGER.info("No Items available to run.")
          )
        )
        .onFailure(err -> LOGGER.error("Unable to get job from queue:", err));
    }
  }

  protected Future<QueueJob> processQueueItem(DataImportQueueItem queueItem) {
    LOGGER.info("Starting to process job execution {}", queueItem.getJobExecutionId());

    // we need to store out here to ensure it is properly deleted
    // on failure and success
    AtomicReference<File> localFile = new AtomicReference<>();

    ConnectionParams params = getConnectionParams(queueItem);

    return Future.succeededFuture(new QueueJob().withQueueItem(queueItem))
      .compose((QueueJob job) ->
        createLocalFile(queueItem)
          .map((File file) -> {
            localFile.set(file);
            return job.withFile(file);
          })
      )
      .compose(job -> uploadDefinitionService.getJobExecutionById(queueItem.getJobExecutionId(), params)
        .map(job::withJobExecution)
      )
      .compose(job -> updateJobExecutionStatusSafely(job.getJobExecution().getId(),
        new StatusDto().withStatus(StatusDto.Status.PROCESSING_IN_PROGRESS), params)
        .map(job)
      )
      .compose(this::downloadFromS3)
      .compose(job ->
        fileProcessor.processFile(
            job.getFile(),
            job.getJobExecution().getId(),
            // this is the only part used on our end
            new JobProfileInfo()
              .withDataType(JobProfileInfo.DataType.fromValue(job.getQueueItem().getDataType())),
            // we need to include the user ID here since some later checks in mod-invoice/etc use it
            getConnectionParams(queueItem, job.getJobExecution().getUserId()))
          .map(job)
      )
      .onFailure((Throwable err) -> {
        LOGGER.error("Unable to start chunk {}", queueItem, err);

        updateJobExecutionStatusSafely(queueItem.getJobExecutionId(), new StatusDto()
          .withErrorStatus(ErrorStatus.FILE_PROCESSING_ERROR)
          .withStatus(StatusDto.Status.ERROR), params);
      })
      .onSuccess((QueueJob result) ->
        LOGGER.info("Completed processing job execution {}!", queueItem.getJobExecutionId()))
      .onComplete((AsyncResult<QueueJob> v) -> {
        queueItemDao.deleteQueueItemById(queueItem.getId());

        File file = localFile.get();
        if (file != null) {
          vertx.fileSystem().delete(file.toString());
        }
      });
  }

  protected Future<Void> updateJobExecutionStatusSafely(String jobExecutionId,
                                                        StatusDto status,
                                                        ConnectionParams params) {
    return uploadDefinitionService
      .updateJobExecutionStatus(jobExecutionId, status, params)
      .map((Boolean successful) -> {
        if (Boolean.FALSE.equals(successful)) {
          LOGGER.error(
            "Unable to change job {} status to {}",
            jobExecutionId,
            status
          );
          throw new IllegalStateException(
            "Unable to update job execution status"
          );
        }
        return successful;
      })
      .mapEmpty();
  }

  protected Future<QueueJob> downloadFromS3(QueueJob job) {
    return minioStorageService
      .readFile(job.getJobExecution().getSourcePath())
      .map((InputStream inputStream) -> {
        try (
          InputStream autoCloseMe = inputStream;
          OutputStream outputStream = new FileOutputStream(job.getFile())
        ) {
          inputStream.transferTo(outputStream);

          return job;
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      });
  }

  protected Future<File> createLocalFile(DataImportQueueItem queueItem) {
    String prefix = "di-tmp-";
    String suffix = Path.of(queueItem.getFilePath()).getFileName().toString();

    Future<String> tempFileFuture;
    if (FileSplitUtilities.isWindows()) {
      // Windows doesn't support POSIX permissions - create without them
      tempFileFuture = vertx.fileSystem().createTempFile(prefix, suffix);
    } else {
      // Unix/Linux/Mac - use POSIX permissions for security
      tempFileFuture = vertx.fileSystem().createTempFile(prefix, suffix, "rwx------");
    }

    return tempFileFuture
      .map(File::new)
      .onSuccess(localFile ->
        LOGGER.info("Created temporary file {}", localFile.toPath())
      );
  }

  /**
   * Get connection parameters (Okapi URL/token).
   */
  protected ConnectionParams getConnectionParams(DataImportQueueItem queueItem) {
    return ConnectionParams.createSystemUserConnectionParams(
      Map.of(
        XOkapiHeaders.URL, queueItem.getOkapiUrl(),
        XOkapiHeaders.TENANT, queueItem.getTenant(),
        XOkapiHeaders.TOKEN, queueItem.getOkapiToken(),
        XOkapiHeaders.PERMISSIONS, queueItem.getOkapiPermissions(),
        XOkapiHeaders.REQUEST_ID, queueItem.getOkapiRequestId()
      )
    );
  }

  /**
   * Get connection parameters (Okapi URL/token), including a user ID.
   */
  protected ConnectionParams getConnectionParams(DataImportQueueItem queueItem, String userId) {
    return ConnectionParams.createSystemUserConnectionParams(
      Map.of(
        XOkapiHeaders.URL, queueItem.getOkapiUrl(),
        XOkapiHeaders.TENANT, queueItem.getTenant(),
        XOkapiHeaders.TOKEN, queueItem.getOkapiToken(),
        XOkapiHeaders.PERMISSIONS, queueItem.getOkapiPermissions(),
        XOkapiHeaders.REQUEST_ID, queueItem.getOkapiRequestId(),
        XOkapiHeaders.USER_ID, userId
      )
    );
  }

  @Data
  @With
  @NoArgsConstructor
  @AllArgsConstructor
  protected static class QueueJob {

    private DataImportQueueItem queueItem;
    private JobExecution jobExecution;
    private File file;
  }
}
