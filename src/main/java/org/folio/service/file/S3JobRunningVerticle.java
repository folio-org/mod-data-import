package org.folio.service.file;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.DataImportQueueItemDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.StatusDto.ErrorStatus;
import org.folio.service.auth.SystemUserAuthService;
import org.folio.service.processing.ParallelFileChunkingProcessor;
import org.folio.service.processing.ranking.ScoreService;
import org.folio.service.s3storage.MinioStorageService;
import org.folio.service.upload.UploadDefinitionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Worker verticle to handle running jobs from S3 storage.
 *
 * This is configured as a verticle to enable asynchronous processing apart from all normal HTTP/API threads
 */
@Component
public class S3JobRunningVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LogManager.getLogger();

  private static final AtomicInteger workCounter = new AtomicInteger(0);

  private static final AtomicBoolean pollingIsActive = new AtomicBoolean(false);

  private DataImportQueueItemDao queueItemDao;

  private MinioStorageService minioStorageService;
  private ScoreService scoreService;
  private SystemUserAuthService systemUserService;
  private UploadDefinitionService uploadDefinitionService;

  private ParallelFileChunkingProcessor fileProcessor;

  private int pollInterval;

  private int maxWorkersCount;

  // constructs the processor automatically
  @Autowired
  public S3JobRunningVerticle(
    Vertx vertx,
    DataImportQueueItemDao queueItemDao,
    MinioStorageService minioStorageService,
    ScoreService scoreService,
    SystemUserAuthService systemUserService,
    UploadDefinitionService uploadDefinitionService,
    ParallelFileChunkingProcessor fileProcessor,
    @Value("${ASYNC_PROCESSOR_POLL_INTERVAL_MS:5000}") int pollInterval,
    @Value("${ASYNC_PROCESSOR_MAX_WORKERS_COUNT:5}") int maxWorkersCount
  ) {
    this.vertx = vertx;

    this.queueItemDao = queueItemDao;

    this.minioStorageService = minioStorageService;
    this.systemUserService = systemUserService;
    this.scoreService = scoreService;
    this.uploadDefinitionService = uploadDefinitionService;
    this.fileProcessor = fileProcessor;
    this.pollInterval = pollInterval;
    this.maxWorkersCount = maxWorkersCount;
  }

  @Override
  public void start() {
    LOGGER.info("Running S3JobRunningVerticle");
    vertx.runOnContext(v -> this.pollForJobs2());
  }

  /**
   * Loops indefinitely, polling for available jobs.
   * This is the best approach, as the only other option is to use a trigger in the DB, which also requires polling.
   */
  protected synchronized void pollForJobs() {
    this.scoreService.getBestQueueItemAndMarkInProgress()
      .compose((Optional<DataImportQueueItem> optional) -> {
        // a promise with result of if a queue item was processed or not
        // this determines cool down before next check
        Promise<Boolean> promise = Promise.promise();

        optional.ifPresentOrElse(
          item ->
            processQueueItem(item).map(v -> true).onComplete(promise::handle),
          () -> promise.complete(false)
        );

        return promise.future();
      })
      .onSuccess((Boolean didRunJob) -> {
        if (Boolean.TRUE.equals(didRunJob)) {
          // use setTimer to avoid a stack overflow on multiple repeats
          vertx.setTimer(0, v -> this.pollForJobs());
        } else {
          // wait before checking again
          LOGGER.info(
            "No queue items available to run, checking again in {}ms",
            this.pollInterval
          );

          vertx.setTimer(this.pollInterval, v -> this.pollForJobs());
        }
      })
      .onFailure((Throwable err) -> {
        LOGGER.error("Error running queue item...", err);
        vertx.setTimer(this.pollInterval, v -> this.pollForJobs());
      });
  }


  protected void pollForJobs2() {
    if (pollingIsActive.compareAndSet(false, true)) {
      try {
        LOGGER.info("Checking for items available to run");

        var workers = workCounter.get();
        if (workers < maxWorkersCount) {
          this.scoreService
            .getBestQueueItemAndMarkInProgress()
            .onComplete(ar -> {
              if (ar.succeeded()) {
                var opt = ar.result();
                opt.ifPresentOrElse(item -> {
                  LOGGER.info("Item available to run: " + item);
                  var localworkers = workCounter.incrementAndGet();
                  processQueueItem(item).onComplete(v -> {
                    workCounter.decrementAndGet();
                    LOGGER.info("Competed Item run: " + item);
                    vertx.runOnContext(vv -> this.pollForJobs2());
                  });
                  if (localworkers < maxWorkersCount) {
                    vertx.runOnContext(v -> this.pollForJobs2());
                  }
                }, () -> {
                  LOGGER.info("No Items available to run: ");
                  vertx.setTimer(this.pollInterval, v -> this.pollForJobs2());
                });
              } else {//TODO: add some useful error message with a stacktrace
                ar.cause().printStackTrace();
                LOGGER.error(ar.cause());
                vertx.setTimer(this.pollInterval, v -> this.pollForJobs2());
              }
            });
        } else {
          LOGGER.info("All workers are active: " + workers);
        }
      } finally {
        pollingIsActive.set(false);
      }
    }
  }

  protected Future<QueueJob> processQueueItem(DataImportQueueItem queueItem) {
    LOGGER.info(
      "Starting to process job execution {}",
      queueItem.getJobExecutionId()
    );

    OkapiConnectionParams params = getConnectionParams(queueItem);

    File localFile = createLocalFile(queueItem);

    return Future
      .succeededFuture(
        new QueueJob().withQueueItem(queueItem).withFile(localFile)
      )
      .compose(job ->
        uploadDefinitionService
          .getJobExecutionById(queueItem.getJobExecutionId(), params)
          .map(jobExecution -> job.withJobExecution(jobExecution))
      )
      .compose(job ->
        updateJobExecutionStatusSafely(
          job.getJobExecution().getId(),
          new StatusDto().withStatus(StatusDto.Status.PROCESSING_IN_PROGRESS),
          params
        )
          .map(v -> job)
      )
      .compose(this::downloadFromS3)
      .compose(job ->
        fileProcessor
          .processFile(
            job.getFile(),
            job.getJobExecution().getId(),
            // this is the only part used on our end
            new JobProfileInfo()
              .withDataType(
                JobProfileInfo.DataType.fromValue(
                  job.getQueueItem().getDataType()
                )
              ),
            params
          )
          .map(v -> job)
      )
      .onFailure((Throwable err) -> {
        LOGGER.error("Unable to start chunk {}", queueItem, err);
        err.printStackTrace();

        updateJobExecutionStatusSafely(
          queueItem.getJobExecutionId(),
          new StatusDto()
            .withErrorStatus(ErrorStatus.FILE_PROCESSING_ERROR)
            .withStatus(StatusDto.Status.ERROR),
          params
        );
      })
      .onSuccess((QueueJob result) -> {
        queueItemDao.deleteDataImportQueueItem(queueItem.getId());

        LOGGER.info(
          "Completed processing job execution {}!",
          queueItem.getJobExecutionId()
        );
      })
      .onComplete(v -> {
        try {
          FileUtils.delete(localFile);
        } catch (IOException e) {
          LOGGER.error(
            "Could not clean up temporary file {}: ",
            localFile.toPath(),
            e
          );
        }
      });
  }

  protected Future<Void> updateJobExecutionStatusSafely(
    String jobExecutionId,
    StatusDto status,
    OkapiConnectionParams params
  ) {
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

  protected File createLocalFile(DataImportQueueItem queueItem) {
    File localFile;
    try {
      localFile =
        Files
          .createTempFile(
            "di-tmp-",
            // later stage requires correct file extension
            Path.of(queueItem.getFilePath()).getFileName().toString(),
            PosixFilePermissions.asFileAttribute(
              PosixFilePermissions.fromString("rwx------")
            )
          )
          .toFile();
      LOGGER.info("Created temporary file {}", localFile.toPath());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return localFile;
  }

  /**
   * Authenticate and get connection parameters (Okapi URL/token)
   */
  protected OkapiConnectionParams getConnectionParams(
    DataImportQueueItem queueItem
  ) {
    OkapiConnectionParams provisionalParams = new OkapiConnectionParams(
      Map.of(
        "x-okapi-url",
        queueItem.getOkapiUrl(),
        "x-okapi-tenant",
        queueItem.getTenant(),
        // filled right after, but we need tenant/URL to get the token
        "x-okapi-token",
        ""
      ),
      vertx
    );

    String token = systemUserService.getAuthToken(provisionalParams);

    return new OkapiConnectionParams(
      Map.of(
        "x-okapi-url",
        queueItem.getOkapiUrl(),
        "x-okapi-tenant",
        queueItem.getTenant(),
        "x-okapi-token",
        token
      ),
      vertx
    );
  }

  @Override
  public void stop() {
    LOGGER.info("Stopping S3JobRunningVerticle");
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
