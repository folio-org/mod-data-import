package org.folio.service.file;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.Semaphore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.DataImportQueueItemDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaConfig;
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
import org.springframework.stereotype.Component;

/**
 * Worker verticle to handle running jobs from S3 storage.
 *
 * This is configured as a verticle to enable
 */
@Component
public class S3JobRunningVerticle extends AbstractVerticle {

  private static final int MS_BETWEEN_IDLE_POLLS = 5000;

  private static final Logger LOGGER = LogManager.getLogger();

  private DataImportQueueItemDao queueItemDao;

  private MinioStorageService minioStorageService;
  private ScoreService scoreService;
  private SystemUserAuthService systemUserService;
  private UploadDefinitionService uploadDefinitionService;

  private ParallelFileChunkingProcessor fileProcessor;

  // used to allow wake-ups from other threads and the timer
  private final Semaphore semaphore;

  @Autowired
  public S3JobRunningVerticle(
    Vertx vertx,
    DataImportQueueItemDao queueItemDao,
    MinioStorageService minioStorageService,
    ScoreService scoreService,
    SystemUserAuthService systemUserService,
    UploadDefinitionService uploadDefinitionService,
    KafkaConfig kafkaConfig
  ) {
    LOGGER.info("Constructing S3JobRunningVerticle");

    this.vertx = vertx;

    this.queueItemDao = queueItemDao;

    this.minioStorageService = minioStorageService;
    this.systemUserService = systemUserService;
    this.scoreService = scoreService;
    this.uploadDefinitionService = uploadDefinitionService;

    this.fileProcessor = new ParallelFileChunkingProcessor(vertx, kafkaConfig);

    this.semaphore = new Semaphore(1);
  }

  @Override
  public void start() {
    LOGGER.info("Running S3JobRunningVerticle");

    this.run();
  }

  public void run() {
    semaphore.release();
    this.pollForJobs();
  }

  /**
   * Loops indefinitely, polling for available jobs.
   * This is the best approach, as the only other option is to use a trigger in the DB, which also requires polling.
   * The semaphore is used to allow us to wake this loop up on an interval and when a job is sent to this worker.
   */
  private synchronized void pollForJobs() {
    try {
      semaphore.acquire();

      // we want to drain all permits so that only one is available at a time
      // e.g. if we're manually invoked and the timer ends, we don't want to loop extra
      semaphore.drainPermits();

      this.scoreService.getBestQueueItemAndMarkInProgress()
        .compose(optional -> {
          // a promise with result of if a queue item was processed or not
          // this determines cool down before next check
          Promise<Boolean> promise = Promise.promise();

          if (optional.isEmpty()) {
            promise.complete(false);
          } else {
            processQueueItem(optional.get())
              .map(v -> true)
              .onComplete(promise::handle);
          }

          return promise.future();
        })
        .onComplete(result -> {
          if (result.succeeded() && Boolean.TRUE.equals(result.result())) {
            this.run();
          } else if (result.succeeded()) {
            // wait before checking again
            LOGGER.info(
              "No queue items available to run, checking again in {}ms",
              MS_BETWEEN_IDLE_POLLS
            );
            vertx.setTimer(MS_BETWEEN_IDLE_POLLS, v -> this.run());
          } else {
            LOGGER.error("Error running queue item...", result.cause());
            result.cause().printStackTrace();
            vertx.setTimer(MS_BETWEEN_IDLE_POLLS, v -> this.run());
          }
        });
    } catch (InterruptedException e) {
      LOGGER.fatal("Interrupted while waiting for semaphore", e);
      Thread.currentThread().interrupt();
    }
  }

  protected Future<QueueJob> processQueueItem(DataImportQueueItem queueItem) {
    LOGGER.info(
      "Starting to process job execution {}",
      queueItem.getJobExecutionId()
    );

    OkapiConnectionParams params = getConnectionParams(
      queueItem.getTenant(),
      queueItem.getOkapiUrl()
    );

    File localFile;
    try {
      localFile =
        File.createTempFile(
          "di-tmp-",
          // later stage requires correct file extension
          Path.of(queueItem.getFilePath()).getFileName().toString()
        );
      LOGGER.info("Created temporary file {}", localFile.toPath());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

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
        uploadDefinitionService
          .updateJobExecutionStatus(
            job.getJobExecution().getId(),
            new StatusDto().withStatus(StatusDto.Status.PROCESSING_IN_PROGRESS),
            params
          )
          .map(successful -> {
            if (Boolean.FALSE.equals(successful)) {
              throw new IllegalStateException(
                "Unable to mark job as in progress"
              );
            }
            return job;
          })
      )
      .compose(job ->
        minioStorageService
          .readFile(job.getJobExecution().getSourcePath())
          .map(inputStream -> job.withInputStream(inputStream))
      )
      .map(job -> {
        try {
          OutputStream outputStream = new FileOutputStream(job.getFile());

          job.getInputStream().transferTo(outputStream);

          job.getInputStream().close();
          outputStream.close();

          return job;
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      })
      .map(job -> {
        fileProcessor.processFile(
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
        );
        return job;
      })
      .onComplete(ar -> {
        queueItemDao.deleteDataImportQueueItem(queueItem.getId());
        try {
          FileUtils.delete(localFile);
        } catch (IOException e) {
          LOGGER.error(
            "Could not clean up temporary file {}: ",
            localFile.toPath(),
            e
          );
        }

        if (ar.failed()) {
          LOGGER.error(ar.cause());
          ar.cause().printStackTrace();

          uploadDefinitionService.updateJobExecutionStatus(
            queueItem.getJobExecutionId(),
            new StatusDto()
              .withErrorStatus(ErrorStatus.FILE_PROCESSING_ERROR)
              .withStatus(StatusDto.Status.ERROR),
            params
          );
        }
      });
  }

  /**
   * Authenticate and get connection parameters (Okapi URL/token)
   */
  private OkapiConnectionParams getConnectionParams(
    String tenant,
    String okapiUrl
  ) {
    OkapiConnectionParams provisionalParams = new OkapiConnectionParams(
      Map.of(
        "x-okapi-url",
        okapiUrl,
        "x-okapi-tenant",
        tenant,
        "x-okapi-token",
        ""
      ),
      vertx
    );

    String token = systemUserService.getAuthToken(provisionalParams);

    return new OkapiConnectionParams(
      Map.of(
        "x-okapi-url",
        okapiUrl,
        "x-okapi-tenant",
        tenant,
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
  private static class QueueJob {

    private DataImportQueueItem queueItem;
    private JobExecution jobExecution;
    private InputStream inputStream;
    private File file;
  }
}
