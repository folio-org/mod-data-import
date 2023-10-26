package org.folio.service.file;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
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
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.StatusDto.ErrorStatus;
import org.folio.service.auth.PermissionsClient;
import org.folio.service.auth.PermissionsClient.PermissionUser;
import org.folio.service.auth.SystemUserAuthService;
import org.folio.service.processing.ParallelFileChunkingProcessor;
import org.folio.service.processing.ranking.ScoreService;
import org.folio.service.s3storage.MinioStorageService;
import org.folio.service.upload.UploadDefinitionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Worker verticle to handle running jobs from S3 storage.
 *
 * This is configured as a verticle to enable asynchronous processing apart from all normal HTTP/API threads
 */
@Component
public class S3JobRunningVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LogManager.getLogger();

  protected static final AtomicInteger workersInUse = new AtomicInteger(0);

  private final DataImportQueueItemDao queueItemDao;
  private final MinioStorageService minioStorageService;
  private final PermissionsClient permissionsClient;
  private final ScoreService scoreService;
  private final SystemUserAuthService systemUserService;
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
    PermissionsClient permissionsClient,
    ScoreService scoreService,
    SystemUserAuthService systemUserService,
    UploadDefinitionService uploadDefinitionService,
    ParallelFileChunkingProcessor fileProcessor,
    @Value("${ASYNC_PROCESSOR_POLL_INTERVAL_MS:5000}") int pollInterval,
    @Value("${ASYNC_PROCESSOR_MAX_WORKERS_COUNT:1}") int maxWorkersCount
  ) {
    this.vertx = vertx;

    this.queueItemDao = queueItemDao;

    this.minioStorageService = minioStorageService;
    this.permissionsClient = permissionsClient;
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
    vertx.setPeriodic(this.pollInterval, v -> this.pollForJobs());
  }

  protected void pollForJobs() {
    int currentWorkersInUse = workersInUse.get();
    LOGGER.info(
      "Checking for items available to run. Worker usage: {}/{}",
      workersInUse,
      maxWorkersCount
    );

    if (currentWorkersInUse < maxWorkersCount) {
      this.scoreService.getBestQueueItemAndMarkInProgress()
        .onSuccess(opt ->
          opt.ifPresentOrElse(
            (DataImportQueueItem item) -> {
              LOGGER.info("Running item: {}", item);

              workersInUse.incrementAndGet();

              long startTimeStamp = System.currentTimeMillis();

              vertx.runOnContext(v ->
                processQueueItem(item)
                  .onComplete((AsyncResult<QueueJob> vv) -> {
                    int workersLeft = workersInUse.decrementAndGet();
                    LOGGER.info(
                      "Competed running item: {}; Time spent (in ms): {}; Active workers left: {}",
                      item,
                      System.currentTimeMillis() - startTimeStamp,
                      workersLeft
                    );
                  })
              );

              // do it one more time in hope that there more items in the queue
              if (workersInUse.get() < maxWorkersCount) {
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
    LOGGER.info(
      "Starting to process job execution {}",
      queueItem.getJobExecutionId()
    );

    // we need to store out here to ensure it is properly deleted
    // on failure and success
    AtomicReference<File> localFile = new AtomicReference<>();

    return getConnectionParams(queueItem)
      .compose(params ->
        Future
          .succeededFuture(new QueueJob().withQueueItem(queueItem))
          .compose((QueueJob job) ->
            createLocalFile(queueItem)
              .map((File file) -> {
                localFile.set(file);
                return job.withFile(file);
              })
          )
          .compose(job ->
            uploadDefinitionService
              .getJobExecutionById(queueItem.getJobExecutionId(), params)
              .map(job::withJobExecution)
          )
          .compose(job ->
            updateJobExecutionStatusSafely(
              job.getJobExecution().getId(),
              new StatusDto()
                .withStatus(StatusDto.Status.PROCESSING_IN_PROGRESS),
              params
            )
              .map(job)
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
                getUserConnectionParams(
                  job.getJobExecution().getUserId(),
                  params
                )
              )
              .map(job)
          )
          .onFailure((Throwable err) -> {
            LOGGER.error("Unable to start chunk {}", queueItem, err);

            updateJobExecutionStatusSafely(
              queueItem.getJobExecutionId(),
              new StatusDto()
                .withErrorStatus(ErrorStatus.FILE_PROCESSING_ERROR)
                .withStatus(StatusDto.Status.ERROR),
              params
            );
          })
          .onSuccess((QueueJob result) ->
            LOGGER.info(
              "Completed processing job execution {}!",
              queueItem.getJobExecutionId()
            )
          )
          .onComplete((AsyncResult<QueueJob> v) -> {
            queueItemDao.deleteQueueItemById(queueItem.getId());

            File file = localFile.get();
            if (file != null) {
              vertx.fileSystem().delete(file.toString());
            }
          })
      );
  }

  private OkapiConnectionParams getUserConnectionParams(
    String userId,
    OkapiConnectionParams params
  ) {
    PermissionUser permissionUser = permissionsClient
      .getPermissionsUserByUserId(params, userId)
      .orElseThrow(() ->
        LOGGER.throwing(
          new IllegalStateException(
            "User ID " + userId + "who created the job was not found"
          )
        )
      );

    return new OkapiConnectionParams(
      Map.of(
        // shared from the system user
        XOkapiHeaders.URL.toLowerCase(),
        params.getOkapiUrl(),
        XOkapiHeaders.TENANT.toLowerCase(),
        params.getTenantId(),
        XOkapiHeaders.TOKEN.toLowerCase(),
        params.getToken(),
        // provided since some checks are made against these in mod-invoice
        XOkapiHeaders.PERMISSIONS.toLowerCase(),
        new JsonArray(
          permissionUser
            .getPermissions()
            .stream()
            .filter(permission ->
              systemUserService.getPermissionsList().contains(permission) ||
              systemUserService
                .getOptionalPermissionsList()
                .contains(permission)
            )
            .toList()
        )
          .toString()
      ),
      vertx
    );
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

  protected Future<File> createLocalFile(DataImportQueueItem queueItem) {
    return vertx
      .fileSystem()
      .createTempFile(
        "di-tmp-",
        Path.of(queueItem.getFilePath()).getFileName().toString(),
        "rwx------"
      )
      .map(File::new)
      .onSuccess(localFile ->
        LOGGER.info("Created temporary file {}", localFile.toPath())
      );
  }

  /**
   * Authenticate and get connection parameters (Okapi URL/token)
   */
  protected Future<OkapiConnectionParams> getConnectionParams(
    DataImportQueueItem queueItem
  ) {
    OkapiConnectionParams provisionalParams = new OkapiConnectionParams(
      Map.of(
        XOkapiHeaders.URL.toLowerCase(),
        queueItem.getOkapiUrl(),
        XOkapiHeaders.TENANT.toLowerCase(),
        queueItem.getTenant(),
        // filled right after, but we need tenant/URL to get the token
        XOkapiHeaders.TOKEN.toLowerCase(),
        ""
      ),
      vertx
    );

    return systemUserService
      .getAuthToken(provisionalParams)
      .map(token ->
        new OkapiConnectionParams(
          Map.of(
            XOkapiHeaders.URL.toLowerCase(),
            queueItem.getOkapiUrl(),
            XOkapiHeaders.TENANT.toLowerCase(),
            queueItem.getTenant(),
            XOkapiHeaders.TOKEN.toLowerCase(),
            token
          ),
          vertx
        )
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
