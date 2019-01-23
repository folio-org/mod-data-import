package org.folio.service.processing;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.lang3.mutable.MutableInt;
import org.folio.dataImport.util.OkapiConnectionParams;
import org.folio.dataImport.util.RestUtil;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollection;
import org.folio.rest.jaxrs.model.JobProfile;
import org.folio.rest.jaxrs.model.ProcessFilesRqDto;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.service.processing.reader.SourceReader;
import org.folio.service.processing.reader.SourceReaderBuilder;
import org.folio.service.storage.FileStorageService;
import org.folio.service.storage.FileStorageServiceBuilder;
import org.folio.service.upload.UploadDefinitionService;
import org.folio.service.upload.UploadDefinitionServiceImpl;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.folio.dataImport.util.RestUtil.validateAsyncResult;
import static org.folio.rest.RestVerticle.MODULE_SPECIFIC_ARGS;
import static org.folio.rest.jaxrs.model.StatusDto.Status.ERROR;
import static org.folio.rest.jaxrs.model.StatusDto.Status.IMPORT_FINISHED;
import static org.folio.rest.jaxrs.model.StatusDto.Status.IMPORT_IN_PROGRESS;

/**
 * Processing files in parallel threads, one thread per one file.
 * File chunking process implies reading and splitting the file into chunks of data.
 * Every chunk represents collection of source records, see ({@link org.folio.rest.jaxrs.model.RawRecordsDto}).
 * After the target file gets split into records, ParallelFileChunkingProcessor sends records to the mod-source-record-manager
 * for further processing.
 */
public class ParallelFileChunkingProcessor implements FileProcessor {
  private static final int THREAD_POOL_SIZE =
    Integer.parseInt(MODULE_SPECIFIC_ARGS.getOrDefault("file.processing.thread.pool.size", "20"));
  private static final String JOB_PROFILE_SERVICE_URL = "/change-manager/jobExecution/%s/jobProfile";
  private static final String JOB_CHILDREN_SERVICE_URL = "/change-manager/jobExecution/%s/children";
  private static final String RAW_RECORDS_SERVICE_URL = "/change-manager/records/";
  private static final Logger LOGGER = LoggerFactory.getLogger(ParallelFileChunkingProcessor.class);

  private Vertx vertx;
  /* WorkerExecutor provides separate worker pool for code execution */
  private WorkerExecutor executor;

  public ParallelFileChunkingProcessor(Vertx vertx) {
    this.vertx = vertx;
    this.executor = this.vertx.createSharedWorkerExecutor("processing-files-thread-pool", THREAD_POOL_SIZE);
  }

  @Override
  public void process(JsonObject jsonRequest, JsonObject jsonParams) {
    ProcessFilesRqDto request = jsonRequest.mapTo(ProcessFilesRqDto.class);
    UploadDefinition uploadDefinition = request.getUploadDefinition();
    JobProfile jobProfile = request.getJobProfile();
    OkapiConnectionParams params = jsonParams.mapTo(OkapiConnectionParams.class);
    String tenantId = params.getTenantId();
    UploadDefinitionService uploadDefinitionService = new UploadDefinitionServiceImpl(vertx, tenantId);
    FileStorageServiceBuilder.build(this.vertx, tenantId, params).setHandler(fileStorageServiceAr -> {
      if (fileStorageServiceAr.failed()) {
        LOGGER.error("Can not build file storage service. Cause: {}", fileStorageServiceAr.cause());
      } else {
        FileStorageService fileStorageService = fileStorageServiceAr.result();
        List<FileDefinition> fileDefinitions = uploadDefinition.getFileDefinitions();
        updateJobsProfile(uploadDefinition.getMetaJobExecutionId(), jobProfile, params).setHandler(updatedProfileAsyncResult -> {
          if (updatedProfileAsyncResult.failed()) {
            LOGGER.error("Can not update profile for jobs. Cause: {}", updatedProfileAsyncResult.cause());
          } else {
            for (FileDefinition fileDefinition : fileDefinitions) {
              this.executor.executeBlocking(blockingFuture -> uploadDefinitionService
                  .updateJobExecutionStatus(fileDefinition.getJobExecutionId(), new StatusDto().withStatus(IMPORT_IN_PROGRESS), params)
                  .compose(ar -> processFile(fileDefinition, jobProfile, fileStorageService, params))
                  .compose(ar -> uploadDefinitionService.updateJobExecutionStatus(fileDefinition.getJobExecutionId(), new StatusDto().withStatus(IMPORT_FINISHED), params))
                  .setHandler(ar -> {
                    if (ar.failed()) {
                      LOGGER.error("Can not process file {}. Cause: {}", fileDefinition.getSourcePath(), ar.cause());
                      uploadDefinitionService.updateJobExecutionStatus(fileDefinition.getJobExecutionId(), new StatusDto().withStatus(ERROR), params);
                      blockingFuture.fail(ar.cause());
                    } else {
                      LOGGER.info("File {} successfully processed.", fileDefinition.getSourcePath());
                      blockingFuture.complete();
                    }
                  }),
                false,
                null
              );
            }
          }
        });
      }
    });
  }

  /**
   * Processing file
   *
   * @param fileDefinition     fileDefinition entity
   * @param jobProfile         job profile, contains profile type
   * @param fileStorageService service to obtain file
   * @param params             parameters necessary for connection to the OKAPI
   * @return Future
   */
  private Future<Void> processFile(FileDefinition fileDefinition,
                                   JobProfile jobProfile,
                                   FileStorageService fileStorageService,
                                   OkapiConnectionParams params) {
    Future<Void> resultFuture = Future.future();
    MutableInt recordsCounter = new MutableInt(0);
    try {
      File file = fileStorageService.getFile(fileDefinition.getSourcePath());
      SourceReader reader = SourceReaderBuilder.build(file, jobProfile);
      /*
        If one of the dedicated handlers for sending chunks is failed, then all the other senders have to be aware of that
        in a terms of the target file processing.
        Using atomic variable because it's value stored in worker thread, but changes in event-loop thread.
      */
      AtomicBoolean canSendNextChunk = new AtomicBoolean(true);
      List<Future> chunkSentFutures = new ArrayList<>();
      while (reader.hasNext()) {
        if (canSendNextChunk.get()) {
          List<String> records = reader.next();
          recordsCounter.add(records.size());
          RawRecordsDto chunk = new RawRecordsDto().withRecords(records).withCounter(recordsCounter.getValue()).withLast(false);
          chunkSentFutures.add(postRawRecords(fileDefinition.getJobExecutionId(), chunk, canSendNextChunk, params));
        } else {
          String errorMessage = "Can not send next chunk of file " + fileDefinition.getSourcePath();
          LOGGER.error(errorMessage);
          return Future.failedFuture(errorMessage);
        }
      }
      CompositeFuture.all(chunkSentFutures).setHandler(ar -> {
        if (ar.failed()) {
          String errorMessage = "File processing stopped. Can not send chunks of the file " + fileDefinition.getSourcePath();
          LOGGER.error(errorMessage);
          resultFuture.fail(errorMessage);
        } else {
          // Sending the last chunk
          RawRecordsDto chunk = new RawRecordsDto().withCounter(recordsCounter.getValue()).withLast(true);
          postRawRecords(fileDefinition.getJobExecutionId(), chunk, canSendNextChunk, params).setHandler(r -> {
            if (r.failed()) {
              String errorMessage = "File processing stopped. Can not send the last chunk of the file " + fileDefinition.getSourcePath();
              LOGGER.error(errorMessage);
              resultFuture.fail(errorMessage);
            } else {
              LOGGER.info("File " + fileDefinition.getSourcePath() + " has been successfully sent.");
              resultFuture.complete();
            }
          });
        }
      });
    } catch (Exception e) {
      String errorMessage = String.format("Can not process file: %s. Cause: %s", fileDefinition.getSourcePath(), e.getMessage());
      LOGGER.error(errorMessage);
      resultFuture.fail(errorMessage);
    }
    return resultFuture;
  }

  /**
   * Sends chunk with records to the corresponding consumer
   *
   * @param jobExecutionId   job id
   * @param chunk            chunk of records
   * @param canSendNextChunk flag the identifies has the last record been successfully sent and can the
   * @param params           parameters necessary for connection to the OKAPI
   * @return Future
   */
  private Future<Void> postRawRecords(String jobExecutionId, RawRecordsDto chunk, AtomicBoolean canSendNextChunk, OkapiConnectionParams params) {
    Future<Void> future = Future.future();
    RestUtil.doRequest(params, RAW_RECORDS_SERVICE_URL + jobExecutionId, HttpMethod.POST, chunk)
      .setHandler(responseResult -> {
        if (responseResult.failed()) {
          canSendNextChunk.set(false);
          String errorMessage = "Can not post raw records for job " + jobExecutionId + ". Cause: " + responseResult.cause();
          LOGGER.error(errorMessage);
          future.fail(errorMessage);
        } else {
          LOGGER.info("Chunk of records with size {} successfully posted for job {}", chunk.getRecords().size(), jobExecutionId);
          future.complete();
        }
      });
    return future;
  }

  /**
   * Updates JobExecutions with given JobProfile value
   *
   * @param metaJobExecutionId parent JobExecution id
   * @param jobProfile         JobProfile entity
   * @param params             parameters necessary for connection to the OKAPI
   * @return Future
   */
  private Future<Boolean> updateJobsProfile(String metaJobExecutionId, JobProfile jobProfile, OkapiConnectionParams
    params) {
    Future<Boolean> future = Future.future();
    RestUtil.doRequest(params, String.format(JOB_CHILDREN_SERVICE_URL, metaJobExecutionId), HttpMethod.GET, null).setHandler(childrenJobsAr -> {
      if (validateAsyncResult(childrenJobsAr, future)) {
        List<JobExecution> childJobs = childrenJobsAr.result().getJson().mapTo(JobExecutionCollection.class).getJobExecutions();
        // TODO remove id generating when UI will receive proper job profile with id
        jobProfile.withId(UUID.randomUUID().toString());
        List<Future> updateJobProfileFutures = new ArrayList<>(childJobs.size());
        for (JobExecution job : childJobs) {
          updateJobProfileFutures.add(updateJobProfile(job.getId(), jobProfile, params));
        }
        CompositeFuture.all(updateJobProfileFutures).setHandler(updatedJobsProfileAr -> {
          if (updatedJobsProfileAr.failed()) {
            future.fail(updatedJobsProfileAr.cause());
          } else {
            LOGGER.info("All the child jobs have been updated by job profile, parent job {}", metaJobExecutionId);
            future.complete();
          }
        });
      }
    });
    return future;
  }

  /**
   * Updates job profile
   *
   * @param jobId      id of the JobExecution entity
   * @param jobProfile JobProfile entity
   * @param params     parameters necessary for connection to the OKAPI
   * @return Future
   */
  private Future updateJobProfile(String jobId, JobProfile jobProfile, OkapiConnectionParams params) {
    Future future = Future.future();
    RestUtil.doRequest(params, String.format(JOB_PROFILE_SERVICE_URL, jobId), HttpMethod.PUT, jobProfile).setHandler(ar -> {
      if (ar.failed()) {
        LOGGER.error("Can not update job profile for job {}", jobId);
        future.fail(ar.cause());
      } else {
        LOGGER.info("Job profile for job {} successfully updated.", jobId);
        future.complete();
      }
    });
    return future;
  }
}
