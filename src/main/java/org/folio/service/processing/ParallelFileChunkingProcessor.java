package org.folio.service.processing;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.handler.impl.HttpStatusException;
import org.apache.commons.collections4.list.UnmodifiableList;
import org.apache.commons.lang3.mutable.MutableInt;
import org.folio.HttpStatus;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.client.ChangeManagerClient;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.ProcessFilesRqDto;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.RecordsMetadata;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.service.processing.coordinator.BlockingCoordinator;
import org.folio.service.processing.coordinator.QueuedBlockingCoordinator;
import org.folio.service.processing.reader.SourceReader;
import org.folio.service.processing.reader.SourceReaderBuilder;
import org.folio.service.storage.FileStorageService;
import org.folio.service.storage.FileStorageServiceBuilder;
import org.folio.service.upload.UploadDefinitionService;
import org.folio.service.upload.UploadDefinitionServiceImpl;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.vertx.core.Future.succeededFuture;
import static org.folio.rest.RestVerticle.MODULE_SPECIFIC_ARGS;
import static org.folio.rest.jaxrs.model.StatusDto.ErrorStatus.FILE_PROCESSING_ERROR;
import static org.folio.rest.jaxrs.model.StatusDto.Status.ERROR;

/**
 * Processing files in parallel threads, one thread per one file.
 * File chunking process implies reading and splitting the file into chunks of data.
 * Every chunk represents collection of source records, see ({@link org.folio.rest.jaxrs.model.RawRecordsDto}).
 * After the target file gets split into records, ParallelFileChunkingProcessor sends records to the mod-source-record-manager
 * for further processing.
 */
public class ParallelFileChunkingProcessor implements FileProcessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParallelFileChunkingProcessor.class);
  private static final int THREAD_POOL_SIZE =
    Integer.parseInt(MODULE_SPECIFIC_ARGS.getOrDefault("file.processing.thread.pool.size", "20"));
  private static final int BLOCKING_COORDINATOR_FILES_NUMBER =
    Integer.parseInt(MODULE_SPECIFIC_ARGS.getOrDefault("file.processing.blocking.coordinator.parallel.files.number", "1"));
  private static final int BLOCKING_COORDINATOR_CHUNKS_NUMBER =
    Integer.parseInt(MODULE_SPECIFIC_ARGS.getOrDefault("file.processing.blocking.coordinator.parallel.chunks.number", "10"));

  private Vertx vertx;
  /* WorkerExecutor provides separate worker pool for code execution */
  private WorkerExecutor executor;

  public ParallelFileChunkingProcessor() {
  }

  public ParallelFileChunkingProcessor(Vertx vertx) {
    this.vertx = vertx;
    this.executor = this.vertx.createSharedWorkerExecutor("processing-files-thread-pool", THREAD_POOL_SIZE);
  }

  @Override
  public void process(JsonObject jsonRequest, JsonObject jsonParams) { //NOSONAR
    succeededFuture().setHandler(v -> {
      ProcessFilesRqDto request = jsonRequest.mapTo(ProcessFilesRqDto.class);
      UploadDefinition uploadDefinition = request.getUploadDefinition();
      JobProfileInfo jobProfile = request.getJobProfileInfo();
      OkapiConnectionParams params = new OkapiConnectionParams(jsonParams.mapTo(HashMap.class), this.vertx);
      UploadDefinitionService uploadDefinitionService = new UploadDefinitionServiceImpl(vertx);
      succeededFuture()
        .compose(ar -> uploadDefinitionService.getJobExecutions(uploadDefinition, params))
        .compose(jobExecutions -> updateJobsProfile(jobExecutions, jobProfile, params))
        .compose(ar -> FileStorageServiceBuilder.build(this.vertx, params))
        .compose(fileStorageService -> {
          processFiles(jobProfile, uploadDefinitionService, fileStorageService, uploadDefinition, params);
          uploadDefinitionService.updateBlocking(
            uploadDefinition.getId(),
            definition -> succeededFuture(definition.withStatus(UploadDefinition.Status.COMPLETED)),
            params.getTenantId());
          return succeededFuture();
        });
    });
  }

  /**
   * Performs processing files from given UploadDefinition
   *
   * @param jobProfile              job profile comes from request, needed to build SourceReader
   * @param uploadDefinitionService upload definition service needed to update job execution status
   * @param fileStorageService      file storage service needed to read file
   * @param uploadDefinition        upload definition entity comes from request
   * @param params                  Okapi connection params
   */
  private void processFiles(JobProfileInfo jobProfile,
                            UploadDefinitionService uploadDefinitionService,
                            FileStorageService fileStorageService,
                            UploadDefinition uploadDefinition,
                            OkapiConnectionParams params) {
    this.executor.executeBlocking(filesBlockingFuture -> {
      BlockingCoordinator blockingCoordinator = new QueuedBlockingCoordinator(BLOCKING_COORDINATOR_FILES_NUMBER);
      List<FileDefinition> fileDefinitions = new UnmodifiableList<>(uploadDefinition.getFileDefinitions());
      for (FileDefinition fileDefinition : fileDefinitions) {
        blockingCoordinator.acceptLock();
        this.executor.executeBlocking(fileBlockingFuture -> processFile(fileDefinition, jobProfile, fileStorageService, params).setHandler(ar -> {
            if (ar.failed()) {
              LOGGER.error("Can not process file {}. Cause: {}", fileDefinition.getSourcePath(), ar.cause());
              uploadDefinitionService.updateJobExecutionStatus(
                fileDefinition.getJobExecutionId(),
                new StatusDto().withStatus(ERROR).withErrorStatus(FILE_PROCESSING_ERROR),
                params);
            } else {
              LOGGER.info("File {} successfully processed.", fileDefinition.getSourcePath());
            }
            blockingCoordinator.acceptUnlock();
          }),
          false, null
        );
      }
    }, null);
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
  protected Future<Void> processFile(FileDefinition fileDefinition,
                                     JobProfileInfo jobProfile,
                                     FileStorageService fileStorageService,
                                     OkapiConnectionParams params) {
    Future<Void> resultFuture = Future.future();
    MutableInt recordsCounter = new MutableInt(0);
    BlockingCoordinator coordinator = new QueuedBlockingCoordinator(BLOCKING_COORDINATOR_CHUNKS_NUMBER);
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
      int totalRecords = countTotalRecordsInFile(file, jobProfile);
      while (reader.hasNext()) {
        if (canSendNextChunk.get()) {
          coordinator.acceptLock();
          List<String> records = reader.next();
          recordsCounter.add(records.size());
          RawRecordsDto chunk = new RawRecordsDto()
            .withRecords(records)
            .withRecordsMetadata(new RecordsMetadata()
              .withContentType(reader.getContentType())
              .withCounter(recordsCounter.getValue())
              .withLast(false)
              .withTotal(totalRecords));
          chunkSentFutures.add(postRawRecords(fileDefinition.getJobExecutionId(), chunk, canSendNextChunk, coordinator, params));
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
          RawRecordsDto chunk = new RawRecordsDto()
            .withRecordsMetadata(new RecordsMetadata()
              .withContentType(reader.getContentType())
              .withCounter(recordsCounter.getValue())
              .withLast(true)
              .withTotal(totalRecords));
          postRawRecords(fileDefinition.getJobExecutionId(), chunk, canSendNextChunk, coordinator, params).setHandler(r -> {
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
      LOGGER.error(errorMessage, e);
      resultFuture.fail(errorMessage);
    }
    return resultFuture;
  }

  /**
   * Read file and count total records it is contains
   *
   * @param file       - file with records
   * @param jobProfile - job profile main info
   * @return total records in file;
   */
  private int countTotalRecordsInFile(File file, JobProfileInfo jobProfile) {
    int total = 0;
    if (file == null || jobProfile == null) {
      return total;
    }
    SourceReader reader = SourceReaderBuilder.build(file, jobProfile);
    while (reader.hasNext()) {
      total += reader.next().size();
    }
    return total;
  }

  /**
   * Sends chunk with records to the corresponding consumer
   *
   * @param jobExecutionId   job id
   * @param chunk            chunk of records
   * @param canSendNextChunk flag the identifies has the last record been successfully sent and can the other handlers
   *                         send raw records (chunks)
   * @param coordinator      blocking coordinator
   * @param params           parameters necessary for connection to the OKAPI
   * @return Future
   */
  private Future<Void> postRawRecords(String jobExecutionId, RawRecordsDto chunk, AtomicBoolean canSendNextChunk,
                                      BlockingCoordinator coordinator, OkapiConnectionParams params) {
    Future<Void> future = Future.future();
    ChangeManagerClient client = new ChangeManagerClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.postChangeManagerJobExecutionsRecordsById(jobExecutionId, chunk, response -> {
        coordinator.acceptUnlock();
        if (response.statusCode() == HttpStatus.HTTP_NO_CONTENT.toInt()) {
          LOGGER.info("Chunk of records with size {} was successfully posted for JobExecution {}", chunk.getRecords().size(), jobExecutionId);
          future.complete();
        } else {
          canSendNextChunk.set(false);
          LOGGER.error("Error posting chunk of raw records for JobExecution with id {}", jobExecutionId, response.statusMessage());
          future.fail(new HttpStatusException(response.statusCode(), "Error posting chunk of raw records"));
        }
      });
    } catch (Exception e) {
      coordinator.acceptUnlock();
      canSendNextChunk.set(false);
      LOGGER.error("Can not post chunk of raw records for JobExecution with id {}", jobExecutionId, e);
      future.fail(e);
    }
    return future;
  }

  /**
   * Updates JobExecutions with given JobProfile value
   *
   * @param jobs       jobs to update
   * @param jobProfile JobProfile entity
   * @param params     parameters necessary for connection to the OKAPI
   * @return Future
   */
  private Future<Void> updateJobsProfile(List<JobExecution> jobs, JobProfileInfo jobProfile, OkapiConnectionParams params) {
    Future<Void> future = Future.future();
    List<Future> updateJobProfileFutures = new ArrayList<>(jobs.size());
    for (JobExecution job : jobs) {
      updateJobProfileFutures.add(updateJobProfile(job.getId(), jobProfile, params));
    }
    CompositeFuture.all(updateJobProfileFutures).setHandler(updatedJobsProfileAr -> {
      if (updatedJobsProfileAr.failed()) {
        future.fail(updatedJobsProfileAr.cause());
      } else {
        LOGGER.info("All the child jobs have been updated by job profile, parent job {}", jobs.get(0).getParentJobId());
        future.complete();
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
  private Future<Void> updateJobProfile(String jobId, JobProfileInfo jobProfile, OkapiConnectionParams params) {
    Future<Void> future = Future.future();
    ChangeManagerClient client = new ChangeManagerClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.putChangeManagerJobExecutionsJobProfileById(jobId, jobProfile, response -> {
        if (response.statusCode() != HttpStatus.HTTP_OK.toInt()) {
          LOGGER.error("Error updating job profile for JobExecution {}", jobId);
          future.fail(new HttpStatusException(response.statusCode(), "Error updating JobExecution"));
        } else {
          LOGGER.info("Job profile for job {} successfully updated.", jobId);
          future.complete();
        }
      });
    } catch (Exception e) {
      LOGGER.error("Couldn't update jobProfile for JobExecution with id {}", jobId, e);
      future.fail(e);
    }
    return future;
  }
}
