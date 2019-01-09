package org.folio.service.processing;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.http.HttpStatus;
import org.folio.dataImport.util.OkapiConnectionParams;
import org.folio.dataImport.util.RestUtil;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollection;
import org.folio.rest.jaxrs.model.JobProfile;
import org.folio.rest.jaxrs.model.ProcessChunkingRqDto;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.service.processing.reader.FileSystemStubSourceReader;
import org.folio.service.processing.reader.SourceReader;
import org.folio.service.storage.FileStorageService;
import org.folio.service.storage.FileStorageServiceBuilder;
import org.folio.service.upload.UploadDefinitionService;
import org.folio.service.upload.UploadDefinitionServiceImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.folio.dataImport.util.RestUtil.validateAsyncResult;
import static org.folio.rest.RestVerticle.MODULE_SPECIFIC_ARGS;
import static org.folio.rest.jaxrs.model.StatusDto.Status.IMPORT_FINISHED;
import static org.folio.rest.jaxrs.model.StatusDto.Status.IMPORT_IN_PROGRESS;

/**
 * Processing files in parallel threads. One thread per one file.
 * File chunking process implies reading and splitting the file into chunks of data.
 * Every chunk represents collection of source records, see ({@link org.folio.rest.jaxrs.model.RawRecordsDto}).
 * After splitting the file into records ParallelFileChunkingProcessor sends records to the mod-source-record-manager
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
    ProcessChunkingRqDto request = jsonRequest.mapTo(ProcessChunkingRqDto.class);
    UploadDefinition uploadDefinition = request.getUploadDefinition();
    JobProfile jobProfile = request.getJobProfile();
    OkapiConnectionParams params = jsonParams.mapTo(OkapiConnectionParams.class);
    String tenantId = params.getTenantId();
    UploadDefinitionService uploadDefinitionService = new UploadDefinitionServiceImpl(vertx, tenantId);
    FileStorageServiceBuilder.build(this.vertx, tenantId, params).setHandler(fileStorageServiceAr -> {
      if (fileStorageServiceAr.failed()) {
        LOGGER.error("Can not build file storage service. Cause: " + fileStorageServiceAr.cause());
      } else {
        FileStorageService fileStorageService = fileStorageServiceAr.result();
        List<FileDefinition> fileDefinitions = uploadDefinition.getFileDefinitions();
        updateJobsProfile(uploadDefinition.getMetaJobExecutionId(), jobProfile, params).setHandler(updatedProfileAsyncResult -> {
          if (updatedProfileAsyncResult.failed()) {
            LOGGER.error("Can not update profile for jobs. Cause: " + updatedProfileAsyncResult.cause());
          } else {
            List<Future> fileBlockingFutures = new ArrayList<>(fileDefinitions.size());
            for (FileDefinition fileDefinition : fileDefinitions) {
              this.executor.executeBlocking(blockingFuture -> {
                  fileBlockingFutures.add(blockingFuture);
                  uploadDefinitionService
                    .updateJobExecutionStatus(fileDefinition.getJobExecutionId(), new StatusDto().withStatus(IMPORT_IN_PROGRESS), params)
                    .compose(ar -> processFile(fileDefinition, fileStorageService, params))
                    .compose(ar -> uploadDefinitionService.updateJobExecutionStatus(fileDefinition.getJobExecutionId(), new StatusDto().withStatus(IMPORT_FINISHED), params))
                    .setHandler(ar -> {
                      if (ar.failed()) {
                        LOGGER.error("Can not process file " + fileDefinition.getSourcePath() + ". Cause: " + ar.cause());
                        blockingFuture.fail(ar.cause());
                      } else {
                        LOGGER.info("File " + fileDefinition.getSourcePath() + " successfully processed.");
                        blockingFuture.complete();
                      }
                    });
                }, null
              );
            }
            CompositeFuture.all(fileBlockingFutures).setHandler(ar -> {
              if (ar.failed()) {
                LOGGER.error("Error while processing file of upload definition " + uploadDefinition.getId() + ". Cause: " + ar.cause());
              } else {
                LOGGER.info("All the file of upload definition " + uploadDefinition.getId() + "are successfully processed.");
              }
            });
          }
        });
      }
    });
  }

  /**
   * Processing file
   *
   * @param fileDefinition     fileDefinition entity
   * @param fileStorageService service to obtain file
   * @param params             parameters necessary for connection to the OKAPI
   * @return Future
   */
  private Future<Void> processFile(FileDefinition fileDefinition, FileStorageService
    fileStorageService, OkapiConnectionParams params) { // NOSONAR
    Future<Void> future = Future.future();
    SourceReader reader = new FileSystemStubSourceReader(this.vertx.fileSystem());
    while (reader.hasNext()) {
      reader.readNext().setHandler(readRecordsAr -> {
        if (readRecordsAr.failed()) {
          LOGGER.error("Can not read next chunk of records for the file " + fileDefinition.getSourcePath());
          future.fail(readRecordsAr.cause());
        } else {
          RawRecordsDto chunk = readRecordsAr.result();
          postRawRecords(fileDefinition.getJobExecutionId(), chunk, params).setHandler(postedRecordsAr -> {
            if (postedRecordsAr.failed()) {
              future.fail(postedRecordsAr.cause());
            } else {
              if (!reader.hasNext()) {
                LOGGER.info("All the chunks for file " + fileDefinition.getSourcePath() + " successfully sent.");
                future.complete();
              }
            }
          });
        }
      });
    }
    return future;
  }

  /**
   * Sends chunk with records to the corresponding consumer
   *
   * @param jobExecutionId job id
   * @param chunk          chunk of records
   * @param params         parameters necessary for connection to the OKAPI
   * @return Future
   */
  private Future<Void> postRawRecords(String jobExecutionId, RawRecordsDto chunk, OkapiConnectionParams params) {
    Future<Void> future = Future.future();
    RestUtil.doRequest(params, RAW_RECORDS_SERVICE_URL + jobExecutionId, HttpMethod.POST, chunk)
      .setHandler(responseResult -> {
        if (responseResult.failed()
          || responseResult.result() == null
          || responseResult.result().getCode() != HttpStatus.SC_NO_CONTENT) {
          LOGGER.error("Can not post raw records for job " + jobExecutionId);
          future.fail(responseResult.cause());
        } else {
          LOGGER.info("Chunk of records with size " + chunk.getRecords().size() + " successfully posted for job " + jobExecutionId);
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
            LOGGER.info("All the child jobs have been updated by job profile, parent job " + metaJobExecutionId);
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
        LOGGER.error("Can not update job profile for job " + jobId);
        future.fail(ar.cause());
      } else {
        LOGGER.info("Job profile for job " + jobId + " successfully updated.");
        future.complete();
      }
    });
    return future;
  }
}
