package org.folio.service.chunking;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.http.HttpStatus;
import org.folio.dataImport.util.OkapiConnectionParams;
import org.folio.dataImport.util.RestUtil;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.Profile;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.UploadDefinition;

import java.util.ArrayList;
import java.util.List;

import static org.folio.rest.jaxrs.model.JobExecution.Status.IMPORT_FINISHED;
import static org.folio.rest.jaxrs.model.JobExecution.Status.IMPORT_IN_PROGRESS;

/**
 * Implementation of the FileChunkingHandler.
 * Runs file dividing into background process.
 * Sends result chunks to the mod-source-record-manager.
 */
public class FileBlockingChunkingHandlerImpl implements FileChunkingHandler {

  private static final Logger logger = LoggerFactory.getLogger(FileBlockingChunkingHandlerImpl.class);

  private static final String UPDATE_JOB_STATUS_SERVICE_URL = "/change-manager/jobExecution/%s/status";
  private static final String UPDATE_JOB_PROFILE_SERVICE_URL = "/change-manager/jobExecution/%s";
  private static final String POST_RAW_RECORDS_PATH = "/change-manager/records/$s";
  private final String marcRecordPath = "src/main/resources/sample/records/marcRecord.sample";

  private Vertx vertx;

  public FileBlockingChunkingHandlerImpl(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public Future<UploadDefinition> handle(UploadDefinition uploadDefinition, Profile profile, OkapiConnectionParams params) {
    Future<UploadDefinition> future = Future.future();
    updateJobExecutionProfile(uploadDefinition.getMetaJobExecutionId(), profile, params).setHandler(updatedProfileAsyncResult -> {
      if (updatedProfileAsyncResult.failed()) {
        logger.error(String.format("Can not update JobProfile with id: %s having MetaJobExecution id: %s",
          profile.getId(),
          uploadDefinition.getMetaJobExecutionId())
        );
        future.fail(updatedProfileAsyncResult.cause());
      } else {
        runBlockingChunkingProcess(uploadDefinition, params);
        future.complete();
      }
    });
    return future;
  }

  /**
   * Runs background blocking process for handling files linked to the one UploadDefinition.
   *
   * @param uploadDefinition target UploadDefinition entity with files for handling
   * @param params           parameters necessary to connect to the OKAPI
   */
  private void runBlockingChunkingProcess(UploadDefinition uploadDefinition, OkapiConnectionParams params) {
    logger.info("Running blocking process for UploadDefinition with id: " + uploadDefinition.getId());
    vertx.executeBlocking(
      blockingFuture -> {
        updateStatusForJobsOfUploadDefinition(uploadDefinition, IMPORT_IN_PROGRESS, params)
          .compose(ar -> handleFileDefinitions(uploadDefinition.getFileDefinitions(), params))
          .compose(ar -> postRawRecords(uploadDefinition.getMetaJobExecutionId(), new RawRecordsDto().withLast(true), params))
          .compose(ar -> updateStatusForJobsOfUploadDefinition(uploadDefinition, IMPORT_FINISHED, params))
          .setHandler(ar -> {
            if (ar.failed()) {
              blockingFuture.fail(ar.cause());
            } else {
              blockingFuture.complete();
            }
          });
      },
      asyncResult -> {
        if (asyncResult.failed()) {
          String errorMessage = String.format("Error while executing blocking process for UploadDefinition with id: %s. Cause: %s",
            uploadDefinition.getId(), asyncResult.cause()
          );
          logger.error(errorMessage);
        } else {
          String infoMessage = String.format("Blocking process for UploadDefinition with id: %s has been successfully complete.", uploadDefinition.getId());
          logger.info(infoMessage);
        }
      }
    );
  }


  /**
   * Updates status for the jobs related to received upload definition
   * TODO for the sake of simplicity let's stay with updating all the jobs at one time
   * TODO this functionality may be affected further
   *
   * @param uploadDefinition target upload definition entity
   * @param status           job status value
   * @param params           connection params enough to connect to OKAPI
   * @return Future
   */
  private Future<Void> updateStatusForJobsOfUploadDefinition(UploadDefinition uploadDefinition, JobExecution.Status status, OkapiConnectionParams params) {
    Future<Void> resultFuture = Future.future();
    List<Future> updatedJobStatusFutures = new ArrayList<>(uploadDefinition.getFileDefinitions().size());
    for (FileDefinition fileDefinition : uploadDefinition.getFileDefinitions()) {
      Future updateJobStatusFuture =
        updateJobExecutionStatus(fileDefinition.getJobExecutionId(), status, params);
      updatedJobStatusFutures.add(updateJobStatusFuture);
    }
    CompositeFuture.all(updatedJobStatusFutures).setHandler(compositeFutureAr -> {
      if (compositeFutureAr.failed()) {
        resultFuture.fail(compositeFutureAr.cause());
      } else {
        resultFuture.complete();
      }
    });
    return resultFuture;
  }

  /**
   * Performs file upload and file parsing, sends chunks of data to the dedicated consumer
   *
   * @param fileDefinitions file definition entities which files will be handled
   * @param params          parameters necessary to connect to the OKAPI
   * @return Future
   */
  private Future<Void> handleFileDefinitions(List<FileDefinition> fileDefinitions, OkapiConnectionParams params) {
    Future<Void> chunkHandlingFuture = Future.future();
    // This is stub implementation, will be overridden in a scope of MODDATAIMP-45
    // For now let's upload and parse only one sample file for the first FileDefinition
    FileDefinition fileDefinition = fileDefinitions.get(0);
    this.vertx.fileSystem().readFile(marcRecordPath, bufferAsyncResult -> {
      if (bufferAsyncResult.failed()) {
        logger.error("Can not read file from the file system by the path: " + marcRecordPath);
        chunkHandlingFuture.fail(bufferAsyncResult.cause());
      } else {
        Buffer buffer = bufferAsyncResult.result();
        List<String> records = new ArrayList<>();
        records.add(buffer.toString());
        RawRecordsDto chunk = new RawRecordsDto()
          .withRecords(records)
          .withLast(false)
          .withTotal(records.size());
        postRawRecords(fileDefinition.getJobExecutionId(), chunk, params).setHandler(postRawRecordsAr -> {
          if (postRawRecordsAr.failed()) {
            chunkHandlingFuture.fail(postRawRecordsAr.cause());
          } else {
            chunkHandlingFuture.complete();
          }
        });
      }
    });
    return chunkHandlingFuture;
  }

  /**
   * Sends given chunk to the dedicated consumer
   *
   * @param jobExecutionId job id
   * @param chunk          chunk of file data with raw records
   * @param params         parameters necessary to connect to the OKAPI
   * @return Future
   */
  private Future<Void> postRawRecords(String jobExecutionId, RawRecordsDto chunk, OkapiConnectionParams params) {
    Future<Void> future = Future.future();
    RestUtil.doRequest(params, String.format(POST_RAW_RECORDS_PATH, jobExecutionId), HttpMethod.POST, chunk)
      .setHandler(responseResult -> {
        if (responseResult.failed()
          || responseResult.result() == null
          || responseResult.result().getCode() != HttpStatus.SC_NO_CONTENT) {
          logger.error("Can not post raw records for JobExecution with id: " + jobExecutionId);
          future.fail(responseResult.cause());
        } else {
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
  private Future<Boolean> updateJobExecutionProfile(String metaJobExecutionId, Profile jobProfile, OkapiConnectionParams params) {
    Future<Boolean> future = Future.future();
    JobExecution parentJobExecution = new JobExecution()
      .withId(metaJobExecutionId)
      .withProfile(jobProfile);
    RestUtil.doRequest(params, String.format(UPDATE_JOB_PROFILE_SERVICE_URL, metaJobExecutionId), HttpMethod.PUT, parentJobExecution)
      .setHandler(responseResult -> {
        if (responseResult.failed() || responseResult.result() == null || responseResult.result().getCode() != HttpStatus.SC_OK) {
          logger.error("Error while updating JobProfile with id: %s for JobExecution with id:", jobProfile.getId(), metaJobExecutionId);
          future.fail(responseResult.cause());
        } else {
          future.complete(true);
        }
      });
    return future;
  }

  /**
   * Updates status for JobExecution with given status value
   *
   * @param jobExecutionId job id
   * @param status         new status value
   * @param params         parameters necessary for connection to the OKAPI
   * @return Future
   */
  private Future<Boolean> updateJobExecutionStatus(String jobExecutionId, JobExecution.Status status, OkapiConnectionParams params) {
    Future<Boolean> future = Future.future();
    StatusDto statusDto = new StatusDto().withStatus(StatusDto.Status.valueOf(status.value()));
    RestUtil.doRequest(params, String.format(UPDATE_JOB_STATUS_SERVICE_URL, jobExecutionId), HttpMethod.PUT, statusDto)
      .setHandler(responseResult -> {
        if (responseResult.failed() || responseResult.result() == null || responseResult.result().getCode() != HttpStatus.SC_OK) {
          logger.error("Error while updating status for JobExecution with id %s: ", jobExecutionId);
          future.fail(responseResult.cause());
        } else {
          future.complete(true);
        }
      });
    return future;
  }
}
