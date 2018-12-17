package org.folio.service.chunking;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionProfile;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.service.storage.FileStorageService;
import org.folio.service.storage.FileStorageServiceBuilder;
import org.folio.util.OkapiConnectionParams;
import org.folio.util.RestUtil;

import java.util.ArrayList;
import java.util.List;

import static org.folio.util.RestUtil.CREATED_STATUS_CODE;

/**
 * Implementation of the FileChunkingHandler.
 * Runs file dividing into background process.
 * Sends result chunks to the mod-source-record-manager.
 */
public class FileBlockingChunkingHandlerImpl implements FileChunkingHandler {

  public static final String UPDATE_JOBEXECUTION_SERVICE_URL = "/change-manager/jobExecution/";
  private static final Logger logger = LoggerFactory.getLogger(FileBlockingChunkingHandlerImpl.class);

  private Vertx vertx;
  private String tenantId;

  public FileBlockingChunkingHandlerImpl(Vertx vertx, String tenantId) {
    this.vertx = vertx;
    this.tenantId = tenantId;
  }

  @Override
  public Future handle(UploadDefinition uploadDefinition, JobExecutionProfile jobExecutionProfile, OkapiConnectionParams params) {
    Future future = Future.future();
    updateJobExecutionProfile(uploadDefinition.getMetaJobExecutionId(), jobExecutionProfile, params).setHandler(updatedProfileAsyncResult -> {
      if (updatedProfileAsyncResult.failed()) {
        logger.error("Can't update JobExecutionProfile having MetaJobExecutionId: " + uploadDefinition.getMetaJobExecutionId());
        future.fail(updatedProfileAsyncResult.cause());
      } else {
        logger.info("JobExecutionProfile for MetaJobExecutionId: " + uploadDefinition.getMetaJobExecutionId() + " successfully updated");
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
    logger.info("Building FileStorageService");
    FileStorageServiceBuilder.build(this.vertx, this.tenantId, params).setHandler(fileStorageServiceAsyncResult -> {
      if (fileStorageServiceAsyncResult.failed()) {
        logger.error("Can't build FileStorageService. Cause: " + fileStorageServiceAsyncResult.cause());
      } else {
        FileStorageService fileStorageService = fileStorageServiceAsyncResult.result();
        logger.info("Running blocking chunking process");
        vertx.executeBlocking(
          blockingFuture -> {
            List<Future> fileHandlingFutures = new ArrayList<>(uploadDefinition.getFileDefinitions().size());
            int totalChunksCounter = 0;
            logger.info("Staring the loop iterating through FileDefinitions");
            for (FileDefinition fileDefinition : uploadDefinition.getFileDefinitions()) {
              fileStorageService.getFile(fileDefinition.getSourcePath()).setHandler(bufferAsyncResult -> {
                if (bufferAsyncResult.failed()) {
                  blockingFuture.fail("Can't obtain file from the source path: " + fileDefinition.getSourcePath() + ". Cause: " + bufferAsyncResult.cause());
                } else {
                  logger.info("File from the source path: " + fileDefinition.getSourcePath() + "has been obtained");
                  Buffer fileBuffer = bufferAsyncResult.result();
                  Future handleFileChunkingFuture = handleFileChunking(fileBuffer, fileDefinition, totalChunksCounter, params);
                  fileHandlingFutures.add(handleFileChunkingFuture);
                }
              });
            }
            CompositeFuture.all(fileHandlingFutures).setHandler(compositeFileHandlingFuture -> {
              if (compositeFileHandlingFuture.failed()) {
                logger.error(
                  "Error while handling files for given UploadDefinition. UploadDefinition id: " + uploadDefinition.getId(),
                  "Cause: " + compositeFileHandlingFuture.cause()
                );
                blockingFuture.fail(compositeFileHandlingFuture.cause());
              } else {
                logger.info("Files have been successfully handled. UploadDefinition id: " + uploadDefinition.getId());
                // TODO notify mod-source-records-manager with totalChunkCounter
                blockingFuture.complete();
              }
            });
          },
          blockingAsyncResult -> {
            if (blockingAsyncResult.failed()) {
              logger.error("Error while executing blocking process: " + blockingAsyncResult.cause());
            } else {
              logger.info("Blocking process for handling UploadDefinition has been successfully complete.",
                "UploadDefinition id: " + uploadDefinition.getId()
              );
            }
          }
        );
      }
    });
  }

  /**
   * Divides files linked to UploadDefinition entity and sends data to the mod-source-record-manager
   *
   * @param buffer             file content
   * @param fileDefinition     contains metadata that describes stored file
   * @param totalChunksCounter counter of the total amount of chunks
   * @param params             parameters necessary for connection to the OKAPI
   * @return Future
   */
  private Future handleFileChunking(Buffer buffer, FileDefinition fileDefinition, int totalChunksCounter, OkapiConnectionParams params) {
    Future resultFuture = Future.future();
    List<Future> chunkHandlingFutures = new ArrayList<>();
    logger.info("Dividing file with source path: " + fileDefinition.getSourcePath() + "into chunks of data");
    /*
      TODO before file processing update status for JobExecution to IMPORT_IN_PROGRESS
      TODO divide the buffer into chunks using default placeholder and send chunk to the mod-source-record-manager
      TODO increment totalChunksCounter per each chunk of data
    */
    CompositeFuture.all(chunkHandlingFutures).setHandler(compositeFutureAsyncResult -> {
      if (compositeFutureAsyncResult.failed()) {

      } else {
        /*
          FIXME set IMPORT_FINISHED status for the JobExecution
        */
        resultFuture.complete();
      }
    });
    return resultFuture;
  }

  /**
   * Updates JobExecutionProfile for all JobExecutions which have parentId equal to metaJobExecutionId
   *
   * @param metaJobExecutionId parent JobExecution id
   * @param profile            JobExecutionProfile entity
   * @param params             parameters necessary for connection to the OKAPI
   * @return Future
   */
  private Future<Boolean> updateJobExecutionProfile(String metaJobExecutionId, JobExecutionProfile profile, OkapiConnectionParams params) {
    Future<Boolean> future = Future.future();

    JobExecution parentJobExecution = new JobExecution()
      .withId(metaJobExecutionId)
      // TODO set Profile object that contains all the necessary fields
      .withJobProfileName(profile.getName());

    RestUtil.doRequest(params, UPDATE_JOBEXECUTION_SERVICE_URL + parentJobExecution.getId(), HttpMethod.PUT, parentJobExecution)
      .setHandler(responseResult -> {
        try {
          if (responseResult.failed() || responseResult.result() == null || responseResult.result().getCode() != CREATED_STATUS_CODE) {
            logger.error("Error while updating JobExecution.", responseResult.cause());
            future.fail(responseResult.cause());
          } else {
            future.complete(true);
          }
        } catch (Exception e) {
          logger.error("Error while updating JobExecution.", e, e.getMessage());
          future.fail(e);
        }
      });
    return future;
  }

  /**
   * Updates status for the child JobExecution with given status value
   *
   * @param parentJobExecutionId parent job id which child job will be affected by update
   * @param status               new status value
   * @param params               parameters necessary for connection to the OKAPI
   * @return Future
   */
  private Future updateChildJobExecutionsStatus(String parentJobExecutionId, JobExecution.Status status, OkapiConnectionParams params) {
    // TODO implement me
    return Future.succeededFuture();
  }
}
