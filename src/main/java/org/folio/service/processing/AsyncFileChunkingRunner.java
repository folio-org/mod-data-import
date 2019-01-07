package org.folio.service.processing;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dataImport.util.OkapiConnectionParams;
import org.folio.dataImport.util.RestUtil;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollection;
import org.folio.rest.jaxrs.model.JobProfile;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.service.upload.UploadDefinitionService;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.folio.dataImport.util.RestUtil.validateAsyncResult;

/**
 * Runs file chunking process asynchronously in a separate background thread.
 * File chunking process implies reading and dividing the file into chunks of data (see {@link FileProcessor}).
 * Further, the result source records can be handled in their own way.
 * Every chunk represents collection of source records, see ({@link org.folio.rest.jaxrs.model.RawRecordsDto}).
 */
public class AsyncFileChunkingRunner implements FileProcessingRunner {

  private static final Logger logger = LoggerFactory.getLogger(AsyncFileChunkingRunner.class);

  private static final String JOB_CHILDREN_SERVICE_URL = "/change-manager/jobExecution/%s/children";
  private static final String JOB_PROFILE_SERVICE_URL = "/change-manager/jobExecution/%s/jobProfile";

  private Vertx vertx;
  private FileProcessor fileProcessor;

  public AsyncFileChunkingRunner() {
  }

  public AsyncFileChunkingRunner(Vertx vertx, String tenantId, UploadDefinitionService uploadDefinitionService) {
    this.vertx = vertx;
    this.fileProcessor = new ParallelFileChunkingProcessor(vertx, tenantId, uploadDefinitionService);
  }

  @Override
  public Future<Void> run(UploadDefinition uploadDefinition, JobProfile jobProfile, OkapiConnectionParams params) {
    Future<Void> future = Future.future();
    updateJobsProfile(uploadDefinition.getMetaJobExecutionId(), jobProfile, params).setHandler(updatedProfileAsyncResult -> {
      if (updatedProfileAsyncResult.failed()) {
        future.fail(updatedProfileAsyncResult.cause());
      } else {
        vertx.executeBlocking(
          asyncFuture -> fileProcessor.process(uploadDefinition, params),
          asyncResult -> {
            if (asyncResult.failed()) {
              String errorMessage =
                String.format("Error while executing blocking process for upload definition with id: %s. Cause: %s",
                  uploadDefinition.getId(), asyncResult.cause()
                );
              logger.error(errorMessage);
            } else {
              String infoMessage =
                String.format("Blocking process for upload definition with id: %s successfully completed.",
                  uploadDefinition.getId());
              logger.info(infoMessage);
            }
          });
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
  private Future<Boolean> updateJobsProfile(String metaJobExecutionId, JobProfile jobProfile, OkapiConnectionParams params) {
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
            logger.info("All the child jobs have been updated by job profile, parent job id: " + metaJobExecutionId);
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
    RestUtil.doRequest(params, String.format(JOB_PROFILE_SERVICE_URL, jobId), HttpMethod.PUT, jobProfile).setHandler(childrenJobsAr -> {
      if (validateAsyncResult(childrenJobsAr, future)) {
        future.complete();
      }
    });
    return future;
  }
}
