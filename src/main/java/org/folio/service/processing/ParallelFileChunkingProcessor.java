package org.folio.service.processing;

import org.folio.okapi.common.GenericCompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.vertx.ext.web.handler.impl.HttpStatusException;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.apache.commons.collections4.list.UnmodifiableList;
import org.folio.HttpStatus;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.rest.client.ChangeManagerClient;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.ProcessFilesRqDto;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.service.processing.kafka.SourceReaderReadStreamWrapper;
import org.folio.service.processing.kafka.WriteStreamWrapper;
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

import static io.vertx.core.Future.succeededFuture;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_MARC_BIB_RECORDS_CHUNK_READ;
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

  private static final Logger LOGGER = LogManager.getLogger();

  private Vertx vertx;

  private KafkaConfig kafkaConfig;

  public ParallelFileChunkingProcessor() {
  }

  public ParallelFileChunkingProcessor(Vertx vertx, KafkaConfig kafkaConfig) {
    this.vertx = vertx;
    this.kafkaConfig = kafkaConfig;
  }

  @Override
  public void process(JsonObject jsonRequest, JsonObject jsonParams) { //NOSONAR
    ProcessFilesRqDto request = jsonRequest.mapTo(ProcessFilesRqDto.class);
    UploadDefinition uploadDefinition = request.getUploadDefinition();
    JobProfileInfo jobProfile = request.getJobProfileInfo();
    OkapiConnectionParams params = new OkapiConnectionParams(jsonParams.mapTo(HashMap.class), this.vertx);
    UploadDefinitionService uploadDefinitionService = new UploadDefinitionServiceImpl(vertx);
    succeededFuture()
      .compose(ar -> uploadDefinitionService.getJobExecutions(uploadDefinition, params))
      .compose(jobExecutions -> updateJobsProfile(jobExecutions, jobProfile, params))
      .compose(ar -> FileStorageServiceBuilder.build(this.vertx, params.getTenantId(), params))
      .compose(fileStorageService -> {
        processFiles(jobProfile, uploadDefinitionService, fileStorageService, uploadDefinition, params);
        uploadDefinitionService.updateBlocking(
          uploadDefinition.getId(),
          definition -> succeededFuture(definition.withStatus(UploadDefinition.Status.COMPLETED)),
          params.getTenantId());
        return succeededFuture();
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

    List<FileDefinition> fileDefinitions = new UnmodifiableList<>(uploadDefinition.getFileDefinitions());
    for (FileDefinition fileDefinition : fileDefinitions) {
      vertx.runOnContext(v ->
        processFile(fileDefinition, jobProfile, fileStorageService, params).onComplete(par -> {
            if (par.failed()) {
              LOGGER.error("File was processed with errors {}. Cause: {}", fileDefinition.getSourcePath(), par.cause());
              uploadDefinitionService.updateJobExecutionStatus(
                fileDefinition.getJobExecutionId(),
                new StatusDto().withStatus(ERROR).withErrorStatus(FILE_PROCESSING_ERROR),
                params);
            } else {
              LOGGER.info("File {} successfully processed.", fileDefinition.getSourcePath());
            }
          }
        ));
    }
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

    String topicName = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(),
      KafkaTopicNameHelper.getDefaultNameSpace(), params.getTenantId(),
      DI_RAW_MARC_BIB_RECORDS_CHUNK_READ.value());

    File file = fileStorageService.getFile(fileDefinition.getSourcePath());
    SourceReader reader = SourceReaderBuilder.build(file, jobProfile);

    int totalRecords = countTotalRecordsInFile(file, jobProfile);

    SourceReaderReadStreamWrapper readStreamWrapper = new SourceReaderReadStreamWrapper(
      vertx, reader, fileDefinition.getJobExecutionId(), totalRecords,
      params, 100, topicName);
    readStreamWrapper.pause();

    Promise<Void> processFilePromise = Promise.promise();
    LOGGER.debug("About to start piping to KafkaProducer... jobProfile: {}", jobProfile);
    KafkaProducer<String, String> producer = KafkaProducer.createShared(vertx,
      DI_RAW_MARC_BIB_RECORDS_CHUNK_READ + "_Producer", kafkaConfig.getProducerProps());
    readStreamWrapper.pipeTo(new WriteStreamWrapper(producer), ar -> {
      boolean succeeded = ar.succeeded();
      LOGGER.debug("Data piping has been completed. ar.succeeded(): {} jobProfile: {}",succeeded, jobProfile);
      LOGGER.debug("Closing KafkaProducer jobProfile: {}", jobProfile);
      producer.end(par -> LOGGER.debug("KafkaProducer has been closed jobProfile: {}", jobProfile));
      processFilePromise.handle(ar);
    });

    return processFilePromise.future();

  }

  /**
   * Read file and count total records it is contains
   *
   * @param file       - file with records
   * @param jobProfile - job profile main info
   * @return total records in file;
   */
  //TODO: just quite a contradictory method
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
   * Updates JobExecutions with given JobProfile value
   *
   * @param jobs       jobs to update
   * @param jobProfile JobProfile entity
   * @param params     parameters necessary for connection to the OKAPI
   * @return Future
   */
  private Future<Void> updateJobsProfile(List<JobExecution> jobs, JobProfileInfo jobProfile, OkapiConnectionParams params) {
    Promise<Void> promise = Promise.promise();
    List<Future<Void>> updateJobProfileFutures = new ArrayList<>(jobs.size());
    for (JobExecution job : jobs) {
      updateJobProfileFutures.add(updateJobProfile(job.getId(), jobProfile, params));
    }
    GenericCompositeFuture.all(updateJobProfileFutures).onComplete(updatedJobsProfileAr -> {
      if (updatedJobsProfileAr.failed()) {
        promise.fail(updatedJobsProfileAr.cause());
      } else {
        LOGGER.info("All the child jobs have been updated by job profile, parent job {}", jobs.get(0).getParentJobId());
        promise.complete();
      }
    });
    return promise.future();
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
    Promise<Void> promise = Promise.promise();
    ChangeManagerClient client = new ChangeManagerClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.putChangeManagerJobExecutionsJobProfileById(jobId, jobProfile, response -> {
        if (response.result().statusCode() != HttpStatus.HTTP_OK.toInt()) {
          LOGGER.error("Error updating job profile for JobExecution {}. StatusCode: {}", jobId, response.result().statusMessage());
          promise.fail(new HttpStatusException(response.result().statusCode(), "Error updating JobExecution"));
        } else {
          LOGGER.info("Job profile for job {} successfully updated.", jobId);
          promise.complete();
        }
      });
    } catch (Exception e) {
      LOGGER.error("Couldn't update jobProfile for JobExecution with id {}", jobId, e);
      promise.fail(e);
    }
    return promise.future();
  }

}
