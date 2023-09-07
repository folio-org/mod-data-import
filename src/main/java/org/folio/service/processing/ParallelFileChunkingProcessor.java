package org.folio.service.processing;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.apache.commons.collections4.list.UnmodifiableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HttpStatus;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.rest.client.ChangeManagerClient;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.DataImportInitConfig;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.ProcessFilesRqDto;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.service.processing.kafka.SourceReaderReadStreamWrapper;
import org.folio.service.processing.kafka.WriteStreamWrapper;
import org.folio.service.processing.reader.RecordsReaderException;
import org.folio.service.processing.reader.SourceReader;
import org.folio.service.processing.reader.SourceReaderBuilder;
import org.folio.service.storage.FileStorageService;
import org.folio.service.storage.FileStorageServiceBuilder;
import org.folio.service.upload.UploadDefinitionService;
import org.folio.service.upload.UploadDefinitionServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static io.vertx.core.Future.succeededFuture;
import static org.folio.rest.jaxrs.model.StatusDto.ErrorStatus.FILE_PROCESSING_ERROR;
import static org.folio.rest.jaxrs.model.StatusDto.Status.ERROR;
import static org.folio.service.util.EventHandlingUtil.sendEventToKafka;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INITIALIZATION_STARTED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_READ;

/**
 * Processing files in parallel threads, one thread per one file.
 * File chunking process implies reading and splitting the file into chunks of data.
 * Every chunk represents collection of source records, see ({@link org.folio.rest.jaxrs.model.RawRecordsDto}).
 * After the target file gets split into records, ParallelFileChunkingProcessor sends records to the mod-source-record-manager
 * for further processing.
 */
// autowirable for async processing outside the normal Vert.x eventbus
@Component
public class ParallelFileChunkingProcessor implements FileProcessor {

  private static final Logger LOGGER = LogManager.getLogger();

  private Vertx vertx;

  private KafkaConfig kafkaConfig;

  public ParallelFileChunkingProcessor() {}

  @Autowired
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
        }).onFailure(e -> LOGGER.warn("process:: Can`t process file. Cause: {}", e.getMessage()));
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
        processFile(fileStorageService.getFile(fileDefinition.getSourcePath()), fileDefinition.getJobExecutionId(),
                    jobProfile, params).onComplete(par -> {
            if (par.failed()) {
              LOGGER.warn("processFiles:: File was processed with errors {}. Cause: {}", fileDefinition.getSourcePath(), par.cause());
              uploadDefinitionService.updateJobExecutionStatus(
                fileDefinition.getJobExecutionId(),
                new StatusDto().withStatus(ERROR).withErrorStatus(FILE_PROCESSING_ERROR),
                params);
            } else {
              LOGGER.info("processFiles:: File {} successfully processed.", fileDefinition.getSourcePath());
            }
          }
        ));
    }
  }

  /**
   * Process file, in a future
   *
   * @param file               file on disk
   * @param jobExecutionId     job execution ID
   * @param jobProfile         job profile, contains profile type
   * @param fileStorageService service to obtain file
   * @param params             parameters necessary for connection to the OKAPI
   * @return Future
   */
  public Future<Void> processFile(File file,
                                  String jobExecutionId,
                                  JobProfileInfo jobProfile,
                                  OkapiConnectionParams params) {
    String eventType = DI_RAW_RECORDS_CHUNK_READ.value();

    String topicName = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(),
      KafkaTopicNameHelper.getDefaultNameSpace(), params.getTenantId(), eventType);

    Promise<Void> processFilePromise = Promise.promise();

    int totalRecords;
    SourceReader reader;
    try {
      totalRecords = countTotalRecordsInFile(file, jobProfile);
      if (totalRecords == 0) {
        String errorMessage = "File is empty or content is invalid!";
        LOGGER.warn(errorMessage);
        processFilePromise.fail(errorMessage);
        sendDiErrorForJob(jobExecutionId, params, errorMessage);
        return processFilePromise.future();
      }
      reader = SourceReaderBuilder.build(file, jobProfile);
    } catch (RecordsReaderException | UnsupportedOperationException e) {
      String errorMessage = "Can not initialize reader. Cause: " + e.getMessage();
      LOGGER.warn(errorMessage);
      processFilePromise.fail(errorMessage);
      sendDiErrorForJob(jobExecutionId, params, errorMessage);
      return processFilePromise.future();
    }

    sendDiInitializationForJob(jobExecutionId, totalRecords, params);

    SourceReaderReadStreamWrapper readStreamWrapper = new SourceReaderReadStreamWrapper(
      vertx, reader, jobExecutionId, totalRecords,
      params, 100, topicName);
    readStreamWrapper.pause();

    LOGGER.debug("processFile:: Starting to send event to Kafka... jobProfile: {}, eventType: {}", jobProfile, eventType);
    KafkaProducer<String, String> producer = KafkaProducer.createShared(vertx,
      eventType + "_Producer", kafkaConfig.getProducerProps());
    readStreamWrapper.pipeTo(new WriteStreamWrapper(producer))
      .<Void>mapEmpty()
      .eventually(x -> producer.flush())
      .eventually(x -> producer.close())
      .onComplete(res -> {
        if (res.succeeded()) {
          LOGGER.info("processFile:: Sending event to Kafka finished. jobProfile: {}, eventType: {}", jobProfile, eventType);
        } else {
          LOGGER.warn("processFile:: Error sending event to Kafka. jobProfile: {}, eventType: {}", jobProfile, eventType, res.cause());
        }
        processFilePromise.handle(res);
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
  public Future<Void> updateJobsProfile(List<JobExecutionDto> jobs, JobProfileInfo jobProfile, OkapiConnectionParams params) {
    Promise<Void> promise = Promise.promise();
    List<Future<Void>> updateJobProfileFutures = new ArrayList<>(jobs.size());
    for (JobExecutionDto job : jobs) {
      updateJobProfileFutures.add(updateJobProfile(job.getId(), jobProfile, params));
    }
    GenericCompositeFuture.all(updateJobProfileFutures).onComplete(updatedJobsProfileAr -> {
      if (updatedJobsProfileAr.failed()) {
        promise.fail(updatedJobsProfileAr.cause());
      } else {
        LOGGER.info("updateJobsProfile:: All the child jobs have been updated by job profile, parent job {}", jobs.get(0).getParentJobId());
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
          LOGGER.warn("updateJobProfile:: Error updating job profile for JobExecution {}. StatusCode: {}", jobId, response.result().statusMessage());
          promise.fail(new HttpException(response.result().statusCode(), "Error updating JobExecution"));
        } else {
          LOGGER.info("updateJobProfile:: Job profile for job {} successfully updated.", jobId);
          promise.complete();
        }
      });
    } catch (Exception e) {
      LOGGER.warn("updateJobProfile:: Couldn't update jobProfile for JobExecution with id {}", jobId, e);
      promise.fail(e);
    }
    return promise.future();
  }

  private void sendDiInitializationForJob(String jobExecutionId, int totalRecordsCount, OkapiConnectionParams okapiParams) {
    DataImportInitConfig initConfig = new DataImportInitConfig();
    initConfig.setJobExecutionId(jobExecutionId);
    initConfig.setTotalRecords(totalRecordsCount);

    sendEventToKafka(okapiParams.getTenantId(), Json.encode(initConfig), DI_INITIALIZATION_STARTED.value(), KafkaHeaderUtils.kafkaHeadersFromMultiMap(okapiParams.getHeaders()), kafkaConfig, null, vertx)
      .onFailure(th -> LOGGER.warn("sendDiInitializationForJob:: Error publishing DI_INITIALIZATION_STARTED event for jobExecutionId: {}", jobExecutionId, th));
  }

  private void sendDiErrorForJob(String jobExecutionId, OkapiConnectionParams okapiParams, String errorMsg) {
    HashMap<String, String> contextItems = new HashMap<>();
    contextItems.put("ERROR", errorMsg);

    DataImportEventPayload errorPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withJobExecutionId(jobExecutionId)
      .withOkapiUrl(okapiParams.getOkapiUrl())
      .withTenant(okapiParams.getTenantId())
      .withToken(okapiParams.getToken())
      .withContext(contextItems);

    sendEventToKafka(okapiParams.getTenantId(), Json.encode(errorPayload), DI_ERROR.value(), KafkaHeaderUtils.kafkaHeadersFromMultiMap(okapiParams.getHeaders()), kafkaConfig, null, vertx)
      .onFailure(th -> LOGGER.warn("sendDiErrorForJob:: Error publishing DI_ERROR event for jobExecutionId: {}", errorPayload.getJobExecutionId(), th));
  }
}
