package org.folio.service.file;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.web.handler.HttpException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HttpStatus;
import org.folio.dao.DataImportQueueItemDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.client.ChangeManagerClient;
import org.folio.rest.impl.util.BufferMapper;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.ProcessFilesRqDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.service.processing.split.FileSplitService;
import org.folio.service.processing.split.FileSplitUtilities;
import org.folio.service.s3storage.MinioStorageService;
import org.folio.service.upload.UploadDefinitionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Service containing methods to manage the lifecycle and initiate processing of
 * split files.
 */
@Service
public class SplitFileProcessingService {

  private static final Logger LOGGER = LogManager.getLogger();

  private Vertx vertx;

  private FileSplitService fileSplitService;
  private MinioStorageService minioStorageService;

  private DataImportQueueItemDao queueItemDao;
  private UploadDefinitionService uploadDefinitionService;

  @Autowired
  public SplitFileProcessingService(
    Vertx vertx,
    FileSplitService fileSplitService,
    MinioStorageService minioStorageService,
    DataImportQueueItemDao queueItemDao,
    UploadDefinitionService uploadDefinitionService
  ) {
    this.vertx = vertx;

    this.fileSplitService = fileSplitService;
    this.minioStorageService = minioStorageService;

    this.queueItemDao = queueItemDao;
    this.uploadDefinitionService = uploadDefinitionService;
  }

  public Future<Void> startJob(
    ProcessFilesRqDto entity,
    ChangeManagerClient client,
    String tenant
  ) {
    CompositeFuture splittingFuture = CompositeFuture
      .all(
        entity
          .getUploadDefinition()
          .getFileDefinitions()
          .stream()
          .map(FileDefinition::getName)
          .map(this::splitFile)
          .collect(Collectors.toList())
      )
      .onSuccess(v -> LOGGER.info("Finished splittingFuture"))
      .onFailure(v -> LOGGER.info("Failed splittingFuture"));

    CompositeFuture initializationFuture = CompositeFuture
      .all(
        createParentJobExecutions(entity, client),
        splittingFuture.map(cf ->
          cf
            .list()
            .stream()
            .map(SplitFileInformation.class::cast)
            .collect(Collectors.toMap(SplitFileInformation::getKey, el -> el))
        )
      )
      .onSuccess(v -> LOGGER.info("Finished initializationFuture"))
      .onFailure(v -> LOGGER.info("Failed initializationFuture"));

    return initializationFuture
      .compose(result -> {
        // type erasure for conversion to Map<String, JobExecution> since Java can only check that it's a Map
        // https://stackoverflow.com/questions/2592642/type-safety-unchecked-cast-from-object
        @SuppressWarnings("unchecked")
        Map<String, JobExecution> parentJobExecutions = (Map<String, JobExecution>) result
          .list()
          .get(0);

        // same here
        @SuppressWarnings("unchecked")
        Map<String, SplitFileInformation> splitInformation = (Map<String, SplitFileInformation>) result
          .list()
          .get(1);

        return CompositeFuture
          .all(
            parentJobExecutions
              .entrySet()
              .stream()
              .map(jobExecEntry -> {
                // should always be here, but let's orElseThrow to be safe
                SplitFileInformation split = Optional
                  .ofNullable(splitInformation.get(jobExecEntry.getKey()))
                  .orElseThrow();

                return registerSplitFileParts(
                  entity.getUploadDefinition(),
                  jobExecEntry.getValue(),
                  entity.getJobProfileInfo(),
                  client,
                  split.getTotalRecords(),
                  tenant,
                  split.getSplitKeys()
                )
                  .onSuccess(v ->
                    LOGGER.info(
                      "Finished nested registerSplitFileParts {}",
                      jobExecEntry.getValue().getJobPartNumber()
                    )
                  )
                  .onFailure(v ->
                    LOGGER.info(
                      "Failed nested registerSplitFileParts {}",
                      jobExecEntry.getValue().getJobPartNumber()
                    )
                  );
              })
              .collect(Collectors.toList())
          )
          .onSuccess(v ->
            LOGGER.info("Finished nested CF BBBBBBBBBBBBBBBBBBBBBBBB")
          )
          .onFailure(v ->
            LOGGER.info("Failed nested CF BBBBBBBBBBBBBBBBBBBBBBBB")
          );
      })
      .compose(v -> Future.succeededFuture());
  }

  /**
   * Registers split parts as Job Executions in mod-source-record-manager
   * and adds each part to the DI queue.
   *
   * @param parentUploadDefinition the upload definition representing these files
   * @param parentJobExecution the parent composite job execution
   * @param jobProfileInfo the job profile to be used for later processing
   * @param client the {@link ChangeManagerClient} to make API calls to
   * @param jobProfileId the ID of the job profile to be used for later processing
   * @param parentJobSize the size of the parent job, as calculated by {@code FileSplitUtilities}
   * @param tenant the tenant of the request
   * @param keys the list of S3 keys to register, as returned by {@code FileSplitService}
   *
   * @return a {@link CompositeFuture} of {@link JobExecution}
   */
  public CompositeFuture registerSplitFileParts(
    UploadDefinition parentUploadDefinition,
    JobExecution parentJobExecution,
    JobProfileInfo jobProfileInfo,
    ChangeManagerClient client,
    int parentJobSize,
    String tenant,
    List<String> keys
  ) {
    List<Future<JobExecution>> futures = new ArrayList<>();

    int partNumber = 1;
    for (String key : keys) {
      InitJobExecutionsRqDto initJobExecutionsRqDto = new InitJobExecutionsRqDto()
        .withFiles(Arrays.asList(new File().withName(key)))
        .withParentJobId(parentJobExecution.getId())
        .withJobProfileInfo(jobProfileInfo)
        .withJobPartNumber(partNumber)
        .withTotalJobParts(keys.size())
        .withSourceType(InitJobExecutionsRqDto.SourceType.COMPOSITE)
        .withUserId(
          Objects.nonNull(parentUploadDefinition.getMetadata())
            ? parentUploadDefinition.getMetadata().getCreatedByUserId()
            : null
        );

      // outer scope variable could change before lambda execution, so we make it final here
      int thisPartNumber = partNumber;

      futures.add(
        sendJobExecutionRequest(client, initJobExecutionsRqDto)
          .map(collection -> collection.getJobExecutions().get(0))
          .compose(execution ->
            queueItemDao
              .addQueueItem(
                new DataImportQueueItem()
                  .withJobExecutionId(execution.getId())
                  .withUploadDefinitionId(parentUploadDefinition.getId())
                  .withTenant(tenant)
                  .withOriginalSize(parentJobSize)
                  .withFilePath(key)
                  .withTimestamp(Instant.now().toString())
                  .withPartNumber(thisPartNumber)
                  .withProcessing(false)
              )
              // we don't want the queue item, just the execution, so we discard the result
              // and return the execution from the earlier step
              .map(v -> execution)
          )
      );

      partNumber++;
    }

    return CompositeFuture.join(
      futures.stream().map(Future.class::cast).collect(Collectors.toList())
    );
  }

  /**
   * Gets the S3 storage key for a given job execution ID.
   *
   * <strong>No guarantee is made that the returned key will be valid in these cases:</strong>
   * <ul>
   * <li>The job execution ID refers to a parent or other meta job</li>
   * <li>The job execution was created before S3-like storage was enabled, meaning
   *     the original file was never uploaded to S3</li>
   * </ul>
   *
   * The returned key <strong>may or may not</strong> exist and may have been deleted;
   * no checking for this is done.
   *
   * Asynchronous as we need to communicate with mod-srm to get the key.
   * - The alternative to this would be to add an API to mod-srm (requiring adding
   *     a full S3 library to mod-srm), or
   * - Allowing the UI to provide the key back to us, leading to arbitrary
   *     file access, potentially across tenants (even if the keys are hard-to-guess).
   *
   * @param jobExecutionId the job execution ID for the slice that we want the key from
   * @return a future for the job's S3 key
   */
  public Future<String> getKey(
    String jobExecutionId,
    OkapiConnectionParams params
  ) {
    return uploadDefinitionService
      .getJobExecutionById(jobExecutionId, params)
      .map(JobExecution::getSourcePath);
  }

  /**
   * Create parent job executions for all files described in the upload definition.
   * @return a {@link Future} containing a map from filename/key -> {@link JobExecution}
   */
  private Future<Map<String, JobExecution>> createParentJobExecutions(
    ProcessFilesRqDto entity,
    ChangeManagerClient client
  ) {
    InitJobExecutionsRqDto initJobExecutionsRqDto = new InitJobExecutionsRqDto()
      .withFiles(
        entity
          .getUploadDefinition()
          .getFileDefinitions()
          .stream()
          .map(FileDefinition::getName)
          .map(key -> new File().withName(key))
          .collect(Collectors.toList())
      )
      .withJobProfileInfo(entity.getJobProfileInfo())
      .withSourceType(InitJobExecutionsRqDto.SourceType.COMPOSITE)
      .withUserId(
        Objects.nonNull(entity.getUploadDefinition().getMetadata())
          ? entity.getUploadDefinition().getMetadata().getCreatedByUserId()
          : null
      );

    return sendJobExecutionRequest(client, initJobExecutionsRqDto)
      .map(response ->
        // turn into map from filename -> JobExecution
        response
          .getJobExecutions()
          .stream()
          .collect(Collectors.toMap(JobExecution::getFileName, exec -> exec))
      );
  }

  /**
   * Sends a InitJobExecutionsRqDto with sufficient error handling
   *
   * @param client the {@link ChangeMAnagerClient} to send the request with
   * @param request the request to send
   * @return a promise which will succeed with the response body as a {@link InitJobExecutionsRsDto}
   */
  private Future<InitJobExecutionsRsDto> sendJobExecutionRequest(
    ChangeManagerClient client,
    InitJobExecutionsRqDto request
  ) {
    Promise<InitJobExecutionsRsDto> promise = Promise.promise();

    client.postChangeManagerJobExecutions(
      request,
      response -> {
        try {
          if (
            response.result().statusCode() != HttpStatus.HTTP_CREATED.toInt()
          ) {
            LOGGER.warn(
              "Error creating parent JobExecution from request {}. Status message: {}",
              request,
              response.result().statusMessage()
            );
            throw new HttpException(
              response.result().statusCode(),
              "Error creating new JobExecution"
            );
          }

          promise.complete(
            BufferMapper.mapBufferContentToEntitySync(
              response.result().bodyAsBuffer(),
              InitJobExecutionsRsDto.class
            )
          );
        } catch (Exception e) {
          promise.fail(e);
        }
      }
    );

    return promise.future();
  }

  /**
   * Split a file into chunks, returning a {@link SplitFileInformation} object containing
   * the original key, the total number of records in the file, and the keys of the split chunks
   */
  private Future<SplitFileInformation> splitFile(String key) {
    // object will be hydrated by futures
    SplitFileInformation result = new SplitFileInformation();
    result.setKey(key);

    Promise<SplitFileInformation> promise = Promise.promise();

    minioStorageService
      .readFile(key)
      // must be done sequentially as splitting deletes the original file
      .map(stream -> {
        try {
          result.setTotalRecords(
            FileSplitUtilities.countRecordsInMarcFile(stream)
          );
          return null;
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      })
      .compose(v ->
        fileSplitService.splitFileFromS3(vertx.getOrCreateContext(), key)
      )
      .onSuccess(list -> {
        result.setSplitKeys(list);
        // all data is now in the object
        promise.complete(result);
      })
      .onFailure(promise::fail);

    return promise.future();
  }

  /**
   * Hold information about a split file that will be needed for the creation
   * of job executions, etc
   */
  @Data
  public static class SplitFileInformation {

    private String key;
    private Integer totalRecords;
    private List<String> splitKeys;
  }
}
