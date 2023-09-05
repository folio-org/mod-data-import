package org.folio.service.file;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.handler.HttpException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;
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
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.JobExecutionDtoCollection;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.ProcessFilesRqDto;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.service.processing.ParallelFileChunkingProcessor;
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

  private ParallelFileChunkingProcessor fileProcessor;

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

    this.fileProcessor = new ParallelFileChunkingProcessor();
  }

  public Future<Void> startJob(
    ProcessFilesRqDto entity,
    ChangeManagerClient client,
    OkapiConnectionParams params
  ) {
    CompositeFuture splittingFuture = CompositeFuture.all(
      entity
        .getUploadDefinition()
        .getFileDefinitions()
        .stream()
        .map(FileDefinition::getSourcePath)
        .map(this::splitFile)
        .collect(Collectors.toList())
    );

    CompositeFuture initializationFuture = CompositeFuture.all(
      createParentJobExecutions(entity, client),
      splittingFuture.map(cf ->
        cf
          .list()
          .stream()
          .map(SplitFileInformation.class::cast)
          .collect(Collectors.toMap(SplitFileInformation::getKey, el -> el))
      )
    );

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

        return CompositeFuture.all(
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
                params,
                split.getSplitKeys()
              )
                .compose(childExecs ->
                  fileProcessor.updateJobsProfile(
                    childExecs
                      .list()
                      .stream()
                      .map(JobExecution.class::cast)
                      .map(jobExec ->
                        new JobExecutionDto().withId(jobExec.getId())
                      )
                      .collect(Collectors.toList()),
                    entity.getJobProfileInfo(),
                    params
                  )
                )
                .compose(r ->
                  fileProcessor.updateJobsProfile(
                    Arrays.asList(
                      new JobExecutionDto()
                        .withId(jobExecEntry.getValue().getId())
                    ),
                    entity.getJobProfileInfo(),
                    params
                  )
                )
                .compose(r ->
                  uploadDefinitionService.updateJobExecutionStatus(
                    jobExecEntry.getValue().getId(),
                    new StatusDto()
                      .withStatus(StatusDto.Status.COMMIT_IN_PROGRESS),
                    params
                  )
                )
                .andThen(v ->
                  LOGGER.info(
                    "Created child job executions for {}",
                    jobExecEntry.getKey()
                  )
                );
            })
            .collect(Collectors.toList())
        );
      })
      // do this after everything has been queued successfully
      .compose(v ->
        uploadDefinitionService.updateBlocking(
          entity.getUploadDefinition().getId(),
          definition ->
            Future.succeededFuture(
              definition.withStatus(UploadDefinition.Status.COMPLETED)
            ),
          params.getTenantId()
        )
      )
      .andThen(v -> LOGGER.info("Job split and queued successfully!"))
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
   * @param params the headers from the original request
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
    OkapiConnectionParams params,
    Collection<String> keys
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
                  .withId(UUID.randomUUID().toString())
                  .withJobExecutionId(execution.getId())
                  .withUploadDefinitionId(parentUploadDefinition.getId())
                  .withTenant(params.getTenantId())
                  .withOriginalSize(parentJobSize)
                  .withFilePath(key)
                  .withTimestamp(new Date())
                  .withPartNumber(thisPartNumber)
                  .withProcessing(false)
                  .withOkapiUrl(params.getOkapiUrl())
                  .withDataType(jobProfileInfo.getDataType().toString())
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
   * Delete all queue items (DI) and child job executions (SRM) for a given job execution ID
   */
  public Future<Void> cancelJob(
    String jobExecutionId,
    OkapiConnectionParams params,
    ChangeManagerClient client
  ) {
    return uploadDefinitionService
      .getJobExecutionById(jobExecutionId, params)
      // we have the job execution here
      .compose(jobExecutionResponse -> {
        if (
          JobExecution.SubordinationType.PARENT_MULTIPLE.equals(
            jobExecutionResponse.getSubordinationType()
          )
        ) {
          return client.getChangeManagerJobExecutionsChildrenById(
            jobExecutionId,
            Integer.MAX_VALUE,
            0
          );
        } else {
          throw new IllegalStateException("JobExecution is not a parent job");
        }
      })
      // we have the response buffer
      .compose(this::verifyOkStatus)
      .map(buffer ->
        BufferMapper.mapBufferContentToEntitySync(
          buffer,
          JobExecutionDtoCollection.class
        )
      )
      // we have the list of children
      .compose(collection -> {
        List<Future<Void>> deleteQueueFutures = new ArrayList<>();
        for (JobExecutionDto exec : collection.getJobExecutions()) {
          deleteQueueFutures.add(
            queueItemDao
              .deleteDataImportQueueItemByJobExecutionId(exec.getId())
              // the delete call can fail if the queue item doesn't exist (has already been processed)
              .recover(err -> Future.succeededFuture())
          );

          deleteQueueFutures.add(
            client
              .putChangeManagerJobExecutionsStatusById(
                exec.getId(),
                new StatusDto().withStatus(StatusDto.Status.CANCELLED)
              )
              .compose(this::verifyOkStatus)
              .mapEmpty()
          );
        }

        return CompositeFuture.all(
          deleteQueueFutures
            .stream()
            .map(Future.class::cast)
            .collect(Collectors.toList())
        );
      })
      .mapEmpty();
  }

  /**
   * Sends a InitJobExecutionsRqDto with sufficient error handling
   *
   * @param client the {@link ChangeManagerClient} to send the request with
   * @param request the request to send
   * @return a promise which will succeed with the response body as a {@link InitJobExecutionsRsDto}
   */
  private static Future<InitJobExecutionsRsDto> sendJobExecutionRequest(
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
      // all data is now in the object
      .onSuccess(list -> promise.complete(result.withSplitKeys(list)))
      .onFailure(promise::fail);

    return promise.future();
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
          .map(FileDefinition::getSourcePath)
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
      .map(response -> {
        LOGGER.info("Created parent job execution: {}", response);
        // turn into map from filename -> JobExecution
        return response
          .getJobExecutions()
          .stream()
          .collect(Collectors.toMap(JobExecution::getSourcePath, exec -> exec));
      })
      .onFailure(err -> LOGGER.error("Error creating parent job execution", err)
      );
  }

  protected Future<Buffer> verifyOkStatus(HttpResponse<Buffer> response) {
    if (response.statusCode() >= 200 || response.statusCode() <= 299) {
      return Future.succeededFuture(response.bodyAsBuffer());
    } else {
      return Future.failedFuture(
        LOGGER.throwing(
          new IllegalStateException(
            "Response came back with status code " + response.statusCode()
          )
        )
      );
    }
  }

  /**
   * Hold information about a split file that will be needed for the creation
   * of job executions, etc
   */
  @Data
  @With
  @NoArgsConstructor
  @AllArgsConstructor
  public static class SplitFileInformation {

    private String key;
    private Integer totalRecords;
    private List<String> splitKeys;
  }
}
