package org.folio.service.file;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.CheckForNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.folio.rest.jaxrs.model.Metadata;
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

  private final Vertx vertx;

  private final FileSplitService fileSplitService;
  private final MinioStorageService minioStorageService;

  private final DataImportQueueItemDao queueItemDao;
  private final UploadDefinitionService uploadDefinitionService;

  private final ParallelFileChunkingProcessor fileProcessor;

  @Autowired
  public SplitFileProcessingService(
    Vertx vertx,
    FileSplitService fileSplitService,
    MinioStorageService minioStorageService,
    DataImportQueueItemDao queueItemDao,
    UploadDefinitionService uploadDefinitionService,
    ParallelFileChunkingProcessor fileProcessor
  ) {
    this.vertx = vertx;

    this.fileSplitService = fileSplitService;
    this.minioStorageService = minioStorageService;

    this.queueItemDao = queueItemDao;
    this.uploadDefinitionService = uploadDefinitionService;

    this.fileProcessor = fileProcessor;
  }

  /** Start a job based on information passed to the /processFiles endpoint */
  public Future<Void> startJob(
    ProcessFilesRqDto entity,
    ChangeManagerClient client,
    OkapiConnectionParams params
  ) {
    return initializeJob(entity, client)
      .compose(splitPieces ->
        CompositeFuture.all(
          splitPieces
            .values()
            .stream()
            .map(splitFileInformation ->
              initializeChildren(entity, client, params, splitFileInformation)
            )
            .map(Future.class::cast)
            .toList()
        )
      )
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
      .onSuccess(v -> LOGGER.info("Job split and queued successfully!"))
      .onFailure(err -> LOGGER.error("Unable to start job: ", err))
      .mapEmpty();
  }

  /** Split file and create parent job executions for a new job */
  protected Future<Map<String, SplitFileInformation>> initializeJob(
    ProcessFilesRqDto entity,
    ChangeManagerClient client
  ) {
    CompositeFuture splittingFuture = CompositeFuture.all(
      entity
        .getUploadDefinition()
        .getFileDefinitions()
        .stream()
        .map(FileDefinition::getSourcePath)
        .map(key -> splitFile(key, entity.getJobProfileInfo()))
        .map(Future.class::cast)
        .toList()
    );

    return CompositeFuture
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
      .map((CompositeFuture cf) -> {
        Map<String, JobExecution> executions = cf.resultAt(0);
        Map<String, SplitFileInformation> splitInformation = cf.resultAt(1);

        splitInformation.forEach((key, value) ->
          value.setJobExecution(executions.get(key))
        );

        return splitInformation;
      })
      .compose(result ->
        CompositeFuture
          .all(
            result
              .values()
              .stream()
              .map((SplitFileInformation splitInfo) -> {
                JobExecution execution = splitInfo.getJobExecution();
                execution.setTotalRecordsInFile(splitInfo.getTotalRecords());

                return client
                  .putChangeManagerJobExecutionsById(
                    execution.getId(),
                    null,
                    execution
                  )
                  .map(this::verifyOkStatus);
              })
              .map(Future.class::cast)
              .toList()
          )
          .map(result)
      )
      .onFailure(e -> LOGGER.error("Unable to initialize parent job: ", e));
  }

  /** Register split file parts and fill split job execution job profile/status */
  protected Future<Void> initializeChildren(
    ProcessFilesRqDto entity,
    ChangeManagerClient client,
    OkapiConnectionParams params,
    SplitFileInformation splitInfo
  ) {
    return registerSplitFileParts(
      entity.getUploadDefinition(),
      splitInfo.getJobExecution(),
      entity.getJobProfileInfo(),
      client,
      splitInfo.getTotalRecords(),
      params,
      splitInfo.getSplitKeys()
    )
      .map(childExecs ->
        childExecs
          .list()
          .stream()
          .map(JobExecution.class::cast)
          .map(jobExec ->
            new JobExecutionDto()
              .withId(jobExec.getId())
              .withSourcePath(jobExec.getSourcePath())
          )
          .toList()
      )
      // update all children
      .compose(childExecs ->
        fileProcessor
          .updateJobsProfile(childExecs, entity.getJobProfileInfo(), params)
          // update parent
          .compose(r ->
            fileProcessor.updateJobsProfile(
              Arrays.asList(
                new JobExecutionDto()
                  .withId(splitInfo.getJobExecution().getId())
              ),
              entity.getJobProfileInfo(),
              params
            )
          )
          .compose(r ->
            uploadDefinitionService.updateJobExecutionStatus(
              splitInfo.getJobExecution().getId(),
              new StatusDto().withStatus(StatusDto.Status.COMMIT_IN_PROGRESS),
              params
            )
          )
          .map((Boolean result) -> {
            if (Boolean.FALSE.equals(result)) {
              throw new IllegalStateException(
                "Could not mark job as in progress"
              );
            }
            return result;
          })
          .compose(v ->
            CompositeFuture.all(
              // we use an IntStream here to have access to i, as the index (and therefore part number)
              IntStream
                .range(0, childExecs.size())
                .mapToObj((int i) -> {
                  JobExecutionDto execution = childExecs.get(i);
                  return queueItemDao.addQueueItem(
                    new DataImportQueueItem()
                      .withId(UUID.randomUUID().toString())
                      .withJobExecutionId(execution.getId())
                      .withUploadDefinitionId(
                        entity.getUploadDefinition().getId()
                      )
                      .withTenant(params.getTenantId())
                      .withOriginalSize(splitInfo.getTotalRecords())
                      .withFilePath(execution.getSourcePath())
                      .withTimestamp(new Date())
                      .withPartNumber(i + 1)
                      .withProcessing(false)
                      .withOkapiUrl(params.getOkapiUrl())
                      .withDataType(
                        entity.getJobProfileInfo().getDataType().toString()
                      )
                  );
                })
                .map(Future.class::cast)
                .toList()
            )
          )
      )
      .onSuccess(v ->
        LOGGER.info("Created child job executions for {}", splitInfo.getKey())
      )
      .onFailure(err -> LOGGER.error("Unable to initialize children", err))
      .mapEmpty();
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
  protected CompositeFuture registerSplitFileParts(
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
          getUserIdFromMetadata(parentUploadDefinition.getMetadata())
        );

      futures.add(
        sendJobExecutionRequest(client, initJobExecutionsRqDto)
          .map(collection -> collection.getJobExecutions().get(0))
          .onFailure(err ->
            LOGGER.error(
              "Unable to register split file execution for {}: ",
              key,
              err
            )
          )
      );

      partNumber++;
    }

    return CompositeFuture.join(
      futures.stream().map(Future.class::cast).toList()
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
      .compose((JobExecution jobExecution) -> {
        if (
          jobExecution.getSubordinationType() ==
          JobExecution.SubordinationType.COMPOSITE_PARENT
        ) {
          return client.getChangeManagerJobExecutionsChildrenById(
            jobExecutionId,
            Integer.MAX_VALUE,
            0
          );
        } else {
          throw new IllegalStateException("Job execution is not a parent job!");
        }
      })
      // we have the response buffer
      .map(this::verifyOkStatus)
      .map(buffer ->
        BufferMapper.mapBufferContentToEntitySync(
          buffer,
          JobExecutionDtoCollection.class
        )
      )
      // we have the list of children
      .compose((JobExecutionDtoCollection collection) -> {
        List<Future<Void>> futures = new ArrayList<>();

        // parent execution
        futures.add(
          client
            .putChangeManagerJobExecutionsStatusById(
              jobExecutionId,
              new StatusDto().withStatus(StatusDto.Status.CANCELLED)
            )
            .map(this::verifyOkStatus)
            .mapEmpty()
        );

        // all children
        for (JobExecutionDto exec : collection.getJobExecutions()) {
          futures.add(
            queueItemDao
              .deleteDataImportQueueItemByJobExecutionId(exec.getId())
              // the delete call can fail if the queue item doesn't exist (has already been processed)
              .recover(err -> Future.succeededFuture())
          );

          switch (exec.getStatus()) {
            case COMMITTED, ERROR, DISCARDED, CANCELLED -> {
              // don't cancel jobs that are already completed
            }
            default -> futures.add(
              client
                .putChangeManagerJobExecutionsStatusById(
                  exec.getId(),
                  new StatusDto().withStatus(StatusDto.Status.CANCELLED)
                )
                .map(this::verifyOkStatus)
                .mapEmpty()
            );
          }
        }

        return CompositeFuture.all(
          futures.stream().map(Future.class::cast).toList()
        );
      })
      .onFailure(err -> LOGGER.error("Error cancelling job", err))
      .mapEmpty();
  }

  /**
   * Sends a InitJobExecutionsRqDto with sufficient error handling
   *
   * @param client the {@link ChangeManagerClient} to send the request with
   * @param request the request to send
   * @return a promise which will succeed with the response body as a {@link InitJobExecutionsRsDto}
   */
  protected Future<InitJobExecutionsRsDto> sendJobExecutionRequest(
    ChangeManagerClient client,
    InitJobExecutionsRqDto request
  ) {
    Promise<HttpResponse<Buffer>> promise = Promise.promise();

    client.postChangeManagerJobExecutions(request, promise::handle);

    return promise
      .future()
      .map(this::verifyOkStatus)
      .map(buffer ->
        BufferMapper.mapBufferContentToEntitySync(
          buffer,
          InitJobExecutionsRsDto.class
        )
      );
  }

  /**
   * Create parent job executions for all files described in the upload definition.
   * @return a {@link Future} containing a map from filename/key -> {@link JobExecution}
   */
  protected Future<Map<String, JobExecution>> createParentJobExecutions(
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
          .toList()
      )
      .withJobProfileInfo(entity.getJobProfileInfo())
      .withSourceType(InitJobExecutionsRqDto.SourceType.COMPOSITE)
      .withUserId(
        getUserIdFromMetadata(entity.getUploadDefinition().getMetadata())
      );

    return sendJobExecutionRequest(client, initJobExecutionsRqDto)
      .map((InitJobExecutionsRsDto response) -> {
        LOGGER.info(
          "Created {} parent job executions",
          response.getJobExecutions().size()
        );
        // turn into map from filename -> JobExecution
        return response
          .getJobExecutions()
          .stream()
          .collect(Collectors.toMap(JobExecution::getSourcePath, exec -> exec));
      })
      .onFailure(err -> LOGGER.error("Error creating parent job execution", err)
      );
  }

  /**
   * Split a file into chunks, returning a {@link SplitFileInformation} object containing
   * the original key, the total number of records in the file, and the keys of the split chunks.
   *
   * Note that non-MARC binary format files will not be split; instead, the keys of the split
   * chunks will simply be a list containing only the original key
   */
  protected Future<SplitFileInformation> splitFile(
    String key,
    JobProfileInfo profile
  ) {
    return Future
      .succeededFuture(new SplitFileInformation().withKey(key))
      // splitting and counting must be done sequentially as splitting deletes the original file
      .compose(result ->
        minioStorageService
          .readFile(key)
          .map((InputStream stream) -> {
            try {
              return result.withTotalRecords(
                FileSplitUtilities.countRecordsInFile(key, stream, profile)
              );
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          })
      )
      .compose((SplitFileInformation file) -> {
        if (FileSplitUtilities.isMarcBinary(key, profile)) {
          return fileSplitService
            .splitFileFromS3(vertx.getOrCreateContext(), key)
            .map(file::withSplitKeys);
        } else {
          return Future.succeededFuture(file.withSplitKeys(Arrays.asList(key)));
        }
      });
  }

  protected Buffer verifyOkStatus(HttpResponse<Buffer> response) {
    if (
      response.statusCode() >= HttpStatus.SC_OK &&
      response.statusCode() <= HttpStatus.SC_NO_CONTENT
    ) {
      return response.bodyAsBuffer();
    } else {
      throw LOGGER.throwing(
        new IllegalStateException(
          "Response came back with status code " +
          response.statusCode() +
          " and body " +
          response.bodyAsString()
        )
      );
    }
  }

  @CheckForNull
  protected String getUserIdFromMetadata(@CheckForNull Metadata metadata) {
    if (Objects.nonNull(metadata)) {
      return metadata.getCreatedByUserId();
    } else {
      return null;
    }
  }

  /**
   * Hold information about a split file that will be needed for the creation
   * of job executions, etc
   */
  @Data
  @With
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class SplitFileInformation {

    private String key;
    private Integer totalRecords;
    private List<String> splitKeys;

    private JobExecution jobExecution;
  }
}
