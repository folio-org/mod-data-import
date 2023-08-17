package org.folio.service.file;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.ext.web.handler.HttpException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HttpStatus;
import org.folio.dao.DataImportQueueItemDao;
import org.folio.rest.client.ChangeManagerClient;
import org.folio.rest.impl.util.BufferMapper;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.JobExecutionDtoCollection;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Service containing methods to manage the lifecycle and initiate processing of
 * split files.
 */
@Service
public class SplitFileProcessingService {

  private static final Logger LOGGER = LogManager.getLogger();

  private DataImportQueueItemDao queueItemDao;

  @Autowired
  public SplitFileProcessingService(DataImportQueueItemDao queueItemDao) {
    this.queueItemDao = queueItemDao;
  }

  /**
   * Registers split parts as Job Executions in mod-source-record-manager
   * and adds each part to the DI queue.
   *
   * @return a {@link CompositeFuture} of {@link JobExecutionDto}
   */
  public CompositeFuture registerSplitFiles(
    UploadDefinition parentUploadDefinition,
    JobExecution parentJobExecution,
    ChangeManagerClient client,
    int parentJobSize,
    String tenant,
    List<String> keys
  ) {
    List<Future<JobExecutionDto>> futures = new ArrayList<>();

    int partNumber = 1;
    for (String key : keys) {
      InitJobExecutionsRqDto initJobExecutionsRqDto = new InitJobExecutionsRqDto()
        .withFiles(Arrays.asList(new File().withName(key)))
        .withParentJobId(parentJobExecution.getId())
        .withJobPartNumber(partNumber)
        .withTotalJobParts(keys.size())
        .withSourceType(InitJobExecutionsRqDto.SourceType.COMPOSITE)
        .withUserId(
          Objects.nonNull(parentUploadDefinition.getMetadata())
            ? parentUploadDefinition.getMetadata().getCreatedByUserId()
            : null
        );

      // outer scope variable could change before lambda execution, so we make it final here
      final int thisPartNumber = partNumber;

      Promise<JobExecutionDto> promise = Promise.promise();
      futures.add(promise.future());

      client.postChangeManagerJobExecutions(
        initJobExecutionsRqDto,
        response -> {
          try {
            if (
              response.result().statusCode() != HttpStatus.HTTP_CREATED.toInt()
            ) {
              LOGGER.warn(
                "registerSplitFiles:: Error creating new child JobExecution for key {}. Status message: {}",
                key,
                response.result().statusMessage()
              );
              throw new HttpException(
                response.result().statusCode(),
                "Error creating new JobExecution"
              );
            } else {
              BufferMapper
                .mapBufferContentToEntity(
                  response.result().bodyAsBuffer(),
                  InitJobExecutionsRsDto.class
                )
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
                    .onSuccess(v -> promise.complete(execution))
                )
                .onFailure(promise::fail);
            }
          } catch (Exception e) {
            promise.fail(e);
          }
        }
      );
      partNumber++;
    }

    return CompositeFuture.join(
      futures.stream().map(Future.class::cast).collect(Collectors.toList())
    );
  }
}
