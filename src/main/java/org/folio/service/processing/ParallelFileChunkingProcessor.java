package org.folio.service.processing;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.http.HttpStatus;
import org.folio.dataImport.util.OkapiConnectionParams;
import org.folio.dataImport.util.RestUtil;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.service.processing.reader.FileSystemStubSourceReader;
import org.folio.service.processing.reader.SourceReader;
import org.folio.service.storage.FileStorageService;
import org.folio.service.storage.FileStorageServiceBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Processing files in parallel threads. One thread per one file.
 */
public class ParallelFileChunkingProcessor implements FileProcessor {
  private final Logger logger = LoggerFactory.getLogger(ParallelFileChunkingProcessor.class);

  private static final int THREAD_POOL_SIZE = 20;
  private static final String RAW_RECORDS_SERVICE_URL = "/change-manager/records/";
  private Vertx vertx;
  private String tenantId;
  /* WorkerExecutor provides separate worker pool for code execution */
  private WorkerExecutor executor;

  public ParallelFileChunkingProcessor(Vertx vertx, String tenantId) {
    this.vertx = vertx;
    this.tenantId = tenantId;
    this.executor = this.vertx.createSharedWorkerExecutor("processing-files-thread-pool", THREAD_POOL_SIZE);
  }

  @Override
  public Future<Void> process(UploadDefinition uploadDefinition, OkapiConnectionParams params) {
    Future<Void> future = Future.future();
    FileStorageServiceBuilder.build(this.vertx, this.tenantId, params).setHandler(fileStorageServiceAr -> {
      if (fileStorageServiceAr.failed()) {
        future.fail(fileStorageServiceAr.cause());
      } else {
        FileStorageService fileStorageService = fileStorageServiceAr.result();
        List<FileDefinition> fileDefinitions = uploadDefinition.getFileDefinitions();
        List<Future> processFileDefinitionFutures = new ArrayList<>(fileDefinitions.size());
        for (FileDefinition fileDefinition : fileDefinitions) {
          this.executor.executeBlocking(blockingFuture -> {
              processFile(fileDefinition, fileStorageService, params, blockingFuture);
              processFileDefinitionFutures.add(blockingFuture);
            },
            asyncResult -> {
              if (asyncResult.failed()) {
                logger.error(asyncResult.cause());
              } else {
                logger.error("File with id " + fileDefinition.getId() + " successfully handled.");
              }
            });
        }
        CompositeFuture.all(processFileDefinitionFutures).setHandler(ar -> {
          if (ar.failed()) {
            future.fail(ar.cause());
          } else {
            logger.info("Files linked to the upload definition with id " + uploadDefinition.getId() + " are handled");
            future.complete();
          }
        });
      }
    });
    return future;
  }

  /**
   * Processing file
   *
   * @param fileDefinition     fileDefinition entity
   * @param fileStorageService service to obtain file
   * @param params             parameters necessary for connection to the OKAPI
   * @param blockingHandler    handler to complete by the end of file processing
   */
  private void processFile(FileDefinition fileDefinition, FileStorageService fileStorageService, OkapiConnectionParams params, Handler<AsyncResult<Object>> blockingHandler) { // NOSONAR
    SourceReader reader = new FileSystemStubSourceReader(this.vertx.fileSystem());
    while (reader.hasNext()) {
      reader.readNext().setHandler(readRecordsAr -> {
        if (readRecordsAr.failed()) {
          logger.error("Can not read next chunk of records for the file: " + fileDefinition.getSourcePath());
          blockingHandler.handle(Future.failedFuture(readRecordsAr.cause()));
        } else {
          RawRecordsDto chunk = readRecordsAr.result();
          postRawRecords(fileDefinition.getJobExecutionId(), chunk, params).setHandler(postedRecordsAr -> {
            if (postedRecordsAr.failed()) {
              blockingHandler.handle(Future.failedFuture(postedRecordsAr.cause()));
            } else {
              if (!reader.hasNext()) {
                logger.info("All the chunks for file: " + fileDefinition.getSourcePath() + " successfully sent");
                blockingHandler.handle(Future.succeededFuture());
              }
            }
          });
        }
      });
    }
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
          logger.error("Can not post raw records for job with id: " + jobExecutionId);
          future.fail(responseResult.cause());
        } else {
          future.complete();
        }
      });
    return future;
  }
}
