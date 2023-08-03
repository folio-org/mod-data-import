package org.folio.service.processing.split;

import static java.nio.file.StandardOpenOption.CREATE;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.validation.constraints.Min;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.service.s3storage.MinioStorageService;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;

public class FileSplitWriter implements WriteStream<Buffer> {

  private static final Logger LOGGER = LogManager.getLogger();

  @Autowired
  private MinioStorageService minioStorageService;

  private final Context vertxContext;
  private final Promise<CompositeFuture> chunkUploadingCompositeFuturePromise;
  private final List<Future<String>> chunkProcessingFutures;
  private Handler<Throwable> exceptionHandler;

  private String chunkFolder;
  private String outputKey;
  private boolean uploadFilesToS3;
  private boolean deleteLocalFiles;

  private int maxRecordsPerChunk;
  private byte recordTerminator;

  private OutputStream currentChunkStream;
  private String currentChunkPath;
  private String currentChunkKey;

  private int chunkIndex = 1;
  private int recordCount = 0;

  public FileSplitWriter(
      Context vertxContext,
      Promise<CompositeFuture> chunkUploadingCompositeFuturePromise,
      String outputKey,
      String chunkFolder,
      @Min(1) int maxRecordsPerChunk,
      byte recordTerminator,
      boolean uploadFilesToS3,
      boolean deleteLocalFiles) throws IOException {
    this.vertxContext = vertxContext;
    this.chunkUploadingCompositeFuturePromise = chunkUploadingCompositeFuturePromise;
    chunkProcessingFutures = new ArrayList<>();
    this.outputKey = outputKey;
    this.chunkFolder = chunkFolder;

    this.maxRecordsPerChunk = maxRecordsPerChunk;
    this.recordTerminator = recordTerminator;
    this.uploadFilesToS3 = uploadFilesToS3;
    this.deleteLocalFiles = deleteLocalFiles;

    startChunk();
  }

  @Override
  public WriteStream<Buffer> exceptionHandler(
      @Nullable Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public Future<Void> write(Buffer data) {
    Promise<Void> promise = Promise.promise();
    write(data, promise);
    return promise.future();
  }

  @Override
  public void write(Buffer data, Handler<AsyncResult<Void>> handler) {
    var bytes = data.getBytes();
    for (var b : bytes) {
      if (currentChunkStream == null) {
        try {
          startChunk();
        } catch (IOException e) {
          handleWriteException(handler, e);
          return;
        }
      }
      try {
        if (currentChunkStream != null) {
          currentChunkStream.write(b);
        } else {
          var e = new RuntimeException("Unreachable statement");
          handleWriteException(handler, e);
        }
        if (b == recordTerminator && (++recordCount == maxRecordsPerChunk)) {
          endChunk();
        }
      } catch (IOException e) {
        handleWriteException(handler, e);
      }
    }
  }

  private void handleWriteException(
      Handler<AsyncResult<Void>> handler,
      Exception e) {
    LOGGER.error("Error writing file chunk: ", e);
    if (handler != null) {
      handler.handle(Future.failedFuture(e));
    }
    if (exceptionHandler != null) {
      exceptionHandler.handle(e);
    }
    chunkUploadingCompositeFuturePromise.fail(e);
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    try {
      endChunk();
      handler.handle(Future.succeededFuture());
      // Future.all is not available, CompositeFuture.all is broken,
      // and we're on an older Vert.x, so we must resort to this ugly re-encapsulation
      // https://github.com/eclipse-vertx/vert.x/issues/2627
      chunkUploadingCompositeFuturePromise.complete(
          CompositeFuture.all(
              Arrays.asList(
                  chunkProcessingFutures.toArray(
                      new Future[chunkProcessingFutures.size()]))));
    } catch (IOException e) {
      handler.handle(Future.failedFuture(e));
      chunkUploadingCompositeFuturePromise.fail(e);
    }
  }

  // unused
  @Override
  public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
    return this;
  }

  // unused
  @Override
  public boolean writeQueueFull() {
    return false;
  }

  // unused
  @Override
  public WriteStream<Buffer> drainHandler(@Nullable Handler<Void> handler) {
    return this;
  }

  /** Start processing a new chunk */
  private void startChunk() throws IOException {
    String fileName = FileSplitUtilities.buildChunkKey(outputKey, chunkIndex++);
    var path = Path.of(chunkFolder, fileName);
    currentChunkPath = path.toString();
    currentChunkKey = fileName;
    currentChunkStream = Files.newOutputStream(path, CREATE);
    LOGGER.info(
        "{}: startChunk:{} time:{}",
        Thread.currentThread().getName(),
        currentChunkPath,
        System.currentTimeMillis());
  }

  /** Finalize the current chunk */
  private void endChunk() throws IOException {
    if (currentChunkStream != null) {
      currentChunkStream.close();
      uploadChunkAsync(currentChunkPath, currentChunkKey);
      currentChunkStream = null;
      recordCount = 0;
      LOGGER.info(
          "{}: endChunk:{} time:{}",
          Thread.currentThread().getName(),
          currentChunkPath,
          System.currentTimeMillis());
    } else {
      LOGGER.error(
          "{}: stream was null, so did not end this chunk",
          Thread.currentThread().getName());
    }
  }

  private void uploadChunkAsync(String chunkPath, String chunkKey) {
    Promise<String> chunkPromise = Promise.promise();
    chunkProcessingFutures.add(chunkPromise.future());
    vertxContext.executeBlocking(
        event -> {
          Path cp = Path.of(chunkPath);
          // chunk file uploading to S3
          if (uploadFilesToS3) {
            LOGGER.info(
                "{}: Uploading file:{}:key{} time:{}",
                Thread.currentThread().getName(),
                chunkPath,
                chunkKey,
                System.currentTimeMillis());

            try {
              minioStorageService
                  .write(chunkKey, Files.newInputStream(cp))
                  .onComplete(s3Path -> {
                    if (s3Path.failed()) {
                      LOGGER.info(
                          "{}: Failed uploading file: {}",
                          Thread.currentThread().getName(),
                          chunkPath);

                      chunkPromise.fail(s3Path.cause());
                    } else if (s3Path.succeeded()) {
                      LOGGER.info(
                          "{}: Successfully uploaded file: {} time:{}",
                          Thread.currentThread().getName(),
                          chunkPath,
                          System.currentTimeMillis());
                    }
                  });
            } catch (IOException e) {
              LOGGER.error(
                  "{}: Exception uploading file: {}",
                  Thread.currentThread().getName(),
                  chunkPath);
              LOGGER.error(e);
              event.fail(e);
              chunkPromise.fail(e);
              return;
            }
          }
          onFileComplete();
          if (deleteLocalFiles) {
            try {
              Files.delete(cp);
            } catch (IOException e) {
              event.fail(e);
              chunkPromise.fail(e);
              return;
            }
          }
          LOGGER.info(
              "{}: Finished processing chunk: {} Completed time: {}",
              Thread.currentThread().getName(),
              chunkPath,
              System.currentTimeMillis());
          event.complete();
          chunkPromise.complete(chunkPath);
        },
        false);
  }

  private void onFileComplete() {
    // Do any work here that is required once the fragment file is uploaded
    // TODO: is this needed?
  }
}
