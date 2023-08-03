package org.folio.service.processing.split;

import static java.nio.file.StandardOpenOption.CREATE;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.service.s3storage.MinioStorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

public class FileSplitWriter implements WriteStream<Buffer> {

  @Autowired
  private MinioStorageService minioStorageService;

  @Value("${RECORDS_PER_SPLIT_FILE:1000}")
  private int maxRecordsPerChunk;

  private final Context vertxContext;
  private String chunkFolder;

  private String key;

  private boolean uploadFilesToS3;

  private boolean deleteLocalFiles;

  private byte recordTerminator;

  private final Promise<CompositeFuture> chunkUploadingCompositeFuturePromise;

  private final List<Future<String>> chunkProcessingFutures;
  private Handler<Throwable> exceptionHandler;

  private OutputStream currentChunkStream;
  private String currentChunkPath;

  private String currentChunkKey;

  private int recordCount = 0;

  private int chunkIndex = 1;

  private static final Logger LOGGER = LogManager.getLogger();

  public FileSplitWriter(
      Context vertxContext,
      Promise<CompositeFuture> chunkUploadingCompositeFuturePromise,
      String key,
      String chunkFolder,
      byte recordTerminator,
      boolean uploadFilesToS3,
      boolean deleteLocalFiles) throws IOException {
    this.vertxContext = vertxContext;
    this.chunkUploadingCompositeFuturePromise = chunkUploadingCompositeFuturePromise;
    chunkProcessingFutures = new ArrayList<>();
    this.key = key;
    this.chunkFolder = chunkFolder;

    this.recordTerminator = recordTerminator;
    this.uploadFilesToS3 = uploadFilesToS3;
    this.deleteLocalFiles = deleteLocalFiles;

    init();
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
    LOGGER.error("Error writing file chunk", e);
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
      // due to code smell related to declaration of chunkProcessingFutures
      // Future.all is not available:
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

  @Override
  public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    return false;
  }

  @Override
  public WriteStream<Buffer> drainHandler(@Nullable Handler<Void> handler) {
    // drain handler is unused
    return this;
  }

  private void startChunk() throws IOException {
    String fileName = FileSplitUtilities.buildChunkKey(key, chunkIndex++);
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
          "{}: stream was null, did not end",
          Thread.currentThread().getName());
    }
  }

  private void init() throws IOException {
    startChunk();
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
                          "{}: Failed Uploading file: {}",
                          Thread.currentThread().getName(),
                          chunkPath);

                      chunkPromise.fail(s3Path.cause());
                    } else if (s3Path.succeeded()) {
                      LOGGER.info(
                          "{}: Successfully Uploading file: {} time:{}",
                          Thread.currentThread().getName(),
                          chunkPath,
                          System.currentTimeMillis());
                    }
                  });
            } catch (IOException e) {
              LOGGER.error(
                  "{}: Uploading file: {} IOEException",
                  Thread.currentThread().getName(),
                  chunkPath);
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
              "{}: Uploading file: {} Completed time: {}",
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
