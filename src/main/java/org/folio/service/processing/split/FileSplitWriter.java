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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.service.s3storage.MinioStorageService;

public class FileSplitWriter implements WriteStream<Buffer> {

  private static final Logger LOGGER = LogManager.getLogger();

  private final Context vertxContext;
  private final MinioStorageService minioStorageService;
  private final Promise<CompositeFuture> chunkUploadingCompositeFuturePromise;
  private final List<Future<String>> chunkProcessingFutures;
  private Handler<Throwable> exceptionHandler;

  private String chunkFolder;
  private String outputKey;
  private boolean uploadFilesToS3;
  private boolean deleteLocalFiles;

  private final byte recordTerminator;

  private int maxRecordsPerChunk;

  private ByteArrayOutputStream currentChunkStream;
  private String currentChunkPath;
  private String currentChunkKey;

  private int chunkIndex = 1;
  private int recordCount = 0;

  // used to hint the buffer size for the next chunks
  private int lastChunkSize;

  public FileSplitWriter(FileSplitWriterOptions options) {
    this.vertxContext = options.getVertxContext();
    this.minioStorageService = options.getMinioStorageService();
    this.chunkUploadingCompositeFuturePromise =
      options.getChunkUploadingCompositeFuturePromise();

    this.outputKey = options.getOutputKey();
    this.chunkFolder = options.getChunkFolder();

    this.maxRecordsPerChunk = options.getMaxRecordsPerChunk();
    this.uploadFilesToS3 = options.isUploadFilesToS3();
    this.deleteLocalFiles = options.isDeleteLocalFiles();

    // Per https://www.loc.gov/marc/makrbrkr.html, the average size of a MARC
    // record is 800-1500 chars (bytes).  So, we will use 800/record as the
    // first buffer's size (then future chunks can be the same size as previous).
    // This allows some responsiveness in the first chunk and later ones, to
    // ensure we don't waste too much time resizing the buffer.
    this.lastChunkSize = 800 * maxRecordsPerChunk;

    this.recordTerminator = options.getRecordTerminator();

    this.chunkProcessingFutures = new ArrayList<>();

    startChunk();
  }

  @Override
  public WriteStream<Buffer> exceptionHandler(
    @Nullable Handler<Throwable> handler
  ) {
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
    byte[] bytes = data.getBytes();
    int start = 0;
    int len = 0;

    for (int i = 0; i < bytes.length; i++) {
      if (
        bytes[i] == recordTerminator && (++recordCount == maxRecordsPerChunk)
      ) {
        len = i + 1 - start;

        try {
          if (currentChunkStream == null) {
            startChunk();
          }
          currentChunkStream.write(bytes, start, len);
          endChunk();
        } catch (IOException e) {
          handleWriteException(handler, e);
        }

        start = i + 1;
      }
    }

    if (start < bytes.length) {
      len = bytes.length - start;

      if (currentChunkStream == null) {
        startChunk();
      }
      currentChunkStream.write(bytes, start, len);
    }
  }

  private void handleWriteException(
    Handler<AsyncResult<Void>> handler,
    Exception e
  ) {
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
              new Future[chunkProcessingFutures.size()]
            )
          )
        )
      );
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
  private void startChunk() {
    String fileName = FileSplitUtilities.buildChunkKey(outputKey, chunkIndex++);
    Path path = Path.of(chunkFolder, new File(fileName).getName());
    currentChunkPath = path.toString();
    currentChunkKey = fileName;
    currentChunkStream = new ByteArrayOutputStream(lastChunkSize);
    LOGGER.info(
      "{}: startChunk:{} time:{}",
      Thread.currentThread().getName(),
      currentChunkPath,
      System.currentTimeMillis()
    );
  }

  /** Finalize the current chunk */
  private void endChunk() throws IOException {
    if (currentChunkStream != null) {
      currentChunkStream.close();
      currentChunkStream.writeTo(
        Files.newOutputStream(Path.of(currentChunkPath), CREATE)
      );
      lastChunkSize = currentChunkStream.size();
      uploadChunkAsync(currentChunkPath, currentChunkKey);

      // this will trigger a startChunk when more data is received
      currentChunkStream = null;
      recordCount = 0;

      LOGGER.info(
        "{}: finished chunk of size {} written to {}",
        Thread.currentThread().getName(),
        lastChunkSize,
        currentChunkPath
      );
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
            "{}: Uploading file: {}, key={}, time={}",
            Thread.currentThread().getName(),
            chunkPath,
            chunkKey,
            System.currentTimeMillis()
          );

          try {
            minioStorageService
              .write(chunkKey, Files.newInputStream(cp))
              .onComplete(s3Path -> {
                if (s3Path.failed()) {
                  LOGGER.info(
                    "{}: Failed uploading file: {}",
                    Thread.currentThread().getName(),
                    chunkPath
                  );

                  chunkPromise.fail(s3Path.cause());
                } else if (s3Path.succeeded()) {
                  LOGGER.info(
                    "{}: Successfully uploaded file: {} time:{}",
                    Thread.currentThread().getName(),
                    chunkPath,
                    System.currentTimeMillis()
                  );

                  if (deleteLocalFiles) {
                    try {
                      LOGGER.info(
                        "{}: Deleting local file: {}",
                        Thread.currentThread().getName(),
                        cp
                      );
                      Files.delete(cp);
                    } catch (IOException e) {
                      event.fail(e);
                      chunkPromise.fail(e);
                      return;
                    }
                  }

                  chunkPromise.complete(chunkKey);
                }
              });
          } catch (IOException e) {
            LOGGER.error(
              "{}: Exception uploading file: {}",
              Thread.currentThread().getName(),
              chunkPath
            );
            LOGGER.error(e);
            event.fail(e);
            chunkPromise.fail(e);
            return;
          }
        }
        if (!uploadFilesToS3 && deleteLocalFiles) {
          try {
            LOGGER.info(
              "{}: Deleting local file: {}",
              Thread.currentThread().getName(),
              cp
            );
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
          System.currentTimeMillis()
        );
        if (!uploadFilesToS3) {
          event.complete();
          chunkPromise.complete(chunkPath);
        }
      },
      false
    );
  }
}
