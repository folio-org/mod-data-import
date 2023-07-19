package org.folio.service.processing.split;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.service.s3storage.MinioStorageService;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static java.nio.file.StandardOpenOption.CREATE;

public class FileSplitWriter implements WriteStream<Buffer> {

  private MinioStorageService minioStorageService;

  private final Context vertxContext;
  private final String chunkFolder;

  private final String key;

  private final byte recordTerminator;
  private final int maxRecordsPerChunk;

  private final Promise<CompositeFuture> chunkUploadingCompositeFuturePromise;

  private final List<Future> chunkProcessingFutures;
  private Handler<Throwable> exceptionHandler;
  private Handler<Void> drainHandler;

  private OutputStream currentChunkStream;
  private String currentChunkPath;

  private String currentChunkKey;

  private int recordCount = 0;

  private int chunkIndex = 1;

  private static final Logger LOGGER = LogManager.getLogger();


  public FileSplitWriter(Context vertxContext, MinioStorageService minioStorageService, Promise<CompositeFuture> chunkUploadingCompositeFuturePromise, String chunkFolder, String key, byte recordTerminator, int maxRecordsPerChunk) throws IOException {
    this.vertxContext = vertxContext;
    this.minioStorageService = minioStorageService;
    this.chunkUploadingCompositeFuturePromise = chunkUploadingCompositeFuturePromise;
    this.chunkFolder = chunkFolder;
    this.key = key;
    this.recordTerminator = recordTerminator;
    this.maxRecordsPerChunk = maxRecordsPerChunk;

    chunkProcessingFutures = new ArrayList<>();

    init();
  }

  @Override
  public WriteStream<Buffer> exceptionHandler(@Nullable Handler<Throwable> handler) {
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
          nextChunk();
        } catch (IOException e) {
          LOGGER.error("Error writing file chunk", e);
          if (handler != null) {
            handler.handle(Future.failedFuture(e));
          }
          if (exceptionHandler != null) {
            exceptionHandler.handle(e);
          }
          chunkUploadingCompositeFuturePromise.fail(e);
          return;
        }
      }
      try {
        if (currentChunkStream != null) {
          currentChunkStream.write(b);
        } else {
          var e = new RuntimeException("Unreachable statement");
          LOGGER.error("Error writing file chunk", e);
          if (handler != null) {
            handler.handle(Future.failedFuture(e));
          }
          if (exceptionHandler != null) {
            exceptionHandler.handle(e);
          }
        }
        if (b == recordTerminator) {
          if (++recordCount == maxRecordsPerChunk) {
            if (currentChunkStream != null) {
              currentChunkStream.close();
              uploadChunkAsync(currentChunkPath, currentChunkKey);
              currentChunkStream = null;
              recordCount = 0;
            }
          }
        }
      } catch (IOException e) {
        LOGGER.error("Error writing file chunk", e);
        if (handler != null) {
          handler.handle(Future.failedFuture(e));
        }
        if (exceptionHandler != null) {
          exceptionHandler.handle(e);
        }
        chunkUploadingCompositeFuturePromise.fail(e);
      }
    }

  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    try {
      if (currentChunkStream != null) {
        currentChunkStream.close();
        uploadChunkAsync(currentChunkPath, currentChunkKey);
      }
      handler.handle(Future.succeededFuture());
      chunkUploadingCompositeFuturePromise.complete(CompositeFuture.all(chunkProcessingFutures));
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
    drainHandler = handler;
    return this;
  }

  private void nextChunk() throws IOException {
    String fileName = FileSplitUtilities.buildPartKey(key, chunkIndex++);
    var path = Path.of(chunkFolder, fileName);
    currentChunkPath = path.toString();
    currentChunkKey = fileName;
    currentChunkStream = Files.newOutputStream(path, CREATE);
    System.out.println(Thread.currentThread().getName() + ": nextChunk: " + currentChunkPath);
  }

  private void init() throws IOException {
    nextChunk();
  }

  private void uploadChunkAsync(String chunkPath, String chunkKey) {
    Promise<String> chunkPromise = Promise.promise();
    chunkProcessingFutures.add(chunkPromise.future());
    vertxContext.executeBlocking(event -> {

      // chunk file uploading to S3
      System.out.println(Thread.currentThread().getName() + ": Uploading file:" + chunkPath + ": key:" + chunkKey);
      Path cp = Path.of(chunkPath);
      try {
        minioStorageService.write(chunkKey, Files.newInputStream(cp)).onComplete(s3Path -> {
          if (s3Path.failed()) {
            System.out.println(Thread.currentThread().getName() + ": Failed Uploading file: " + chunkPath);
            chunkPromise.fail(s3Path.cause());
          } else if (s3Path.succeeded()){
            System.out.println(Thread.currentThread().getName() + ": Successfully Uploaded file: " + chunkPath);
          }
          }
        );
      } catch (IOException e) {
        System.out.println(Thread.currentThread().getName() + ": Uploading file: " + chunkPath + " IOException");
        event.fail(e);
        chunkPromise.fail(e);
        return;
      }
      //TODO: Once file uploading completed,
      // there could be a next async handler to do some DB calls or initiate a next step for chunk processing
      try {
        Files.delete(cp);
      } catch (IOException e) {
        event.fail(e);
        chunkPromise.fail(e);
        return;
      }
      System.out.println(Thread.currentThread().getName() + ": Uploading file: " + chunkPath + " Completed");
      event.complete();
      chunkPromise.complete(chunkPath);
    }, false);
  }
}
