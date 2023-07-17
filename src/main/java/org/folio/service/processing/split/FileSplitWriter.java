package org.folio.service.processing.split;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.CREATE;

public class FileSplitWriter implements WriteStream<Buffer> {
  private final Context vertxContext;
  private final String chunkFolder;
  private final byte recordTerminator;
  private final int maxRecordsPerChunk;
  private Handler<Throwable> exceptionHandler;
  private Handler<Void> drainHandler;

  private OutputStream currentChunkStream;
  private String currentChunkPath;

  private int recordCount = 0;

  private int chunkIndex = 0;

  public FileSplitWriter(Context vertxContext, String chunkFolder, byte recordTerminator, int maxRecordsPerChunk) throws IOException {
    this.vertxContext = vertxContext;
    this.chunkFolder = chunkFolder;
    this.recordTerminator = recordTerminator;
    this.maxRecordsPerChunk = maxRecordsPerChunk;
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
          e.printStackTrace();
          if (handler != null) {
            handler.handle(Future.failedFuture(e));
          }
          if (exceptionHandler != null) {
            exceptionHandler.handle(e);
          }
          return;
        }
      }
      try {
        if (currentChunkStream != null) {
          currentChunkStream.write(b);
        } else {
          var e = new RuntimeException("Unreachable statement");
          e.printStackTrace();
          if (handler != null) {
            handler.handle(Future.failedFuture(e));
          }
          if (exceptionHandler != null) {
            exceptionHandler.handle(e);
          }
        }
        if (b == recordTerminator) {
          if (++recordCount == maxRecordsPerChunk) {
            currentChunkStream.close();
            uploadChunkAsync(currentChunkPath);
            currentChunkStream = null;
            recordCount = 0;
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
        if (handler != null) {
          handler.handle(Future.failedFuture(e));
        }
        if (exceptionHandler != null) {
          exceptionHandler.handle(e);
        }
      }
    }

  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    try {
      if (currentChunkStream != null) {
        currentChunkStream.close();
        uploadChunkAsync(currentChunkPath);
      }
      handler.handle(Future.succeededFuture());
    } catch (IOException e) {
      handler.handle(Future.failedFuture(e));
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
    String fileName = "chunkFile." + chunkIndex++ + ".mrc";
    var path = Path.of(chunkFolder, fileName);
    currentChunkPath = path.toString();
    currentChunkStream = Files.newOutputStream(path, CREATE);
  }

  private void init() throws IOException {
    nextChunk();
  }

  private void uploadChunkAsync(String chunkPath) {
    vertxContext.executeBlocking(event -> {

      //TODO: implement chunk file uploading to S3 and
      System.out.println(Thread.currentThread().getName() + " Uploading file:" + chunkPath);
      //Simply a file uploading simulation
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        System.out.println(Thread.currentThread().getName() + " Uploading file: " + chunkPath + " InterruptedException");
        event.fail(e);
        return;
      }
      //TODO: Once file uploading completed,
      // there could be a next async handler to do some DB calls or initiate a next step for chunk processing
      try {
        Files.delete(Path.of(chunkPath));
      } catch (IOException e) {
        event.fail(e);
        return;
      }
      System.out.println(Thread.currentThread().getName() + " Uploading file: " + chunkPath + " Completed");
      event.complete();
    }, false);
  }
}
