package org.folio.service.processing.split;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

public class AsyncInputStream implements ReadStream<Buffer> {

  public static final int DEFAULT_READ_BUFFER_SIZE = 8192;
  private static final Logger LOGGER = LogManager.getLogger();
  private final ReadableByteChannel channel;
  private final Context context;

  private boolean active = false;

  private boolean closed = false;

  private Handler<Buffer> dataHandler;
  private Handler<Void> endHandler;
  private Handler<Throwable> exceptionHandler;

  /**
   * Create a new AsyncInputStream to wrap a regular {@link InputStream}
   */
  public AsyncInputStream(Vertx vertx, Context context, InputStream in) {
    this.context = context;
    this.channel = Channels.newChannel(in);
  }

  @Override
  public ReadStream<Buffer> exceptionHandler(@Nullable Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public ReadStream<Buffer> handler(@Nullable Handler<Buffer> handler) {
    dataHandler = handler;
    return this;
  }

  @Override
  public ReadStream<Buffer> pause() {
    active = false;
    return this;
  }

  @Override
  public ReadStream<Buffer> resume() {
    if (!closed) {
      active = true;
      fetch(1L);
    }

    return this;
  }

  @Override
  public ReadStream<Buffer> fetch(long amount) {
    doRead();
    return this;
  }

  @Override
  public ReadStream<Buffer> endHandler(@Nullable Handler<Void> endHandler) {
    this.endHandler = endHandler;
    return this;
  }

  private void doRead() {
    context.runOnContext(v -> {
      if (active) {
        int bytesRead;
        ByteBuffer byteBuffer = ByteBuffer.allocate(DEFAULT_READ_BUFFER_SIZE);
        try {
          bytesRead = channel.read(byteBuffer);
        } catch (IOException e) {
          e.printStackTrace();
          LOGGER.error(e);
          doClose();
          return;
        }
        if (bytesRead > 0) {
          byteBuffer.flip();
          Buffer buffer = Buffer.buffer(bytesRead);
          buffer.setBytes(0, byteBuffer);
          dataHandler.handle(buffer);

          doRead();
        } else {
          doClose();
        }
      }
    });
  }

  public void close() {
    doClose();
  }

  private void doClose() {
    if (!closed) {
      closed = true;
      active = false;
      var localEndHandler = endHandler;
      if (localEndHandler != null) {
        context.runOnContext(vv -> localEndHandler.handle(null));
      }
      try {
        if (channel.isOpen()) {
          channel.close();
        }
      } catch (IOException e) {
        var localExceptionHandler = this.exceptionHandler;
        if (localExceptionHandler != null) {
          context.runOnContext(vv -> localExceptionHandler.handle(e));
        }
      }
    }
  }
}
