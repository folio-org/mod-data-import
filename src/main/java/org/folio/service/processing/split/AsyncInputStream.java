package org.folio.service.processing.split;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.Arguments;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.impl.InboundBuffer;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AsyncInputStream implements ReadStream<Buffer>, AutoCloseable {

  public static final int DEFAULT_READ_BUFFER_SIZE = 8192;
  private static final Logger LOGGER = LogManager.getLogger();

  // Based on the inputStream with the real data
  private final ReadableByteChannel ch;
  private final Vertx vertx;
  private final Context context;

  private boolean closed;
  private boolean readInProgress;

  private Handler<Buffer> dataHandler;
  private Handler<Void> endHandler;
  private Handler<Throwable> exceptionHandler;
  private final InboundBuffer<Buffer> queue;

  private int readBufferSize = DEFAULT_READ_BUFFER_SIZE;

  /**
   * Create a new AsyncInputStream to wrap a regular {@link InputStream}
   */
  public AsyncInputStream(Vertx vertx, Context context, InputStream in) {
    this.vertx = vertx;
    this.context = context;
    this.ch = Channels.newChannel(in);
    this.queue = new InboundBuffer<>(context, 0);
    queue.handler(buff -> {
      if (buff.length() > 0) {
        handleData(buff);
      } else {
        handleEnd();
      }
    });
    queue.drainHandler(v -> doRead());
  }

  public void close() {
    closeInternal(null);
  }

  public void close(Handler<AsyncResult<Void>> handler) {
    closeInternal(handler);
  }

  /**
   * {@inheritDoc}
   *
   * @see io.vertx.core.streams.ReadStream#endHandler(io.vertx.core.Handler)
   */
  @Override
  public synchronized AsyncInputStream endHandler(Handler<Void> endHandler) {
    ensureOpen();
    this.endHandler = endHandler;
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @see io.vertx.core.streams.ReadStream#exceptionHandler(io.vertx.core.Handler)
   */
  @Override
  public synchronized AsyncInputStream exceptionHandler(
    Handler<Throwable> exceptionHandler
  ) {
    ensureOpen();
    this.exceptionHandler = exceptionHandler;
    return this;
  }

  /**
   * <strong>This will immediately start consumption of the stream.</strong>
   * Be sure that the endHandler and other options are set first, if needed
   *
   * {@inheritDoc}
   *
   * @see io.vertx.core.streams.ReadStream#handler(io.vertx.core.Handler)
   */
  @Override
  public synchronized AsyncInputStream handler(Handler<Buffer> handler) {
    ensureOpen();
    this.dataHandler = handler;
    if (this.dataHandler != null && !this.closed) {
      if (this.endHandler == null) {
        LOGGER.warn(
          "AsyncInputStream consumption has begun but endHandler is not set!!"
        );
        LOGGER.warn(
          "Iff you wish to use an endHandler, be sure it is set before setting the handler.  Otherwise, this may cause race conditions!"
        );
      }
      this.doRead();
    } else {
      queue.clear();
    }
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @see io.vertx.core.streams.ReadStream#pause()
   */
  @Override
  public synchronized AsyncInputStream pause() {
    ensureOpen();
    queue.pause();
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @see io.vertx.core.streams.ReadStream#resume()
   */
  @Override
  public synchronized AsyncInputStream resume() {
    ensureOpen(); // ensure not yet closed
    queue.resume();
    return this;
  }

  @Override
  public ReadStream<Buffer> fetch(long amount) {
    queue.fetch(amount);
    return this;
  }

  private void ensureOpen() {
    if (this.closed) {
      throw new IllegalStateException("InputStream is closed");
    }
  }

  private void ensureValidContext() {
    if (!vertx.getOrCreateContext().equals(context)) {
      // this is called from a handler, so throwing won't usually do anything
      // so, we want to ensure we log it appropriately
      IllegalStateException ex = new IllegalStateException(
        "AsyncInputStream must only be used in the context that created it, expected: " +
        this.context +
        " actual " +
        vertx.getOrCreateContext()
      );
      handleException(ex);
      throw ex;
    }
  }

  private synchronized void closeInternal(Handler<AsyncResult<Void>> handler) {
    ensureOpen();
    closed = true;
    doClose(handler);
  }

  private void doClose(Handler<AsyncResult<Void>> handler) {
    try {
      ch.close();
      if (handler != null) {
        this.vertx.runOnContext(v -> handler.handle(Future.succeededFuture()));
      }
    } catch (IOException e) {
      if (handler != null) {
        this.vertx.runOnContext(v -> handler.handle(Future.failedFuture(e)));
      }
    }
  }

  public synchronized AsyncInputStream read(
    Buffer buffer,
    int offset,
    int length,
    Handler<AsyncResult<Buffer>> handler
  ) {
    Objects.requireNonNull(buffer, "buffer");
    Objects.requireNonNull(handler, "handler");
    Arguments.require(offset >= 0, "offset must be >= 0");
    Arguments.require(length >= 0, "length must be >= 0");
    ensureOpen();
    ByteBuffer bb = ByteBuffer.allocate(length);
    doRead(buffer, offset, bb, handler);
    return this;
  }

  private void doRead() {
    ensureOpen();
    doRead(ByteBuffer.allocate(readBufferSize));
  }

  private synchronized void doRead(ByteBuffer bb) {
    if (!readInProgress) {
      readInProgress = true;
      Buffer buff = Buffer.buffer(readBufferSize);
      doRead(
        buff,
        0,
        bb,
        ar -> {
          if (ar.succeeded()) {
            readInProgress = false;
            Buffer buffer = ar.result();
            // Empty buffer represents end of file
            if (queue.write(buffer) && buffer.length() > 0) {
              doRead(bb);
            }
          } else {
            handleException(ar.cause());
          }
        }
      );
    }
  }

  private void doRead(
    Buffer writeBuff,
    int offset,
    ByteBuffer buff,
    Handler<AsyncResult<Buffer>> handler
  ) {
    // ReadableByteChannel doesn't have a completion handler, so we wrap it into
    // an executeBlocking and use the future there
    vertx.executeBlocking(
      future -> {
        try {
          Integer bytesRead = ch.read(buff);
          future.complete(bytesRead);
        } catch (Exception e) {
          LOGGER.error(e);
          future.fail(e);
        }
      },
      res -> {
        if (res.failed()) {
          context.runOnContext(v ->
            handler.handle(Future.failedFuture(res.cause()))
          );
        } else {
          // Do the completed check
          Integer bytesRead = (Integer) res.result();
          if (bytesRead == -1) {
            // End of file
            context.runOnContext(v -> {
              buff.flip();
              writeBuff.setBytes(offset, buff);
              buff.compact();
              handler.handle(Future.succeededFuture(writeBuff));
            });
          } else if (buff.hasRemaining()) {
            // resubmit
            doRead(writeBuff, offset, buff, handler);
          } else {
            // It's been fully written

            context.runOnContext(v -> {
              buff.flip();
              writeBuff.setBytes(offset, buff);
              buff.compact();
              handler.handle(Future.succeededFuture(writeBuff));
            });
          }
        }
      }
    );
  }

  private void handleData(Buffer buff) {
    Handler<Buffer> handler;
    synchronized (this) {
      handler = this.dataHandler;
    }
    if (handler != null) {
      ensureValidContext();
      handler.handle(buff);
    }
  }

  private synchronized void handleEnd() {
    Handler<Void> handler;
    synchronized (this) {
      dataHandler = null;
      handler = this.endHandler;
    }
    if (endHandler != null) {
      ensureValidContext();
      handler.handle(null);
    }
  }

  private void handleException(Throwable t) {
    if (exceptionHandler != null && t instanceof Exception) {
      exceptionHandler.handle(t);
    } else {
      LOGGER.error("Unhandled exception", t);
    }
  }
}
