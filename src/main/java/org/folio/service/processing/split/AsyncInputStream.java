package org.folio.service.processing.split;

import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import javax.annotation.CheckForNull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Accessors(chain = true, fluent = true)
public class AsyncInputStream implements ReadStream<Buffer> {

  private static final Logger LOGGER = LogManager.getLogger();

  public static final int READ_BUFFER_SIZE = 8192;

  private final ReadableByteChannel channel;
  private final Context context;

  @Getter
  private boolean active = false;

  @Getter
  private boolean closed = false;

  @Setter
  @CheckForNull
  private Handler<Buffer> handler;

  @Setter
  @CheckForNull
  private Handler<Void> endHandler;

  @Setter
  @CheckForNull
  private Handler<Throwable> exceptionHandler;

  /**
   * Create a new AsyncInputStream to wrap a regular {@link InputStream}
   */
  public AsyncInputStream(Context context, InputStream in) {
    this.context = context;
    this.channel = Channels.newChannel(in);
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
    active = true;
    doRead();

    return this;
  }

  private void doRead() {
    context.runOnContext((Void v) -> {
      if (active) {
        int bytesRead;
        ByteBuffer byteBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE);

        try {
          bytesRead = channel.read(byteBuffer);

          if (bytesRead > 0) {
            byteBuffer.flip();
            Buffer buffer = Buffer.buffer(bytesRead);
            buffer.setBytes(0, byteBuffer);
            handler.handle(buffer);

            doRead();
          } else {
            close();
          }
        } catch (IOException e) {
          LOGGER.error("Unable to read from channel:", e);
          close();
          return;
        }
      }
    });
  }

  public void close() {
    if (!closed) {
      closed = true;
      active = false;

      if (this.endHandler != null) {
        context.runOnContext(vv -> this.endHandler.handle(null));
      }

      try {
        if (channel.isOpen()) {
          channel.close();
        }
      } catch (IOException e) {
        if (this.exceptionHandler != null) {
          context.runOnContext(vv -> this.exceptionHandler.handle(e));
        }
      }
    }
  }
}
