package org.folio.service.split;

import static org.junit.Assert.assertThrows;

import java.io.ByteArrayInputStream;

import org.folio.service.processing.split.AsyncInputStream;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class AsyncInputStreamTest {

  protected static Vertx vertx = Vertx.vertx();

  // 0 chunks
  protected static byte[] emptyBuff = new byte[0];
  // 0.5 chunks
  protected static byte[] smallBuff = new byte[AsyncInputStream.DEFAULT_READ_BUFFER_SIZE / 2];
  // 1.0 chunks
  protected static byte[] mediumBuff = new byte[AsyncInputStream.DEFAULT_READ_BUFFER_SIZE];
  // 2.5 chunks
  protected static byte[] largeBuff = new byte[AsyncInputStream.DEFAULT_READ_BUFFER_SIZE * 2
      + AsyncInputStream.DEFAULT_READ_BUFFER_SIZE / 2];

  static {
    for (int i = 0; i < smallBuff.length; i++) {
      smallBuff[i] = (byte) (i / 32);
    }
    for (int i = 0; i < mediumBuff.length; i++) {
      mediumBuff[i] = (byte) (i / 32);
    }
    for (int i = 0; i < largeBuff.length; i++) {
      largeBuff[i] = (byte) (i / 32);
    }
  }

  @Test
  public void testExceptionalRead() {
    AsyncInputStream stream = new AsyncInputStream(vertx, vertx.getOrCreateContext(),
        new ByteArrayInputStream(emptyBuff));

    Handler<AsyncResult<Buffer>> handler = result -> {
    };

    Buffer testBuffer = Buffer.buffer(1);

    assertThrows(NullPointerException.class, () -> stream.read(null, 0, 0, 0, null));
    assertThrows(NullPointerException.class, () -> stream.read(testBuffer, 0, 0, 0, null));
    assertThrows(IllegalArgumentException.class, () -> stream.read(testBuffer, -1, 0, 0, handler));
    assertThrows(IllegalArgumentException.class, () -> stream.read(testBuffer, 0, -1, 0, handler));
    assertThrows(IllegalArgumentException.class, () -> stream.read(testBuffer, 0, 0, -1, handler));

    stream.close();

    assertThrows(IllegalStateException.class, () -> stream.read(testBuffer, 0, 0, 1, handler));
  }
}
