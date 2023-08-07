package org.folio.service.split;

import static org.folio.util.VertxMatcherAssert.asyncAssertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.folio.service.processing.split.AsyncInputStream;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class AsyncInputStreamTest {

  protected static Vertx vertx = Vertx.vertx();

  // 0 chunks
  protected static byte[] emptyBuff = new byte[0];
  // 0.5 chunks
  protected static byte[] smallBuff = new byte[8192 / 2];
  // 1.0 chunks
  protected static byte[] mediumBuff = new byte[8192];
  // 2.5 chunks
  protected static byte[] largeBuff = new byte[8192 * 2 + 8192 / 2];

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
  public void testEmptyRead(TestContext context) {
    AsyncInputStream stream = new AsyncInputStream(
      vertx,
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(emptyBuff)
    );

    Buffer testBuffer = Buffer.buffer();
    stream.read(
      testBuffer,
      0,
      128,
      context.asyncAssertSuccess(result -> {
        asyncAssertThat(context, result.length(), is(0));

        asyncAssertThat(context, result, is(testBuffer));
        stream.close(context.asyncAssertSuccess());
      })
    );
  }

  @Test
  public void testSmallFullRead(TestContext context) {
    AsyncInputStream stream = new AsyncInputStream(
      vertx,
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(smallBuff)
    );

    Buffer testBuffer = Buffer.buffer();
    stream.read(
      testBuffer,
      0,
      8192,
      context.asyncAssertSuccess(result -> {
        // stops at full stream being consumed
        asyncAssertThat(context, result.length(), is(4096));

        asyncAssertThat(context, result, is(testBuffer));
        stream.close(context.asyncAssertSuccess());
      })
    );
  }

  @Test
  public void testSmallMultiRead(TestContext context) {
    AsyncInputStream stream = new AsyncInputStream(
      vertx,
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(smallBuff)
    );

    Buffer testBuffer = Buffer.buffer();
    stream.read(
      testBuffer,
      0,
      128,
      context.asyncAssertSuccess(result -> {
        asyncAssertThat(context, result.length(), is(128));
        asyncAssertThat(
          context,
          result.getBytes(),
          is(Arrays.copyOfRange(smallBuff, 0, 128))
        );
        asyncAssertThat(context, result, is(testBuffer));

        stream.read(
          testBuffer,
          128,
          4096,
          context.asyncAssertSuccess(_result -> {
            // even though we ask for 4096 more bytes, it should stop at full length
            asyncAssertThat(context, result.length(), is(4096));
            asyncAssertThat(context, result.getBytes(), is(smallBuff));
            asyncAssertThat(context, result, is(testBuffer));

            stream.close(context.asyncAssertSuccess());
          })
        );
      })
    );
  }

  @Test
  public void testMultipleBufferRead(TestContext context) {
    AsyncInputStream stream = new AsyncInputStream(
      vertx,
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(largeBuff)
    );

    Buffer testBuffer = Buffer.buffer();
    // read halfway into first chunk
    stream.read(
      testBuffer,
      0,
      4096,
      context.asyncAssertSuccess(result -> {
        asyncAssertThat(context, result.length(), is(4096));
        asyncAssertThat(
          context,
          result.getBytes(),
          is(Arrays.copyOfRange(largeBuff, 0, 4096))
        );
        asyncAssertThat(context, result, is(testBuffer));

        // read from here to halfway into second chunk
        stream.read(
          testBuffer,
          4096,
          8192,
          context.asyncAssertSuccess(_result -> {
            asyncAssertThat(context, result.length(), is(4096 + 8192));
            asyncAssertThat(
              context,
              result.getBytes(),
              is(Arrays.copyOfRange(largeBuff, 0, 4096 + 8192))
            );

            // read from here to end + one extra chunk over
            stream.read(
              testBuffer,
              4096 + 8192,
              8192 * 2,
              context.asyncAssertSuccess(_result2 -> {
                asyncAssertThat(context, result.length(), is(largeBuff.length));
                asyncAssertThat(context, result.getBytes(), is(largeBuff));

                stream.close(context.asyncAssertSuccess());
              })
            );
          })
        );
      })
    );
  }

  @Test
  public void testSingleLargeBufferRead(TestContext context) {
    AsyncInputStream stream = new AsyncInputStream(
      vertx,
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(largeBuff)
    );

    Buffer testBuffer = Buffer.buffer();
    // read whole thing + some extra off the end
    stream.read(
      testBuffer,
      0,
      8192 * 3,
      context.asyncAssertSuccess(result -> {
        asyncAssertThat(context, result.length(), is(largeBuff.length));
        asyncAssertThat(context, result.getBytes(), is(largeBuff));
        asyncAssertThat(context, result, is(testBuffer));

        stream.close(context.asyncAssertSuccess());
      })
    );
  }

  @Test
  public void testSingleBufferSingleRead(TestContext context) {
    AsyncInputStream stream = new AsyncInputStream(
      vertx,
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(mediumBuff)
    );

    Buffer testBuffer = Buffer.buffer();
    stream.read(
      testBuffer,
      0,
      8192,
      context.asyncAssertSuccess(result -> {
        asyncAssertThat(context, result.length(), is(mediumBuff.length));
        asyncAssertThat(context, result.getBytes(), is(mediumBuff));
        asyncAssertThat(context, result, is(testBuffer));

        stream.read(
          testBuffer,
          8192,
          8192,
          context.asyncAssertSuccess(_result -> {
            // no change
            asyncAssertThat(context, result.length(), is(mediumBuff.length));
            asyncAssertThat(context, result.getBytes(), is(mediumBuff));
            asyncAssertThat(context, result, is(testBuffer));

            stream.close(context.asyncAssertSuccess());
          })
        );
      })
    );
  }

  @Test
  public void testSingleBufferMultiRead(TestContext context) {
    AsyncInputStream stream = new AsyncInputStream(
      vertx,
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(mediumBuff)
    );

    Buffer testBuffer = Buffer.buffer();
    stream.read(
      testBuffer,
      0,
      4096,
      context.asyncAssertSuccess(result -> {
        asyncAssertThat(context, result.length(), is(4096));
        asyncAssertThat(
          context,
          result.getBytes(),
          is(Arrays.copyOfRange(mediumBuff, 0, 4096))
        );
        asyncAssertThat(context, result, is(testBuffer));

        stream.read(
          testBuffer,
          0,
          8192,
          context.asyncAssertSuccess(_result -> {
            asyncAssertThat(context, result.length(), is(4096));
            asyncAssertThat(
              context,
              result.getBytes(),
              is(Arrays.copyOfRange(mediumBuff, 4096, 8192))
            );
            asyncAssertThat(context, result, is(testBuffer));

            stream.close(context.asyncAssertSuccess());
          })
        );
      })
    );
  }

  @Test
  public void testExceptionalRead() {
    AsyncInputStream stream = new AsyncInputStream(
      vertx,
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(emptyBuff)
    );

    Handler<AsyncResult<Buffer>> handler = result -> {};

    Buffer testBuffer = Buffer.buffer();

    assertThrows(
      NullPointerException.class,
      () -> stream.read(null, 0, 0, null)
    );
    assertThrows(
      NullPointerException.class,
      () -> stream.read(testBuffer, 0, 0, null)
    );
    assertThrows(
      IllegalArgumentException.class,
      () -> stream.read(testBuffer, -1, 0, handler)
    );
    // assertThrows(IllegalArgumentException.class, () -> stream.read(testBuffer, 0,
    // -1, 0, handler));
    assertThrows(
      IllegalArgumentException.class,
      () -> stream.read(testBuffer, 0, -1, handler)
    );

    stream.close();

    assertThrows(
      IllegalStateException.class,
      () -> stream.read(testBuffer, 0, 1, handler)
    );
  }

  @Test
  public void testHandlerEmpty(TestContext context) {
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(
      vertx,
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(emptyBuff)
    );

    stream.endHandler(v -> async.complete());
    stream.exceptionHandler(err -> context.fail(err));
    stream.handler(buff -> context.fail("No data should have been read"));
  }

  @Test
  public void testHandlerOutOfOrder(TestContext context) {
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(
      vertx,
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(emptyBuff)
    );

    stream.handler(buff -> context.fail("No data should have been read"));

    vertx.setTimer(
      100,
      _v -> {
        stream.endHandler(v ->
          context.fail(
            "End handler should not be called after stream is consumed"
          )
        );
        stream.exceptionHandler(err -> context.fail(err));

        // make sure neither are called, then complete
        vertx.setTimer(100, __v -> async.complete());
      }
    );
  }

  @Test
  @SuppressWarnings("java:S2699")
  public void testHandlerSmall(TestContext context) {
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(
      vertx,
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(smallBuff)
    );

    List<byte[]> receivedData = new ArrayList<>();

    stream.endHandler(v -> {
      asyncAssertThat(context, receivedData, hasSize(1));
      asyncAssertThat(context, receivedData.get(0), is(smallBuff));
      async.complete();
    });
    stream.exceptionHandler(err -> context.fail(err));
    stream.handler(buff -> receivedData.add(buff.getBytes()));
  }

  @Test
  @SuppressWarnings("java:S2699")
  public void testHandlerMedium(TestContext context) {
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(
      vertx,
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(mediumBuff)
    );

    List<byte[]> receivedData = new ArrayList<>();

    stream.exceptionHandler(err -> context.fail(err));
    stream.endHandler(v -> {
      asyncAssertThat(context, receivedData, hasSize(1));
      asyncAssertThat(context, receivedData.get(0), is(mediumBuff));
      async.complete();
    });
    stream.handler(buff -> receivedData.add(buff.getBytes()));
  }

  @Test
  @SuppressWarnings("java:S2699")
  public void testHandlerLarge(TestContext context) {
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(
      vertx,
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(largeBuff)
    );

    List<byte[]> receivedData = new ArrayList<>();

    stream.exceptionHandler(err -> context.fail(err));
    stream.endHandler(v -> {
      asyncAssertThat(context, receivedData, hasSize(3));
      asyncAssertThat(
        context,
        receivedData.get(0),
        is(Arrays.copyOfRange(largeBuff, 0, 8192))
      );
      asyncAssertThat(
        context,
        receivedData.get(1),
        is(Arrays.copyOfRange(largeBuff, 8192, 8192 * 2))
      );
      asyncAssertThat(
        context,
        receivedData.get(2),
        is(Arrays.copyOfRange(largeBuff, 8192 * 2, 8192 * 2 + 4096))
      );
      async.complete();
    });
    stream.handler(buff -> receivedData.add(buff.getBytes()));
  }

  @Test
  public void testHandlerPauseResume(TestContext context) {
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(
      vertx,
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(largeBuff)
    );

    List<byte[]> receivedData = new ArrayList<>();
    AtomicBoolean isPaused = new AtomicBoolean(false);

    stream.endHandler(v -> {
      asyncAssertThat(context, receivedData, hasSize(3));
      asyncAssertThat(
        context,
        receivedData.get(0),
        is(Arrays.copyOfRange(largeBuff, 0, 8192))
      );
      asyncAssertThat(
        context,
        receivedData.get(1),
        is(Arrays.copyOfRange(largeBuff, 8192, 8192 * 2))
      );
      asyncAssertThat(
        context,
        receivedData.get(2),
        is(Arrays.copyOfRange(largeBuff, 8192 * 2, 8192 * 2 + 4096))
      );
      async.complete();
    });

    stream.handler(buff -> {
      if (isPaused.get()) {
        context.fail("Should not have received data while paused");
      }

      receivedData.add(buff.getBytes());

      stream.pause();
      isPaused.set(true);

      vertx.setTimer(
        100,
        v -> {
          isPaused.set(false);
          stream.resume();
        }
      );
    });
  }

  @Test
  public void testHandlerPauseFetchResume(TestContext context) {
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(
      vertx,
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(largeBuff)
    );

    List<byte[]> receivedData = new ArrayList<>();
    AtomicBoolean isPaused = new AtomicBoolean(false);

    stream.endHandler(v -> {
      asyncAssertThat(context, receivedData, hasSize(3));
      asyncAssertThat(
        context,
        receivedData.get(0),
        is(Arrays.copyOfRange(largeBuff, 0, 8192))
      );
      asyncAssertThat(
        context,
        receivedData.get(1),
        is(Arrays.copyOfRange(largeBuff, 8192, 8192 * 2))
      );
      asyncAssertThat(
        context,
        receivedData.get(2),
        is(Arrays.copyOfRange(largeBuff, 8192 * 2, 8192 * 2 + 4096))
      );
      async.complete();
    });

    stream.handler(buff -> {
      if (isPaused.get()) {
        context.fail("Should not have received data while paused");
      }

      receivedData.add(buff.getBytes());

      stream.pause();
      isPaused.set(true);

      vertx.setTimer(
        100,
        v -> {
          isPaused.set(false);
          stream.fetch(4096); // should resume implicitly
        }
      );
    });
  }

  @Test
  public void testResumeClosed(TestContext context) {
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(
      vertx,
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(largeBuff)
    );

    AtomicBoolean isPaused = new AtomicBoolean(false);

    stream.handler(buff -> {
      if (isPaused.get()) {
        context.fail("Should not have received data while paused");
      }

      stream.pause();
      isPaused.set(true);
      stream.close();

      try {
        stream.resume();
        context.fail("Should not be able to resume closed stream");
      } catch (IllegalStateException e) {
        async.complete();
      }
    });
  }

  @Test
  @SuppressWarnings("java:S2699")
  public void testHandlerRemoval(TestContext context) {
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(
      vertx,
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(largeBuff)
    );

    List<byte[]> receivedData = new ArrayList<>();

    stream.endHandler(v -> {
      asyncAssertThat(context, receivedData, hasSize(1));
      asyncAssertThat(
        context,
        receivedData.get(0),
        is(Arrays.copyOfRange(largeBuff, 0, 8192))
      );
      async.complete();
    });

    stream.handler(buff -> {
      receivedData.add(buff.getBytes());

      // deregister handler; no more chunks should be sent and it should end
      stream.handler(null);
    });
  }

  @Test
  public void testBadContext(TestContext context) {
    Async async = context.async();

    // new Vertx.vertx() is not owner of the context used
    AsyncInputStream stream = new AsyncInputStream(
      Vertx.vertx(),
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(smallBuff)
    );

    stream.endHandler(v ->
      context.fail("Non-exception handlers should not work in invalid contexts")
    );
    stream.exceptionHandler(buff -> async.complete());
    stream.handler(buff ->
      context.fail("Non-exception handlers should not work in invalid contexts")
    );
  }

  @Test
  public void testBadContextNoHandler(TestContext context) {
    Async async = context.async();

    // new Vertx.vertx() is not owner of the context used
    AsyncInputStream stream = new AsyncInputStream(
      Vertx.vertx(),
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(emptyBuff)
    );

    stream.endHandler(v ->
      context.fail("Non-exception handlers should not work in invalid contexts")
    );

    stream.handler(buff ->
      context.fail("Non-exception handlers should not work in invalid contexts")
    );

    // make sure nothing happens, then complete
    vertx.setTimer(100, v -> async.complete());
  }

  @Test
  @SuppressWarnings("java:S2699")
  public void testNoEndHandler(TestContext context) {
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(
      vertx,
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(smallBuff)
    );

    List<byte[]> receivedData = new ArrayList<>();

    stream.handler(buff -> {
      receivedData.add(buff.getBytes());

      asyncAssertThat(context, receivedData, hasSize(1));
      asyncAssertThat(context, receivedData.get(0), is(smallBuff));
      async.complete();
    });
  }

  @Test
  public void testCloseFailure(TestContext context) {
    AsyncInputStream stream = new AsyncInputStream(
      vertx,
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(smallBuff) {
        @Override
        public void close() throws IOException {
          throw new IOException("test");
        }
      }
    );

    stream.close(context.asyncAssertFailure());
  }

  @Test
  public void testReadFailure(TestContext context) {
    AsyncInputStream stream = new AsyncInputStream(
      vertx,
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(smallBuff) {
        @Override
        public int read(byte b[], int off, int len) {
          // ByteArrayInputStream removes the throws declaration, so we can't use an
          // IOException here, even though that would be appropriate/expected
          throw new IllegalStateException("test");
        }
      }
    );

    stream.read(Buffer.buffer(), 0, 12, context.asyncAssertFailure());
  }

  @Test
  public void testReadFailureWithHandler(TestContext context) {
    Async async = context.async();

    AsyncInputStream stream = new AsyncInputStream(
      vertx,
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(smallBuff) {
        @Override
        public int read(byte b[], int off, int len) {
          // ByteArrayInputStream removes the throws declaration, so we can't use an
          // IOException here, even though that would be appropriate/expected
          throw new IllegalStateException("test");
        }
      }
    );

    stream.endHandler(v -> context.fail("Should not have received end"));
    stream.exceptionHandler(t -> async.complete());
    stream.handler(buff -> context.fail("Should not have received data"));
  }
}
