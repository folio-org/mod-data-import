package org.folio.service.split;

import static org.folio.util.VertxMatcherAssert.asyncAssertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.folio.service.processing.split.AsyncInputStream;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import lombok.extern.log4j.Log4j2;

@Log4j2
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
  protected static byte[] largeBuff = new byte[8192 * 2
      + 8192 / 2];

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
    log.info("testEmptyRead");
    AsyncInputStream stream = new AsyncInputStream(vertx, vertx.getOrCreateContext(),
        new ByteArrayInputStream(emptyBuff));

    Buffer testBuffer = Buffer.buffer();
    stream.read(testBuffer, 0, 128, context.asyncAssertSuccess(result -> {
      asyncAssertThat(context, result.length(), is(0));

      asyncAssertThat(context, result, is(testBuffer));
      stream.close(context.asyncAssertSuccess());
    }));
  }

  @Test
  public void testSmallFullRead(TestContext context) {
    log.info("testSmallFullRead");
    AsyncInputStream stream = new AsyncInputStream(vertx, vertx.getOrCreateContext(),
        new ByteArrayInputStream(smallBuff));

    Buffer testBuffer = Buffer.buffer();
    stream.read(testBuffer, 0, 8192, context.asyncAssertSuccess(result -> {
      // stops at full stream being consumed
      asyncAssertThat(context, result.length(), is(4096));

      asyncAssertThat(context, result, is(testBuffer));
      stream.close(context.asyncAssertSuccess());
    }));
  }

  @Test
  public void testSmallMultiRead(TestContext context) {
    log.info("testSmallMultiRead");
    AsyncInputStream stream = new AsyncInputStream(vertx, vertx.getOrCreateContext(),
        new ByteArrayInputStream(smallBuff));

    Buffer testBuffer = Buffer.buffer();
    stream.read(testBuffer, 0, 128, context.asyncAssertSuccess(result -> {
      asyncAssertThat(context, result.length(), is(128));
      asyncAssertThat(context, result.getBytes(), is(Arrays.copyOfRange(smallBuff, 0, 128)));
      asyncAssertThat(context, result, is(testBuffer));

      stream.read(testBuffer, 128, 4096, context.asyncAssertSuccess(_result -> {
        // even though we ask for 4096 more bytes, it should stop at full length
        asyncAssertThat(context, result.length(), is(4096));
        asyncAssertThat(context, result.getBytes(), is(smallBuff));
        asyncAssertThat(context, result, is(testBuffer));

        stream.close(context.asyncAssertSuccess());
      }));
    }));
  }

  @Test
  public void testMultipleBufferRead(TestContext context) {
    log.info("testMultipleBufferRead");
    AsyncInputStream stream = new AsyncInputStream(vertx, vertx.getOrCreateContext(),
        new ByteArrayInputStream(largeBuff));

    Buffer testBuffer = Buffer.buffer();
    // read halfway into first chunk
    stream.read(testBuffer, 0, 4096, context.asyncAssertSuccess(result -> {
      asyncAssertThat(context, result.length(), is(4096));
      asyncAssertThat(context, result.getBytes(), is(Arrays.copyOfRange(largeBuff, 0, 4096)));
      asyncAssertThat(context, result, is(testBuffer));

      // read from here to halfway into second chunk
      stream.read(testBuffer, 4096, 8192, context.asyncAssertSuccess(_result -> {
        asyncAssertThat(context, result.length(), is(4096 + 8192));
        asyncAssertThat(context, result.getBytes(), is(Arrays.copyOfRange(largeBuff, 0, 4096 + 8192)));

        // read from here to end + one extra chunk over
        stream.read(testBuffer, 4096 + 8192, 8192 * 2, context.asyncAssertSuccess(_result2 -> {
          asyncAssertThat(context, result.length(), is(largeBuff.length));
          asyncAssertThat(context, result.getBytes(), is(largeBuff));

          stream.close(context.asyncAssertSuccess());
        }));
      }));
    }));
  }

  @Test
  public void testSingleLargeBufferRead(TestContext context) {
    log.info("testSingleLargeBufferRead");
    AsyncInputStream stream = new AsyncInputStream(vertx, vertx.getOrCreateContext(),
        new ByteArrayInputStream(largeBuff));

    Buffer testBuffer = Buffer.buffer();
    // read whole thing + some extra off the end
    stream.read(testBuffer, 0, 8192 * 3, context.asyncAssertSuccess(result -> {
      asyncAssertThat(context, result.length(), is(largeBuff.length));
      asyncAssertThat(context, result.getBytes(), is(largeBuff));
      asyncAssertThat(context, result, is(testBuffer));

      stream.close(context.asyncAssertSuccess());
    }));
  }

  @Test
  public void testSingleBufferSingleRead(TestContext context) {
    log.info("testSingleBufferSingleRead");
    AsyncInputStream stream = new AsyncInputStream(vertx, vertx.getOrCreateContext(),
        new ByteArrayInputStream(mediumBuff));

    Buffer testBuffer = Buffer.buffer();
    stream.read(testBuffer, 0, 8192, context.asyncAssertSuccess(result -> {
      asyncAssertThat(context, result.length(), is(mediumBuff.length));
      asyncAssertThat(context, result.getBytes(), is(mediumBuff));
      asyncAssertThat(context, result, is(testBuffer));

      stream.read(testBuffer, 8192, 8192, context.asyncAssertSuccess(_result -> {
        // no change
        asyncAssertThat(context, result.length(), is(mediumBuff.length));
        asyncAssertThat(context, result.getBytes(), is(mediumBuff));
        asyncAssertThat(context, result, is(testBuffer));

        stream.close(context.asyncAssertSuccess());
      }));
    }));
  }

  @Test
  public void testSingleBufferMultiRead(TestContext context) {
    log.info("testSingleBufferMultiRead");
    AsyncInputStream stream = new AsyncInputStream(vertx, vertx.getOrCreateContext(),
        new ByteArrayInputStream(mediumBuff));

    Buffer testBuffer = Buffer.buffer();
    stream.read(testBuffer, 0, 4096, context.asyncAssertSuccess(result -> {
      asyncAssertThat(context, result.length(), is(4096));
      asyncAssertThat(context, result.getBytes(), is(Arrays.copyOfRange(mediumBuff, 0, 4096)));
      asyncAssertThat(context, result, is(testBuffer));

      stream.read(testBuffer, 0, 8192, context.asyncAssertSuccess(_result -> {
        asyncAssertThat(context, result.length(), is(4096));
        asyncAssertThat(context, result.getBytes(), is(Arrays.copyOfRange(mediumBuff, 4096, 8192)));
        asyncAssertThat(context, result, is(testBuffer));

        stream.close(context.asyncAssertSuccess());
      }));
    }));
  }

  @Test
  public void testExceptionalRead() {
    log.info("testExceptionalRead");
    AsyncInputStream stream = new AsyncInputStream(vertx, vertx.getOrCreateContext(),
        new ByteArrayInputStream(emptyBuff));

    Handler<AsyncResult<Buffer>> handler = result -> {
    };

    Buffer testBuffer = Buffer.buffer();

    assertThrows(NullPointerException.class, () -> stream.read(null, 0, 0, null));
    assertThrows(NullPointerException.class, () -> stream.read(testBuffer, 0, 0, null));
    assertThrows(IllegalArgumentException.class, () -> stream.read(testBuffer, -1, 0, handler));
    // assertThrows(IllegalArgumentException.class, () -> stream.read(testBuffer, 0,
    // -1, 0, handler));
    assertThrows(IllegalArgumentException.class, () -> stream.read(testBuffer, 0, -1, handler));

    stream.close();

    assertThrows(IllegalStateException.class, () -> stream.read(testBuffer, 0, 1, handler));
  }

  @Test
  public void testHandlerEmpty(TestContext context) {
    log.info("testHandlerEmpty");
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(vertx, vertx.getOrCreateContext(),
        new ByteArrayInputStream(emptyBuff));

    stream.handler(buff -> context.fail("No data should have been read"));
    stream.endHandler(v -> async.complete());
    stream.exceptionHandler(err -> context.fail(err));
  }

  @Test
  @SuppressWarnings("java:S2699")
  public void testHandlerSmall(TestContext context) {
    log.info("testHandlerSmall");
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(vertx, vertx.getOrCreateContext(),
        new ByteArrayInputStream(smallBuff));

    List<byte[]> receivedData = new ArrayList<>();

    stream.handler(buff -> receivedData.add(buff.getBytes()));
    stream.endHandler(v -> {
      asyncAssertThat(context, receivedData, hasSize(1));
      asyncAssertThat(context, receivedData.get(0), is(smallBuff));
      async.complete();
    });
  }

  @Test
  @SuppressWarnings("java:S2699")
  public void testHandlerMedium(TestContext context) {
    log.info("testHandlerMedium");
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(vertx, vertx.getOrCreateContext(),
        new ByteArrayInputStream(mediumBuff));

    List<byte[]> receivedData = new ArrayList<>();

    stream.handler(buff -> receivedData.add(buff.getBytes()));
    stream.endHandler(v -> {
      asyncAssertThat(context, receivedData, hasSize(1));
      asyncAssertThat(context, receivedData.get(0), is(mediumBuff));
      async.complete();
    });
  }

  @Test
  @SuppressWarnings("java:S2699")
  public void testHandlerLarge(TestContext context) {
    log.info("testHandlerLarge");
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(vertx, vertx.getOrCreateContext(),
        new ByteArrayInputStream(largeBuff));

    List<byte[]> receivedData = new ArrayList<>();

    stream.handler(buff -> receivedData.add(buff.getBytes()));
    stream.endHandler(v -> {
      asyncAssertThat(context, receivedData, hasSize(3));
      asyncAssertThat(context, receivedData.get(0), is(Arrays.copyOfRange(largeBuff, 0, 8192)));
      asyncAssertThat(context, receivedData.get(1), is(Arrays.copyOfRange(largeBuff, 8192, 8192 * 2)));
      asyncAssertThat(context, receivedData.get(2), is(Arrays.copyOfRange(largeBuff, 8192 * 2, 8192 * 2 + 4096)));
      async.complete();
    });
  }

  @Test
  public void testHandlerPauseResume(TestContext context) {
    log.info("testHandlerPauseResume");
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(vertx, vertx.getOrCreateContext(),
        new ByteArrayInputStream(largeBuff));

    List<byte[]> receivedData = new ArrayList<>();
    AtomicBoolean isPaused = new AtomicBoolean(false);

    stream.handler(buff -> {
      if (isPaused.get()) {
        context.fail("Should not have received data while paused");
      }

      receivedData.add(buff.getBytes());

      stream.pause();
      isPaused.set(true);

      vertx.setTimer(100, v -> {
        isPaused.set(false);
        stream.resume();
      });
    });

    stream.endHandler(v -> {
      asyncAssertThat(context, receivedData, hasSize(3));
      asyncAssertThat(context, receivedData.get(0), is(Arrays.copyOfRange(largeBuff, 0, 8192)));
      asyncAssertThat(context, receivedData.get(1), is(Arrays.copyOfRange(largeBuff, 8192, 8192 * 2)));
      asyncAssertThat(context, receivedData.get(2), is(Arrays.copyOfRange(largeBuff, 8192 * 2, 8192 * 2 + 4096)));
      async.complete();
    });
  }

  @Test
  public void testHandlerPauseFetchResume(TestContext context) {
    log.info("testHandlerPauseFetchResume");
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(vertx, vertx.getOrCreateContext(),
        new ByteArrayInputStream(largeBuff));

    List<byte[]> receivedData = new ArrayList<>();
    AtomicBoolean isPaused = new AtomicBoolean(false);

    stream.handler(buff -> {
      if (isPaused.get()) {
        context.fail("Should not have received data while paused");
      }

      receivedData.add(buff.getBytes());

      stream.pause();
      isPaused.set(true);

      vertx.setTimer(100, v -> {
        isPaused.set(false);
        stream.fetch(4096); // should resume implicitly
      });
    });

    stream.endHandler(v -> {
      asyncAssertThat(context, receivedData, hasSize(3));
      asyncAssertThat(context, receivedData.get(0), is(Arrays.copyOfRange(largeBuff, 0, 8192)));
      asyncAssertThat(context, receivedData.get(1), is(Arrays.copyOfRange(largeBuff, 8192, 8192 * 2)));
      asyncAssertThat(context, receivedData.get(2), is(Arrays.copyOfRange(largeBuff, 8192 * 2, 8192 * 2 + 4096)));
      async.complete();
    });
  }

  @Test
  public void testResumeClosed(TestContext context) {
    log.info("testResumeClosed");
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(vertx, vertx.getOrCreateContext(),
        new ByteArrayInputStream(largeBuff));

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
    log.info("testHandlerRemoval");
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(vertx, vertx.getOrCreateContext(),
        new ByteArrayInputStream(largeBuff));

    List<byte[]> receivedData = new ArrayList<>();

    stream.handler(buff -> {
      receivedData.add(buff.getBytes());

      // deregister handler; no more chunks should be sent and it should end
      stream.handler(null);
    });

    stream.endHandler(v -> {
      asyncAssertThat(context, receivedData, hasSize(1));
      asyncAssertThat(context, receivedData.get(0), is(Arrays.copyOfRange(largeBuff, 0, 8192)));
      async.complete();
    });
  }

  @Test
  public void testBadContext(TestContext context) {
    log.info("testBadContext");
    Async async = context.async();

    // new Vertx.vertx() is not owner of the context used
    AsyncInputStream stream = new AsyncInputStream(Vertx.vertx(),
        vertx.getOrCreateContext(),
        new ByteArrayInputStream(smallBuff));

    stream.handler(buff -> context.fail("Non-exception handlers should not work in invalid contexts"));
    stream.endHandler(v -> context.fail("Non-exception handlers should not work in invalid contexts"));
    stream.exceptionHandler(buff -> async.complete());
  }

  @Test
  public void testBadContextNoHandler(TestContext context) {
    log.info("testBadContextNoHandler");
    Async async = context.async();

    // new Vertx.vertx() is not owner of the context used
    AsyncInputStream stream = new AsyncInputStream(Vertx.vertx(),
        vertx.getOrCreateContext(),
        new ByteArrayInputStream(emptyBuff));

    stream.handler(buff -> context.fail("Non-exception handlers should not work in invalid contexts"));
    stream.endHandler(v -> context.fail("Non-exception handlers should not work in invalid contexts"));

    // make sure nothing happens, then complete
    vertx.setTimer(100, v -> async.complete());
  }

  @Test
  @SuppressWarnings("java:S2699")
  public void testNoEndHandler(TestContext context) {
    log.info("testNoEndHandler");
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(vertx, vertx.getOrCreateContext(),
        new ByteArrayInputStream(smallBuff));

    List<byte[]> receivedData = new ArrayList<>();

    stream.handler(buff -> {
      receivedData.add(buff.getBytes());

      asyncAssertThat(context, receivedData, hasSize(1));
      asyncAssertThat(context, receivedData.get(0), is(smallBuff));
      async.complete();
    });
  }
}
