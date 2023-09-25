package org.folio.service.split;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
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
  public void testHandlerEmpty(TestContext context) {
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(emptyBuff)
    );

    stream.endHandler(v ->
      context.verify(vv -> {
        async.complete();
        assertThat(stream.closed(), is(true));
      })
    );
    stream.exceptionHandler(err -> context.fail(err));
    stream.handler(buff -> context.fail("No data should have been read"));

    stream.read();
  }

  @Test
  public void testHandlerOutOfOrder(TestContext context) {
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(emptyBuff)
    );

    stream.handler(buff -> context.fail("No data should have been read"));

    stream.read();

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
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(smallBuff)
    );

    List<byte[]> receivedData = new ArrayList<>();

    stream.endHandler(v ->
      context.verify(_v -> {
        assertThat(receivedData, hasSize(1));
        assertThat(receivedData, hasSize(1));
        assertThat(receivedData.get(0), is(smallBuff));
        assertThat(receivedData.get(0), is(smallBuff));
        async.complete();
      })
    );
    stream.exceptionHandler(err -> context.fail(err));
    stream.handler(buff -> receivedData.add(buff.getBytes()));

    stream.read();
  }

  @Test
  @SuppressWarnings("java:S2699")
  public void testHandlerMedium(TestContext context) {
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(mediumBuff)
    );

    List<byte[]> receivedData = new ArrayList<>();

    stream.exceptionHandler(err -> context.fail(err));
    stream.endHandler(v ->
      context.verify(_v -> {
        assertThat(receivedData, hasSize(1));
        assertThat(receivedData, hasSize(1));
        assertThat(receivedData.get(0), is(mediumBuff));
        assertThat(receivedData.get(0), is(mediumBuff));
        async.complete();
      })
    );
    stream.handler(buff -> receivedData.add(buff.getBytes()));

    stream.read();
  }

  @Test
  @SuppressWarnings("java:S2699")
  public void testHandlerLarge(TestContext context) {
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(largeBuff)
    );

    List<byte[]> receivedData = new ArrayList<>();

    stream.exceptionHandler(err -> context.fail(err));
    stream.endHandler(v ->
      context.verify(_v -> {
        assertThat(receivedData, hasSize(3));
        assertThat(receivedData, hasSize(3));
        assertThat(
          receivedData.get(0),
          is(Arrays.copyOfRange(largeBuff, 0, 8192))
        );
        assertThat(
          receivedData.get(1),
          is(Arrays.copyOfRange(largeBuff, 8192, 8192 * 2))
        );
        assertThat(
          receivedData.get(2),
          is(Arrays.copyOfRange(largeBuff, 8192 * 2, 8192 * 2 + 4096))
        );
        async.complete();
      })
    );
    stream.handler(buff -> receivedData.add(buff.getBytes()));

    stream.read();
  }

  @Test
  public void testPauseResume(TestContext context) {
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(largeBuff)
    );

    stream.handler(buff -> {
      stream.pause();
      assertThat(stream.active(), is(false));

      stream.resume();
      assertThat(stream.active(), is(true));

      async.complete();
    });

    stream.read();
  }

  @Test
  public void testPauseFetchResumeForConsumed(TestContext context) {
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(smallBuff)
    );

    List<byte[]> receivedData = new ArrayList<>();
    AtomicBoolean isPaused = new AtomicBoolean(false);

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
          // consumed
          stream.resume();
          assertThat(stream.active(), is(false));

          async.complete();
        }
      );
    });

    stream.read();
  }

  @Test
  public void testResumeClosed(TestContext context) {
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(largeBuff)
    );

    AtomicBoolean isPaused = new AtomicBoolean(false);

    stream.handler(buff ->
      context.verify(v -> {
        if (isPaused.get()) {
          context.fail("Should not have received data while paused");
        }

        assertThat(stream.active(), is(true));

        stream.pause();
        assertThat(stream.active(), is(false));

        isPaused.set(true);

        assertThat(stream.closed(), is(false));
        stream.close();
        assertThat(stream.closed(), is(true));

        // stays paused after closure
        stream.resume();
        stream.read();
        assertThat(stream.active(), is(false));

        // give time for additional chunks to be read,
        // to ensure no more are
        vertx.setTimer(100L, vv -> async.complete());
      })
    );

    stream.read();
  }

  @Test
  @SuppressWarnings("java:S2699")
  public void testHandlerRemoval(TestContext context) {
    Async async = context.async();
    AsyncInputStream stream = new AsyncInputStream(
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(largeBuff)
    );

    List<byte[]> receivedData = new ArrayList<>();

    stream.endHandler(v ->
      context.verify(_v -> {
        assertThat(receivedData, hasSize(1));
        assertThat(receivedData, hasSize(1));
        assertThat(
          receivedData.get(0),
          is(Arrays.copyOfRange(largeBuff, 0, 8192))
        );
        async.complete();
      })
    );

    stream.handler(buff -> {
      receivedData.add(buff.getBytes());

      // deregister handler; no more chunks should be sent and it should end
      stream.handler(null);
    });

    stream.read();
  }

  // @Test
  // public void testBadContextNoHandler(TestContext context) {
  //   Async async = context.async();

  //   // new Vertx.vertx() is not owner of the context used
  //   AsyncInputStream stream = new AsyncInputStream(
  //     Vertx.vertx(),
  //     vertx.getOrCreateContext(),
  //     new ByteArrayInputStream(emptyBuff)
  //   );

  //   stream.endHandler(v ->
  //     context.fail("Non-exception handlers should not work in invalid contexts")
  //   );

  //   stream.handler(buff ->
  //     context.fail("Non-exception handlers should not work in invalid contexts")
  //   );

  //   // make sure nothing happens, then complete
  //   vertx.setTimer(100, v -> async.complete());
  // }

  // @Test
  // @SuppressWarnings("java:S2699")
  // public void testNoEndHandler(TestContext context) {
  //   Async async = context.async();
  //   AsyncInputStream stream = new AsyncInputStream(
  //     vertx.getOrCreateContext(),
  //     new ByteArrayInputStream(smallBuff)
  //   );

  //   List<byte[]> receivedData = new ArrayList<>();

  //   stream.handler(buff ->
  //     context.verify(v -> {
  //       receivedData.add(buff.getBytes());

  //       assertThat(receivedData, hasSize(1));
  //       assertThat(receivedData, hasSize(1));
  //       assertThat(receivedData.get(0), is(smallBuff));
  //       assertThat(receivedData.get(0), is(smallBuff));
  //       async.complete();
  //     })
  //   );
  // }

  @Test
  public void testCloseFailure(TestContext context) {
    AsyncInputStream stream = new AsyncInputStream(
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(smallBuff) {
        @Override
        public void close() throws IOException {
          throw new IOException("test");
        }
      }
    );

    Async async = context.strictAsync(2);

    stream.endHandler(v -> async.countDown());
    stream.exceptionHandler(t -> async.countDown());
    stream.handler(buff -> context.fail("Should not have received data"));

    stream.close();
  }

  @Test
  public void testReadFailure(TestContext context) {
    AsyncInputStream stream = new AsyncInputStream(
      vertx.getOrCreateContext(),
      new InputStream() {
        @Override
        public int read() throws IOException {
          throw new IOException("test");
        }
      }
    );

    Async async = context.strictAsync(2);

    stream.endHandler(v -> async.countDown());
    stream.exceptionHandler(t -> async.countDown());
    stream.handler(buff -> context.fail("Should not have received data"));

    stream.read();
  }
}
