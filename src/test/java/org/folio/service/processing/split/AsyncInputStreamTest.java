package org.folio.service.processing.split;

import static org.assertj.core.api.Assertions.assertThat;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class AsyncInputStreamTest {

  protected static Vertx vertx = Vertx.vertx();

  protected static byte[] emptyBuff = new byte[0];
  protected static byte[] smallBuff = new byte[8192 / 2];
  protected static byte[] mediumBuff = new byte[8192];
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
  @DisplayName("should call end handler and mark stream closed when stream is empty")
  void shouldCallEndHandler_andMarkClosed_whenStreamIsEmpty(VertxTestContext testContext) {
    AsyncInputStream stream = new AsyncInputStream(
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(emptyBuff)
    );

    stream.endHandler(v ->
      testContext.verify(() -> {
        assertThat(stream.closed()).isTrue();
        testContext.completeNow();
      })
    );
    stream.exceptionHandler(testContext::failNow);
    stream.handler(buff -> testContext.failNow(new AssertionError("No data should have been read")));

    stream.read();
  }

  @Test
  @DisplayName("should not call end or exception handler when handlers are set after stream is consumed")
  void shouldNotCallHandlers_whenSetAfterStreamConsumed(VertxTestContext testContext) {
    AsyncInputStream stream = new AsyncInputStream(
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(emptyBuff)
    );

    stream.handler(buff -> testContext.failNow(new AssertionError("No data should have been read")));

    stream.read();

    vertx.setTimer(
      100,
      l -> {
        stream.endHandler(v ->
          testContext.failNow(
            new AssertionError("End handler should not be called after stream is consumed")
          )
        );
        stream.exceptionHandler(testContext::failNow);

        vertx.setTimer(100, lng -> testContext.completeNow());
      }
    );
  }

  @Test
  @SuppressWarnings("java:S2699")
  @DisplayName("should deliver one chunk when stream has less than one buffer worth of data")
  void shouldDeliverOneChunk_whenStreamIsSmall(VertxTestContext testContext) {
    AsyncInputStream stream = new AsyncInputStream(
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(smallBuff)
    );

    List<byte[]> receivedData = new ArrayList<>();

    stream.endHandler(v ->
      testContext.verify(() -> {
        assertThat(receivedData).hasSize(1);
        assertThat(receivedData.getFirst()).isEqualTo(smallBuff);
        testContext.completeNow();
      })
    );
    stream.exceptionHandler(testContext::failNow);
    stream.handler(buff -> receivedData.add(buff.getBytes()));

    stream.read();
  }

  @Test
  @SuppressWarnings("java:S2699")
  @DisplayName("should deliver one chunk when stream size is exactly one buffer")
  void shouldDeliverOneChunk_whenStreamSizeIsOneBuffer(VertxTestContext testContext) {
    AsyncInputStream stream = new AsyncInputStream(
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(mediumBuff)
    );

    List<byte[]> receivedData = new ArrayList<>();

    stream.exceptionHandler(testContext::failNow);
    stream.endHandler(v ->
      testContext.verify(() -> {
        assertThat(receivedData).hasSize(1);
        assertThat(receivedData.getFirst()).isEqualTo(mediumBuff);
        testContext.completeNow();
      })
    );
    stream.handler(buff -> receivedData.add(buff.getBytes()));

    stream.read();
  }

  @Test
  @SuppressWarnings("java:S2699")
  @DisplayName("should deliver three chunks when stream has more than two buffers worth of data")
  void shouldDeliverThreeChunks_whenStreamIsLarge(VertxTestContext testContext) {
    AsyncInputStream stream = new AsyncInputStream(
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(largeBuff)
    );

    List<byte[]> receivedData = new ArrayList<>();

    stream.exceptionHandler(testContext::failNow);
    stream.endHandler(v ->
      testContext.verify(() -> {
        assertThat(receivedData).hasSize(3);
        assertThat(receivedData.getFirst()).isEqualTo(Arrays.copyOfRange(largeBuff, 0, 8192));
        assertThat(receivedData.get(1)).isEqualTo(Arrays.copyOfRange(largeBuff, 8192, 8192 * 2));
        assertThat(receivedData.get(2)).isEqualTo(Arrays.copyOfRange(largeBuff, 8192 * 2, 8192 * 2 + 4096));
        testContext.completeNow();
      })
    );
    stream.handler(buff -> receivedData.add(buff.getBytes()));

    stream.read();
  }

  @Test
  @DisplayName("should toggle active state when paused and resumed")
  void shouldToggleActiveState_whenPausedAndResumed(VertxTestContext testContext) {
    AsyncInputStream stream = new AsyncInputStream(
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(largeBuff)
    );

    stream.handler(buff -> {
      stream.pause();
      assertThat(stream.active()).isFalse();

      stream.resume();
      assertThat(stream.active()).isTrue();

      testContext.completeNow();
    });

    stream.read();
  }

  @Test
  @DisplayName("should not deliver data while paused and mark stream inactive when consumed after pause")
  void shouldNotDeliverData_whilePaused_andMarkInactiveWhenConsumed(VertxTestContext testContext) {
    AsyncInputStream stream = new AsyncInputStream(
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(smallBuff)
    );

    List<byte[]> receivedData = new ArrayList<>();
    AtomicBoolean isPaused = new AtomicBoolean(false);

    stream.handler(buff -> {
      if (isPaused.get()) {
        testContext.failNow(new AssertionError("Should not have received data while paused"));
        return;
      }

      receivedData.add(buff.getBytes());

      stream.pause();
      isPaused.set(true);

      vertx.setTimer(
        100,
        v -> {
          stream.resume();
          assertThat(stream.active()).isFalse();
          testContext.completeNow();
        }
      );
    });

    stream.read();
  }

  @Test
  @DisplayName("should stop delivering data when stream is closed after pause")
  void shouldStopDeliveringData_whenStreamIsClosedAfterPause(VertxTestContext testContext) {
    AsyncInputStream stream = new AsyncInputStream(
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(largeBuff)
    );

    AtomicBoolean isPaused = new AtomicBoolean(false);

    stream.handler(buff ->
      testContext.verify(() -> {
        if (isPaused.get()) {
          testContext.failNow(new AssertionError("Should not have received data while paused"));
          return;
        }

        assertThat(stream.active()).isTrue();

        stream.pause();
        assertThat(stream.active()).isFalse();

        isPaused.set(true);

        assertThat(stream.closed()).isFalse();
        stream.close();
        assertThat(stream.closed()).isTrue();

        stream.resume();
        stream.read();
        assertThat(stream.active()).isFalse();

        vertx.setTimer(100L, vv -> testContext.completeNow());
      })
    );

    stream.read();
  }

  @Test
  @SuppressWarnings("java:S2699")
  @DisplayName("should deliver only the first chunk when handler is removed after first chunk")
  void shouldDeliverOnlyFirstChunk_whenHandlerRemovedAfterFirstChunk(VertxTestContext testContext) {
    AsyncInputStream stream = new AsyncInputStream(
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(largeBuff)
    );

    List<byte[]> receivedData = new ArrayList<>();

    stream.endHandler(v ->
      testContext.verify(() -> {
        assertThat(receivedData).hasSize(1);
        assertThat(receivedData.getFirst()).isEqualTo(Arrays.copyOfRange(largeBuff, 0, 8192));
        testContext.completeNow();
      })
    );

    stream.handler(buff -> {
      receivedData.add(buff.getBytes());
      stream.handler(null);
    });

    stream.read();
  }

  @Test
  @DisplayName("should call both end handler and exception handler when close throws IOException")
  void shouldCallEndAndExceptionHandlers_whenCloseThrowsIOException(VertxTestContext testContext) {
    AsyncInputStream stream = new AsyncInputStream(
      vertx.getOrCreateContext(),
      new ByteArrayInputStream(smallBuff) {
        @Override
        public void close() throws IOException {
          throw new IOException("test");
        }
      }
    );

    var checkpoint = testContext.checkpoint(2);

    stream.endHandler(v -> checkpoint.flag());
    stream.exceptionHandler(t -> checkpoint.flag());
    stream.handler(buff -> testContext.failNow(new AssertionError("Should not have received data")));

    stream.close();
  }

  @Test
  @DisplayName("should call both end handler and exception handler when read throws IOException")
  void shouldCallEndAndExceptionHandlers_whenReadThrowsIOException(VertxTestContext testContext) {
    AsyncInputStream stream = new AsyncInputStream(
      vertx.getOrCreateContext(),
      new InputStream() {
        @Override
        public int read() throws IOException {
          throw new IOException("test");
        }
      }
    );

    var checkpoint = testContext.checkpoint(2);

    stream.endHandler(v -> checkpoint.flag());
    stream.exceptionHandler(t -> checkpoint.flag());
    stream.handler(buff -> testContext.failNow(new AssertionError("Should not have received data")));

    stream.read();
  }
}
