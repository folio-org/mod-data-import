package org.folio.service.processing.kafka;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
//import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

//@ExtendWith({io.vertx.junit5.VertxExtension.class, MockitoExtension.class})
public class WriteStreamWrapperTest {
  @InjectMocks
  private WriteStreamWrapper wrapper;
  private KafkaProducer<String, String> kafkaProducer;
  private KafkaProducerRecord<String, String> data;
/*
  @BeforeEach
  public void onSetUp() {
    kafkaProducer = mock(KafkaProducer.class);
    wrapper = new WriteStreamWrapper(kafkaProducer);
    data = KafkaProducerRecord.create("testTopic", "key", "value", 2);
    data.addHeader("correlationId", "headerValue");
    data.addHeader("chunkNumber", "headerValue1");
  }

  @Test
  public void endShouldCallProducerEnd() {
    //given
    Future<Void> mockedFuture = mock(Future.class);
    when(kafkaProducer.end()).thenReturn(mockedFuture);
    //when
    wrapper.end();
    //then
    verify(kafkaProducer, times(1)).end();
  }

  @Test
  public void endWithArgsShouldCallProducerEndWithArgs() {
    //given
    Handler<AsyncResult<Void>> mockedHandler = mock(Handler.class);
    doNothing().when(kafkaProducer).end(mockedHandler);
    //when
    wrapper.end(mockedHandler);
    //then
    verify(kafkaProducer, times(1)).end(any(Handler.class));
  }

  @Test
  public void writeShouldCallProducerWrite() {
    //given
    Future<Void> mockedFuture = mock(Future.class);
    when(kafkaProducer.write(any())).thenReturn(mockedFuture);
    //when
    wrapper.write(data);
    //then
    verify(kafkaProducer, times(1)).write(any(), any());
  }

  @Test
  public void writeWith2ArgsShouldCallProducerWriteWith2Args() {
    //given
    Handler<AsyncResult<Void>> handler = mock(Handler.class);
    doAnswer(invocation -> {
      Handler<io.vertx.core.AsyncResult<Void>> callBackHandler = invocation.getArgument(1);
      callBackHandler.handle(Future.succeededFuture());
      return null;
    }).when(kafkaProducer).write(any(), any());
    //when
    wrapper.write(data,handler);
    //then
    verify(kafkaProducer, times(1)).write(any(), any());
  }

  @Test
  public void exceptionHandlerShouldCallProducerExceptionHandler() {
    //given
    Handler<Throwable> mockedHandler = mock(Handler.class);
    when(kafkaProducer.exceptionHandler(any())).thenReturn(kafkaProducer);
    //when
    wrapper.exceptionHandler(mockedHandler);
    //then
    verify(kafkaProducer, times(1)).exceptionHandler(any());
  }

  @Test
  public void setWriteQueueMaxSizeShouldCallProducerSetWriteQueueMaxSize() {
    //given
    int maxSize = 15;
    when(kafkaProducer.setWriteQueueMaxSize(maxSize)).thenReturn(kafkaProducer);
    //when
    wrapper.setWriteQueueMaxSize(maxSize);
    //then
    verify(kafkaProducer, times(1)).setWriteQueueMaxSize(maxSize);
  }

  @Test
  public void writeQueueFullShouldCallProducerWriteQueueFull() {
    //given
    boolean isFull = false;
    when(kafkaProducer.writeQueueFull()).thenReturn(isFull);
    //when
    boolean result = wrapper.writeQueueFull();
    //then
    Assertions.assertFalse(result);
    verify(kafkaProducer, times(1)).writeQueueFull();
  }

  @Test
  public void drainHandlerShouldCallProducerDrainHandler() {
    //given
    Handler<Void> handler = mock(Handler.class);
    when(kafkaProducer.drainHandler(any())).thenReturn(kafkaProducer);
    //when
    wrapper.drainHandler(handler);
    //then
    verify(kafkaProducer, times(1)).drainHandler(any());
  }*/
}
