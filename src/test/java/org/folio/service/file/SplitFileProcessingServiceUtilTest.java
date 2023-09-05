package org.folio.service.file;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.buffer.impl.BufferImpl;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.HttpResponse;
import java.util.Arrays;
import org.folio.rest.jaxrs.model.Metadata;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class SplitFileProcessingServiceUtilTest
  extends SplitFileProcessingServiceAbstractTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testVerifyOkStatus() {
    HttpResponse<Buffer> testResponse = (HttpResponse<Buffer>) mock(
      HttpResponse.class
    );

    Buffer expectedBuffer = new BufferImpl();

    when(testResponse.bodyAsBuffer()).thenReturn(expectedBuffer);

    // successful cases
    Arrays
      .asList(200, 201, 204)
      .forEach(statusCode -> {
        when(testResponse.statusCode()).thenReturn(statusCode);

        assertThat(service.verifyOkStatus(testResponse), is(expectedBuffer));
      });

    // unsuccessful cases
    Arrays
      .asList(100, 400, 404, 422, 500)
      .forEach(statusCode -> {
        when(testResponse.statusCode()).thenReturn(statusCode);

        assertThrows(
          "Throws expected exception",
          IllegalStateException.class,
          () -> service.verifyOkStatus(testResponse)
        );
      });
  }

  @Test
  public void testUserIdFromMetadata() {
    assertThat(service.getUserIdFromMetadata(null), is(nullValue()));
    assertThat(
      service.getUserIdFromMetadata(new Metadata().withCreatedByUserId("foo")),
      is("foo")
    );
  }
}
