package org.folio.service.file;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.buffer.impl.BufferImpl;
import io.vertx.ext.web.client.HttpResponse;
import java.util.Arrays;
import org.folio.rest.jaxrs.model.Metadata;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SplitFileProcessingServiceUtilTest extends SplitFileProcessingServiceAbstractTest {

  @DisplayName("should return buffer when response status is 2xx")
  @Test
  @SuppressWarnings("unchecked")
  void shouldReturnBuffer_whenResponseStatusIs2xx() {
    HttpResponse<Buffer> testResponse = (HttpResponse<Buffer>) mock(HttpResponse.class);
    Buffer expectedBuffer = new BufferImpl();
    when(testResponse.bodyAsBuffer()).thenReturn(expectedBuffer);

    Arrays.asList(200, 201, 204).forEach(statusCode -> {
      when(testResponse.statusCode()).thenReturn(statusCode);
      assertThat(service.verifyOkStatus(testResponse)).isEqualTo(expectedBuffer);
    });
  }

  @DisplayName("should throw IllegalStateException when response status is not 2xx")
  @Test
  @SuppressWarnings("unchecked")
  void shouldThrowIllegalStateException_whenResponseStatusIsNot2xx() {
    HttpResponse<Buffer> testResponse = (HttpResponse<Buffer>) mock(HttpResponse.class);

    Arrays.asList(100, 400, 404, 422, 500).forEach(statusCode -> {
      when(testResponse.statusCode()).thenReturn(statusCode);
      assertThatThrownBy(() -> service.verifyOkStatus(testResponse))
        .isInstanceOf(IllegalStateException.class);
    });
  }

  @DisplayName("should extract user ID from metadata or return null when metadata is null")
  @Test
  void shouldExtractUserId_orReturnNull_whenMetadataIsAbsent() {
    assertThat(service.getUserIdFromMetadata(null)).isNull();
    assertThat(service.getUserIdFromMetadata(new Metadata().withCreatedByUserId("foo"))).isEqualTo("foo");
  }
}
