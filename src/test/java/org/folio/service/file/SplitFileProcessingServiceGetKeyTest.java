package org.folio.service.file;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.junit5.VertxTestContext;
import org.folio.rest.jaxrs.model.JobExecution;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SplitFileProcessingServiceGetKeyTest extends SplitFileProcessingServiceAbstractTest {

  @DisplayName("should return source path as key from job execution")
  @Test
  void shouldReturnSourcePathAsKey_fromJobExecution(VertxTestContext testContext) {
    when(uploadDefinitionService.getJobExecutionById(anyString(), any()))
      .thenReturn(Future.succeededFuture(new JobExecution().withSourcePath("key")));

    service.getKey("id", null).onComplete(
      testContext.succeeding(result -> testContext.verify(() -> {
        assertThat(result).isEqualTo("key");
        verify(uploadDefinitionService, times(1)).getJobExecutionById("id", null);
        verifyNoMoreInteractions(uploadDefinitionService);
        testContext.completeNow();
      }))
    );
  }
}
