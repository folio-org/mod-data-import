package org.folio.service.file;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.jaxrs.model.JobExecution;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class SplitFileProcessingServiceGetKeyTest
  extends SplitFileProcessingServiceAbstractTest {

  @Test
  public void testGetKey(TestContext context) {
    when(uploadDefinitionService.getJobExecutionById(anyString(), any()))
      .thenReturn(
        Future.succeededFuture(new JobExecution().withSourcePath("key"))
      );

    service
      .getKey("id", null)
      .onComplete(
        context.asyncAssertSuccess(result -> {
          assertThat(result, is("key"));

          verify(uploadDefinitionService, times(1))
            .getJobExecutionById("id", null);
          verifyNoMoreInteractions(uploadDefinitionService);
        })
      );
  }
}
