package org.folio.service.file;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.HttpResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.ProcessFilesRqDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

@RunWith(VertxUnitRunner.class)
public class SplitFileProcessingServiceStartJobTest
  extends SplitFileProcessingServiceAbstractTest {

  private static final Resource TEST_FILE = new ClassPathResource("10.mrc");

  @Test
  public void testCreateParentJobExecutions(TestContext context) {
    doAnswer(invocation -> {
        InitJobExecutionsRqDto request = invocation.getArgument(0);
        Handler<AsyncResult<HttpResponse<Buffer>>> responseHandler = invocation.getArgument(
          1
        );

        assertThat(
          request
            .getFiles()
            .stream()
            .map(File::getName)
            .collect(Collectors.toList()),
          containsInAnyOrder(
            "key/file-1-key",
            "key/file-2-key",
            "key/file-3-key"
          )
        );
        assertThat(request.getJobProfileInfo(), is(JOB_PROFILE_INFO));
        assertThat(request.getUserId(), is("created-user-id"));

        responseHandler.handle(
          getSuccessArBuffer(
            new InitJobExecutionsRsDto()
              .withJobExecutions(
                Arrays.asList(JOB_EXECUTION_1, JOB_EXECUTION_2, JOB_EXECUTION_3)
              )
          )
        );

        return null;
      })
      .when(changeManagerClient)
      .postChangeManagerJobExecutions(any(), any());

    service
      .createParentJobExecutions(
        new ProcessFilesRqDto()
          .withJobProfileInfo(JOB_PROFILE_INFO)
          .withUploadDefinition(
            new UploadDefinition()
              .withFileDefinitions(
                Arrays.asList(
                  FILE_DEFINITION_1,
                  FILE_DEFINITION_2,
                  FILE_DEFINITION_3
                )
              )
              .withMetadata(METADATA)
          ),
        changeManagerClient
      )
      .onComplete(
        context.asyncAssertSuccess(result -> {
          assertThat(
            result,
            allOf(
              aMapWithSize(3),
              hasEntry("key/file-1-key", JOB_EXECUTION_1),
              hasEntry("key/file-2-key", JOB_EXECUTION_2),
              hasEntry("key/file-3-key", JOB_EXECUTION_3)
            )
          );

          verify(changeManagerClient, times(1))
            .postChangeManagerJobExecutions(any(), any());
        })
      );
  }

  @Test
  public void testCreateParentJobExecutionsFailure(TestContext context) {
    doAnswer(invocation -> {
        invocation
          .<Handler<AsyncResult<HttpResponse<Buffer>>>>getArgument(1)
          .handle(Future.failedFuture("test error"));
        return null;
      })
      .when(changeManagerClient)
      .postChangeManagerJobExecutions(any(), any());

    service
      .createParentJobExecutions(
        new ProcessFilesRqDto()
          .withJobProfileInfo(JOB_PROFILE_INFO)
          .withUploadDefinition(
            new UploadDefinition()
              .withFileDefinitions(
                Arrays.asList(
                  FILE_DEFINITION_1,
                  FILE_DEFINITION_2,
                  FILE_DEFINITION_3
                )
              )
              .withMetadata(METADATA)
          ),
        changeManagerClient
      )
      .onComplete(context.asyncAssertFailure());
  }

  @Test
  public void testSplitFile(TestContext context) throws IOException {
    when(minioStorageService.readFile("test-key"))
      .thenReturn(Future.succeededFuture(TEST_FILE.getInputStream()));

    when(fileSplitService.splitFileFromS3(any(), any()))
      .thenReturn(
        Future.succeededFuture(Arrays.asList("result1", "result2", "result3"))
      );

    service
      .splitFile("test-key")
      .onComplete(
        context.asyncAssertSuccess(result -> {
          assertThat(result.getKey(), is("test-key"));
          assertThat(
            result.getSplitKeys(),
            contains("result1", "result2", "result3")
          );
          assertThat(result.getTotalRecords(), is(10));
        })
      );
  }
}
