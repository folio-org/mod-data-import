package org.folio.service.file;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.HttpResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.ProcessFilesRqDto;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.service.file.SplitFileProcessingService.SplitFileInformation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

@RunWith(VertxUnitRunner.class)
public class SplitFileProcessingServiceStartJobTest
  extends SplitFileProcessingServiceAbstractTest {

  private static final Resource TEST_FILE = new ClassPathResource("10.mrc");
  private static final Resource TEST_EDIFACT_FILE = new ClassPathResource(
    "edifact/CornAuxAm.1605541205.edi"
  );

  @Before
  public void mockDao() {
    when(this.queueItemDao.addQueueItem(any()))
      .thenReturn(Future.succeededFuture("new-id"));
  }

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
      .splitFile("test-key", JOB_PROFILE_MARC)
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

  @Test
  public void testSplitNonMarcFile(TestContext context) throws IOException {
    when(minioStorageService.readFile("test-key"))
      .thenReturn(Future.succeededFuture(TEST_EDIFACT_FILE.getInputStream()));

    when(fileSplitService.splitFileFromS3(any(), any()))
      .thenReturn(
        Future.succeededFuture(Arrays.asList("result1", "result2", "result3"))
      );

    service
      .splitFile("test-key", JOB_PROFILE_EDIFACT)
      .onComplete(
        context.asyncAssertSuccess(result -> {
          assertThat(result.getKey(), is("test-key"));
          assertThat(result.getSplitKeys(), contains("test-key"));
          assertThat(result.getTotalRecords(), is(1));
        })
      );
  }

  @Test
  public void testSplitFileFailure(TestContext context) throws IOException {
    when(minioStorageService.readFile("test-key"))
      .thenReturn(
        Future.succeededFuture(
          new InputStream() {
            @Override
            public int read() throws IOException {
              throw new IOException();
            }
          }
        )
      );

    service
      .splitFile("test-key", JOB_PROFILE_MARC)
      .onComplete(context.asyncAssertFailure());
  }

  @Test
  public void testInitializeJob(TestContext context) {
    doAnswer(invocation ->
        Future.succeededFuture(
          SplitFileInformation
            .builder()
            .key(invocation.getArgument(0))
            .splitKeys(Arrays.asList("a1", "a2", "a3"))
            .totalRecords(10)
            .build()
        )
      )
      .doAnswer(invocation ->
        Future.succeededFuture(
          SplitFileInformation
            .builder()
            .key(invocation.getArgument(0))
            .splitKeys(Arrays.asList("b1"))
            .totalRecords(10)
            .build()
        )
      )
      .doAnswer(invocation ->
        Future.succeededFuture(
          SplitFileInformation
            .builder()
            .key(invocation.getArgument(0))
            .splitKeys(Arrays.asList("c1", "c2"))
            .totalRecords(10)
            .build()
        )
      )
      .when(service)
      .splitFile(any(), any());

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

    when(
      changeManagerClient.putChangeManagerJobExecutionsById(any(), any(), any())
    )
      .thenAnswer(invocation -> {
        assertThat(
          invocation.<JobExecution>getArgument(2).getTotalRecordsInFile(),
          is(10)
        );

        return getSuccessArBuffer(null);
      });

    service
      .initializeJob(
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
          ),
        changeManagerClient
      )
      .onComplete(
        context.asyncAssertSuccess(map -> {
          assertThat(map, is(aMapWithSize(3)));

          assertThat(map.get("key/file-1-key").getKey(), is("key/file-1-key"));
          assertThat(
            map.get("key/file-1-key").getJobExecution(),
            is(JOB_EXECUTION_1.withTotalRecordsInFile(10))
          );
          assertThat(
            map.get("key/file-1-key").getSplitKeys(),
            contains("a1", "a2", "a3")
          );
          assertThat(map.get("key/file-1-key").getTotalRecords(), is(10));

          assertThat(map.get("key/file-2-key").getKey(), is("key/file-2-key"));
          assertThat(
            map.get("key/file-2-key").getJobExecution(),
            is(JOB_EXECUTION_2.withTotalRecordsInFile(10))
          );
          assertThat(map.get("key/file-2-key").getSplitKeys(), contains("b1"));
          assertThat(map.get("key/file-2-key").getTotalRecords(), is(10));

          assertThat(map.get("key/file-3-key").getKey(), is("key/file-3-key"));
          assertThat(
            map.get("key/file-3-key").getJobExecution(),
            is(JOB_EXECUTION_3.withTotalRecordsInFile(10))
          );
          assertThat(
            map.get("key/file-3-key").getSplitKeys(),
            contains("c1", "c2")
          );
          assertThat(map.get("key/file-3-key").getTotalRecords(), is(10));

          verify(service, times(3)).splitFile(any(), any());
          verify(changeManagerClient, times(1))
            .postChangeManagerJobExecutions(any(), any());
        })
      );
  }

  @Test
  public void testInitializeChildren(TestContext context) {
    doReturn(
      CompositeFuture.all(
        Future.succeededFuture(JOB_EXECUTION_2),
        Future.succeededFuture(JOB_EXECUTION_3)
      )
    )
      .when(service)
      .registerSplitFileParts(
        any(),
        eq(JOB_EXECUTION_1),
        eq(JOB_PROFILE_INFO),
        eq(changeManagerClient),
        eq(10),
        any(),
        anyList()
      );

    when(fileProcessor.updateJobsProfile(any(), eq(JOB_PROFILE_INFO), any()))
      // child entities
      .thenAnswer(invocation -> {
        assertThat(
          invocation
            .<List<JobExecutionDto>>getArgument(0)
            .stream()
            .map(JobExecutionDto::getId)
            .collect(Collectors.toList()),
          containsInAnyOrder(JOB_EXECUTION_2.getId(), JOB_EXECUTION_3.getId())
        );
        return Future.succeededFuture();
      })
      // parent entity
      .thenAnswer(invocation -> {
        assertThat(
          invocation
            .<List<JobExecutionDto>>getArgument(0)
            .stream()
            .map(JobExecutionDto::getId)
            .collect(Collectors.toList()),
          containsInAnyOrder(JOB_EXECUTION_1.getId())
        );
        return Future.succeededFuture();
      });

    when(
      uploadDefinitionService.updateJobExecutionStatus(
        eq(JOB_EXECUTION_1.getId()),
        eq(new StatusDto().withStatus(StatusDto.Status.COMMIT_IN_PROGRESS)),
        any()
      )
    )
      .thenReturn(Future.succeededFuture(true));

    service
      .initializeChildren(
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
        changeManagerClient,
        new OkapiConnectionParams(Map.of("x-okapi-tenant", "tenant"), null),
        SplitFileInformation
          .builder()
          .key("key/file-1-key")
          .jobExecution(JOB_EXECUTION_1)
          .totalRecords(10)
          .splitKeys(Arrays.asList("a1", "a2"))
          .build()
      )
      .onComplete(
        context.asyncAssertSuccess(v -> {
          verify(service, times(1))
            .registerSplitFileParts(
              any(),
              eq(JOB_EXECUTION_1),
              eq(JOB_PROFILE_INFO),
              eq(changeManagerClient),
              eq(10),
              any(),
              anyList()
            );
          verify(fileProcessor, times(2))
            .updateJobsProfile(any(), eq(JOB_PROFILE_INFO), any());
          verify(uploadDefinitionService, times(1))
            .updateJobExecutionStatus(
              eq(JOB_EXECUTION_1.getId()),
              eq(
                new StatusDto().withStatus(StatusDto.Status.COMMIT_IN_PROGRESS)
              ),
              any()
            );

          verifyNoMoreInteractions(fileProcessor);
          verifyNoMoreInteractions(uploadDefinitionService);

          verify(queueItemDao, times(2)).addQueueItem(any());

          verifyNoMoreInteractions(queueItemDao);
        })
      );
  }

  @Test
  public void testInitializeChildrenFailure(TestContext context) {
    doReturn(CompositeFuture.all(new ArrayList<>()))
      .when(service)
      .registerSplitFileParts(
        any(),
        any(),
        any(),
        any(),
        anyInt(),
        any(),
        anyList()
      );

    when(fileProcessor.updateJobsProfile(any(), eq(JOB_PROFILE_INFO), any()))
      .thenReturn(Future.succeededFuture());

    when(uploadDefinitionService.updateJobExecutionStatus(any(), any(), any()))
      .thenReturn(Future.succeededFuture(false));

    service
      .initializeChildren(
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
        changeManagerClient,
        new OkapiConnectionParams(Map.of("x-okapi-tenant", "tenant"), null),
        SplitFileInformation
          .builder()
          .key("key/file-1-key")
          .jobExecution(JOB_EXECUTION_1)
          .totalRecords(10)
          .splitKeys(Arrays.asList("a1", "a2"))
          .build()
      )
      .onComplete(
        context.asyncAssertFailure(v -> verifyNoInteractions(queueItemDao))
      );
  }

  @Test
  public void testStartJob() {
    doReturn(
      Future.succeededFuture(
        Map.of(
          "key/file-1-key",
          SplitFileInformation
            .builder()
            .key("key/file-1-key")
            .splitKeys(Arrays.asList("a1", "a2", "a3"))
            .totalRecords(10)
            .jobExecution(JOB_EXECUTION_1)
            .build(),
          "key/file-2-key",
          SplitFileInformation
            .builder()
            .key("key/file-2-key")
            .splitKeys(Arrays.asList("b1"))
            .totalRecords(10)
            .jobExecution(JOB_EXECUTION_2)
            .build()
        )
      )
    )
      .when(service)
      .initializeJob(any(), eq(changeManagerClient));

    doReturn(Future.succeededFuture())
      .when(service)
      .initializeChildren(any(), eq(changeManagerClient), any(), any());

    when(uploadDefinitionService.updateBlocking(any(), any(), any()))
      .thenAnswer(v -> {
        assertThat(
          v
            .<Function<UploadDefinition, UploadDefinition>>getArgument(1)
            .apply(new UploadDefinition())
            .getStatus(),
          is(UploadDefinition.Status.COMPLETED)
        );

        return Future.succeededFuture();
      });

    service.startJob(
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
        ),
      changeManagerClient,
      new OkapiConnectionParams(Map.of("x-okapi-tenant", "tenant"), null)
    );
  }
}
