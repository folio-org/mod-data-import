package org.folio.service.file;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.folio.dao.DataImportQueueItemDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.service.auth.SystemUserAuthService;
import org.folio.service.file.S3JobRunningVerticle.QueueJob;
import org.folio.service.processing.ParallelFileChunkingProcessor;
import org.folio.service.processing.ranking.ScoreService;
import org.folio.service.s3storage.MinioStorageService;
import org.folio.service.upload.UploadDefinitionService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(VertxUnitRunner.class)
public class S3JobRunningVerticleUnitTest {

  protected static final Vertx vertx = Vertx.vertx();

  @Mock
  DataImportQueueItemDao queueItemDao;

  @Mock
  MinioStorageService minioStorageService;

  @Mock
  ScoreService scoreService;

  @Mock
  SystemUserAuthService systemUserService;

  @Mock
  UploadDefinitionService uploadDefinitionService;

  @Mock
  ParallelFileChunkingProcessor fileProcessor;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  S3JobRunningVerticle verticle;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);

    this.verticle =
      new S3JobRunningVerticle(
        vertx,
        queueItemDao,
        minioStorageService,
        scoreService,
        systemUserService,
        uploadDefinitionService,
        fileProcessor,
        100
      );
  }

  @Test
  public void testConnectionParams() {
    when(systemUserService.getAuthToken(any())).thenReturn("token");

    OkapiConnectionParams params = verticle.getConnectionParams(
      new DataImportQueueItem().withTenant("tenant").withOkapiUrl("okapi-url")
    );

    assertThat(params.getTenantId(), is("tenant"));
    assertThat(params.getOkapiUrl(), is("okapi-url"));
    assertThat(params.getToken(), is("token"));

    verify(systemUserService, times(1)).getAuthToken(any());

    verifyNoMoreInteractions(systemUserService);
  }

  @Test
  public void testDownloadFromS3Success(TestContext context)
    throws IOException {
    InputStream inputStream = spy(new ByteArrayInputStream(new byte[5]));
    when(minioStorageService.readFile("test-key"))
      .thenReturn(Future.succeededFuture(inputStream));

    File destFile = temporaryFolder.newFile();

    verticle
      .downloadFromS3(
        new QueueJob()
          .withFile(destFile)
          .withJobExecution(new JobExecution().withSourcePath("test-key"))
      )
      .onComplete(
        context.asyncAssertSuccess(result -> {
          try (InputStream reader = new FileInputStream(destFile)) {
            assertThat(reader.readAllBytes().length, is(5));

            // auto-closed by try-with-resources
            verify(inputStream, times(1)).close();
            verify(inputStream, times(1)).transferTo(any());
            verify(minioStorageService, times(1)).readFile("test-key");

            verifyNoMoreInteractions(minioStorageService);
            verifyNoMoreInteractions(inputStream);
          } catch (IOException e) {
            context.fail(e);
          }
        })
      );
  }
}
