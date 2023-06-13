package org.folio.service.s3storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;

import io.minio.http.Method;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.jaxrs.model.FileUploadInfo;
import org.folio.s3.client.FolioS3Client;
import org.folio.s3.exception.S3ClientException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

@RunWith(VertxUnitRunner.class)
public class MinioStorageServiceTest {

  private final Vertx vertx = Vertx.vertx();

  @Mock
  private FolioS3ClientFactory folioS3ClientFactory;

  @Mock
  private FolioS3Client folioS3Client;

  private MinioStorageService minioStorageService;

  @Before
  public void setUp(TestContext context) {
    vertx.exceptionHandler((err -> context.fail(err)));

    MockitoAnnotations.openMocks(this);
    this.minioStorageService =
      new MinioStorageServiceImpl(folioS3ClientFactory, vertx);

    Mockito
      .when(folioS3ClientFactory.getFolioS3Client())
      .thenReturn(folioS3Client);
  }

  @Test
  public void testSuccessful(TestContext context) {
    Async async = context.async();

    Mockito
      .when(folioS3Client.getPresignedUrl(anyString(), eq(Method.PUT)))
      .thenReturn("url");

    Future<FileUploadInfo> result = minioStorageService.getFileUploadUrl(
      "test-file",
      "test-tenant"
    );

    result.onFailure(_err -> context.fail("getFileUploadUrl should not fail"));
    result.onSuccess(fileInfo -> {
      Mockito.verify(folioS3ClientFactory, times(1)).getFolioS3Client();
      Mockito
        .verify(folioS3Client, times(1))
        .getPresignedUrl(fileInfo.getKey(), Method.PUT);
      Mockito.verifyNoMoreInteractions(folioS3ClientFactory);
      Mockito.verifyNoMoreInteractions(folioS3Client);

      assertEquals("Presigned URL is returned", "url", fileInfo.getUrl());
      assertTrue(
        "Key format is correct",
        fileInfo.getKey().matches("^test-tenant/\\d*-test-file$")
      );
      async.complete();
    });
  }

  @Test
  public void testFailure(TestContext context) {
    Async async = context.async();

    S3ClientException exception = new S3ClientException("test exception");

    Mockito
      .when(folioS3Client.getPresignedUrl(anyString(), eq(Method.PUT)))
      .thenThrow(exception);

    Future<FileUploadInfo> result = minioStorageService.getFileUploadUrl(
      "test-file",
      "test-tenant"
    );

    result.onSuccess(_result -> context.fail("getFileUploadUrl should fail"));
    result.onFailure(err -> {
      Mockito.verify(folioS3ClientFactory, times(1)).getFolioS3Client();
      Mockito
        .verify(folioS3Client, times(1))
        .getPresignedUrl(anyString(), eq(Method.PUT));
      Mockito.verifyNoMoreInteractions(folioS3ClientFactory);
      Mockito.verifyNoMoreInteractions(folioS3Client);

      assertSame("Fails with getPresignedUrl exception", exception, err);
      async.complete();
    });
  }
}
