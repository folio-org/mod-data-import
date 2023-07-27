package org.folio.service.s3storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;

import java.util.ArrayList;
import java.util.List;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

import org.folio.rest.jaxrs.model.FileUploadInfo;
import org.folio.rest.persist.helpers.LocalRowSet;
import org.folio.s3.client.FolioS3Client;
import org.folio.s3.exception.S3ClientException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;

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
  public void testFirstPartSuccessful(TestContext context) {
    Async async = context.async();

    Mockito
      .when(folioS3Client.initiateMultipartUpload(anyString()))
      .thenReturn("upload-id");
    Mockito
      .when(
        folioS3Client.getPresignedMultipartUploadUrl(
          anyString(),
          eq("upload-id"),
          eq(1)
        )
      )
      .thenReturn("upload-url");

    Future<FileUploadInfo> result = minioStorageService.getFileUploadFirstPartUrl(
      "test-file",
      "test-tenant"
    );

    result.onFailure(_err ->
      context.fail("getFileUploadFirstPartUrl should not fail")
    );
    result.onSuccess(fileInfo -> {
      Mockito
        .verify(folioS3Client, times(1))
        .initiateMultipartUpload(fileInfo.getKey());
      Mockito
        .verify(folioS3Client, times(1))
        .getPresignedMultipartUploadUrl(fileInfo.getKey(), "upload-id", 1);
      Mockito.verifyNoMoreInteractions(folioS3Client);

      assertEquals(
        "Presigned URL is returned",
        "upload-url",
        fileInfo.getUrl()
      );
      assertEquals(
        "Upload ID is returned",
        "upload-id",
        fileInfo.getUploadId()
      );
      assertTrue(
        "Key format is correct",
        fileInfo.getKey().matches("^test-tenant/\\d*-test-file$")
      );
      async.complete();
    });
  }

  @Test
  public void testFirstPartFailure(TestContext context) {
    Async async = context.async();

    S3ClientException exception = new S3ClientException("test exception");

    Mockito
      .when(folioS3Client.initiateMultipartUpload(anyString()))
      .thenThrow(exception);

    Future<FileUploadInfo> result = minioStorageService.getFileUploadFirstPartUrl(
      "test-file",
      "test-tenant"
    );

    result.onSuccess(_result ->
      context.fail("getFileUploadFirstPartUrl should fail")
    );
    result.onFailure(err -> {
      Mockito
        .verify(folioS3Client, times(1))
        .initiateMultipartUpload(anyString());
      Mockito.verifyNoMoreInteractions(folioS3Client);

      assertSame("Fails with correct exception", exception, err);
      async.complete();
    });
  }

  @Test
  public void testFirstPartNestedFailure(TestContext context) {
    Async async = context.async();

    S3ClientException exception = new S3ClientException("test exception");

    Mockito
      .when(folioS3Client.initiateMultipartUpload(anyString()))
      .thenReturn("upload-id");
    Mockito
      .when(
        folioS3Client.getPresignedMultipartUploadUrl(
          anyString(),
          eq("upload-id"),
          eq(1)
        )
      )
      .thenThrow(exception);

    Future<FileUploadInfo> result = minioStorageService.getFileUploadFirstPartUrl(
      "test-file",
      "test-tenant"
    );

    result.onSuccess(_result ->
      context.fail("getFileUploadFirstPartUrl should fail")
    );
    result.onFailure(err -> {
      Mockito
        .verify(folioS3Client, times(1))
        .initiateMultipartUpload(anyString());
      Mockito
        .verify(folioS3Client, times(1))
        .getPresignedMultipartUploadUrl(anyString(), eq("upload-id"), eq(1));
      Mockito.verifyNoMoreInteractions(folioS3Client);

      assertSame("Fails with correct exception", exception, err);
      async.complete();
    });
  }

  @Test
  public void testLaterPartSuccessful(TestContext context) {
    Async async = context.async();

    Mockito
      .when(
        folioS3Client.getPresignedMultipartUploadUrl(
          "test-key",
          "upload-id",
          100
        )
      )
      .thenReturn("upload-url-100");

    Future<FileUploadInfo> result = minioStorageService.getFileUploadPartUrl(
      "test-key",
      "upload-id",
      100
    );

    result.onFailure(_err ->
      context.fail("getFileUploadPartUrl should not fail")
    );
    result.onSuccess(fileInfo -> {
      Mockito
        .verify(folioS3Client, times(1))
        .getPresignedMultipartUploadUrl(fileInfo.getKey(), "upload-id", 100);
      Mockito.verifyNoMoreInteractions(folioS3Client);

      assertEquals(
        "Presigned URL is returned",
        "upload-url-100",
        fileInfo.getUrl()
      );
      assertEquals(
        "Upload ID is returned",
        "upload-id",
        fileInfo.getUploadId()
      );
      assertEquals("Key did not change", "test-key", fileInfo.getKey());
      async.complete();
    });
  }
  @Test
  public void testAssembleSuccessful(TestContext context) {
    Async async = context.async();
    List<String> tags = List.of("etag1","etag2","etag3");
    
    doAnswer((InvocationOnMock invocation) -> {

      return null;
    }).when(
        folioS3Client
        ).completeMultipartUpload(
          "test-key",
          "upload-id",
          tags);


    
    Future<Boolean> result = minioStorageService.completeMultipartFileUpload(
        "test-key",
        "upload-id",
        tags);
    result.onFailure(_err -> {
      context.fail("completeMultipartFileUpload should not fail");
    
    });
  result.onSuccess(assembleResult -> {
    Mockito
    .verify(folioS3Client, times(1))
    .completeMultipartUpload("test-key", "upload-id", tags);
  Mockito.verifyNoMoreInteractions(folioS3Client);
  async.complete();

  
    });
  }

}
