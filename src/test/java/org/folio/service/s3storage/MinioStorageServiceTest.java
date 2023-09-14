package org.folio.service.s3storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.folio.rest.jaxrs.model.FileDownloadInfo;
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

  private static final String S3_FILE_KEY = "test-key";

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

    when(folioS3ClientFactory.getFolioS3Client()).thenReturn(folioS3Client);
  }

  @Test
  public void testFirstPartSuccessful(TestContext context) {
    when(folioS3Client.initiateMultipartUpload(anyString()))
      .thenReturn("upload-id");
    when(
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

    result.onComplete(
      context.asyncAssertSuccess(fileInfo -> {
        verify(folioS3Client, times(1))
          .initiateMultipartUpload(fileInfo.getKey());
        verify(folioS3Client, times(1))
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
      })
    );
  }

  @Test
  public void testFirstPartFailure(TestContext context) {
    S3ClientException exception = new S3ClientException("test exception");

    when(folioS3Client.initiateMultipartUpload(anyString()))
      .thenThrow(exception);

    Future<FileUploadInfo> result = minioStorageService.getFileUploadFirstPartUrl(
      "test-file",
      "test-tenant"
    );

    result.onComplete(
      context.asyncAssertFailure(err -> {
        verify(folioS3Client, times(1)).initiateMultipartUpload(anyString());
        Mockito.verifyNoMoreInteractions(folioS3Client);

        assertSame("Fails with correct exception", exception, err);
      })
    );
  }

  @Test
  public void testFirstPartNestedFailure(TestContext context) {
    S3ClientException exception = new S3ClientException("test exception");

    when(folioS3Client.initiateMultipartUpload(anyString()))
      .thenReturn("upload-id");
    when(
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

    result.onComplete(
      context.asyncAssertFailure(err -> {
        verify(folioS3Client, times(1)).initiateMultipartUpload(anyString());
        verify(folioS3Client, times(1))
          .getPresignedMultipartUploadUrl(anyString(), eq("upload-id"), eq(1));
        Mockito.verifyNoMoreInteractions(folioS3Client);

        assertSame("Fails with correct exception", exception, err);
      })
    );
  }

  @Test
  public void testLaterPartSuccessful(TestContext context) {
    when(
      folioS3Client.getPresignedMultipartUploadUrl("test-key", "upload-id", 100)
    )
      .thenReturn("upload-url-100");

    Future<FileUploadInfo> result = minioStorageService.getFileUploadPartUrl(
      "test-key",
      "upload-id",
      100
    );

    result.onComplete(
      context.asyncAssertSuccess(fileInfo -> {
        verify(folioS3Client, times(1))
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
      })
    );
  }

  @Test
  public void testReadFileSuccessful(TestContext context) {
    String testData = "Testing";
    InputStream sampleDataStream = new ByteArrayInputStream(
      testData.getBytes(StandardCharsets.UTF_8)
    );

    Mockito.doReturn(sampleDataStream).when(folioS3Client).read(S3_FILE_KEY);

    Future<InputStream> result = minioStorageService.readFile(S3_FILE_KEY);

    result.onComplete(
      context.asyncAssertSuccess(inStream -> {
        Mockito.verify(folioS3Client, times(1)).read(S3_FILE_KEY);
        Mockito.verifyNoMoreInteractions(folioS3Client);

        try {
          assertEquals(
            testData,
            IOUtils.toString(inStream, StandardCharsets.UTF_8)
          );
        } catch (IOException e) {
          context.fail("testReadFileSuccessful should not fail");
        }
      })
    );
  }

  @Test
  public void testReadFileFailure(TestContext context) {
    S3ClientException exception = new S3ClientException("test exception");

    Mockito.doThrow(exception).when(folioS3Client).read(S3_FILE_KEY);

    Future<InputStream> result = minioStorageService.readFile(S3_FILE_KEY);

    result.onComplete(
      context.asyncAssertFailure(err -> {
        Mockito.verify(folioS3Client, times(1)).read(S3_FILE_KEY);
        Mockito.verifyNoMoreInteractions(folioS3Client);
        assertSame("Fails with correct exception", exception, err);
      })
    );
  }

  @Test
  public void testWriteFileSuccessful(TestContext context) throws IOException {
    String testData = "Testing";
    InputStream sampleDataStream = new ByteArrayInputStream(
      testData.getBytes(StandardCharsets.UTF_8)
    );

    doReturn(S3_FILE_KEY)
      .when(folioS3Client)
      .write(S3_FILE_KEY, sampleDataStream);

    Future<String> result = minioStorageService.write(
      S3_FILE_KEY,
      sampleDataStream
    );

    result.onComplete(
      context.asyncAssertSuccess(path -> {
        verify(folioS3Client, times(1)).write(S3_FILE_KEY, sampleDataStream);
        Mockito.verifyNoMoreInteractions(folioS3Client);

        assertEquals(S3_FILE_KEY, path);
      })
    );
  }

  @Test
  public void testWriteFileFailure(TestContext context) throws IOException {
    String testData = "Testing";
    InputStream sampleDataStream = new ByteArrayInputStream(
      testData.getBytes(StandardCharsets.UTF_8)
    );

    S3ClientException exception = new S3ClientException("test exception");

    doThrow(exception).when(folioS3Client).write(S3_FILE_KEY, sampleDataStream);

    Future<String> result = minioStorageService.write(
      S3_FILE_KEY,
      sampleDataStream
    );

    result.onComplete(
      context.asyncAssertFailure(err -> {
        verify(folioS3Client, times(1)).write(S3_FILE_KEY, sampleDataStream);
        Mockito.verifyNoMoreInteractions(folioS3Client);
        assertSame("Fails with correct exception", exception, err);
      })
    );
  }

  @Test
  public void testRemoveFileSuccessful(TestContext context) throws IOException {
    Mockito.doReturn(S3_FILE_KEY).when(folioS3Client).remove(S3_FILE_KEY);

    Future<Void> result = minioStorageService.remove(S3_FILE_KEY);

    result.onComplete(
      context.asyncAssertSuccess(_v -> {
        Mockito.verify(folioS3Client, times(1)).remove(S3_FILE_KEY);
        Mockito.verifyNoMoreInteractions(folioS3Client);
      })
    );
  }

  @Test
  public void testRemoveFileFailure(TestContext context) throws IOException {
    S3ClientException exception = new S3ClientException("test exception");

    Mockito.doThrow(exception).when(folioS3Client).remove(S3_FILE_KEY);

    Future<Void> result = minioStorageService.remove(S3_FILE_KEY);

    result.onComplete(
      context.asyncAssertFailure(err -> {
        Mockito.verify(folioS3Client, times(1)).remove(S3_FILE_KEY);
        Mockito.verifyNoMoreInteractions(folioS3Client);
        assertSame("Fails with correct exception", exception, err);
      })
    );
  }

  @Test
  public void testDownloadPresignedSuccessful(TestContext context) {
    when(folioS3Client.getPresignedUrl("test-file")).thenReturn("download-url");

    Future<FileDownloadInfo> result = minioStorageService.getFileDownloadUrl(
      "test-file"
    );

    result.onComplete(
      context.asyncAssertSuccess(fileInfo -> {
        verify(folioS3Client, times(1)).getPresignedUrl("test-file");
        Mockito.verifyNoMoreInteractions(folioS3Client);

        assertEquals(
          "Presigned URL is returned",
          "download-url",
          fileInfo.getUrl()
        );
      })
    );
  }

  @Test
  public void testDownloadPresignedFailure(TestContext context) {
    when(folioS3Client.getPresignedUrl("test-file"))
      .thenThrow(new RuntimeException());

    Future<FileDownloadInfo> result = minioStorageService.getFileDownloadUrl(
      "test-file"
    );

    result.onComplete(
      context.asyncAssertFailure(_err -> {
        verify(folioS3Client, times(1)).getPresignedUrl("test-file");
        Mockito.verifyNoMoreInteractions(folioS3Client);
      })
    );
  }
}
