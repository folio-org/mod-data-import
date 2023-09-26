package org.folio.service.s3storage;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isA;
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

import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import javax.ws.rs.NotFoundException;
import org.apache.commons.io.IOUtils;
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

  private static final String S3_TEST_KEY = "data-import/test-key";

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

    minioStorageService
      .getFileUploadFirstPartUrl("test-file", "test-tenant")
      .onComplete(
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
            fileInfo
              .getKey()
              .matches("^data-import/test-tenant/\\d+-test-file$")
          );
        })
      );
  }

  @Test
  public void testFirstPartFailure(TestContext context) {
    S3ClientException exception = new S3ClientException("test exception");

    when(folioS3Client.initiateMultipartUpload(anyString()))
      .thenThrow(exception);

    minioStorageService
      .getFileUploadFirstPartUrl(S3_TEST_KEY, "test-tenant")
      .onComplete(
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

    minioStorageService
      .getFileUploadFirstPartUrl(S3_TEST_KEY, "test-tenant")
      .onComplete(
        context.asyncAssertFailure(err -> {
          verify(folioS3Client, times(1)).initiateMultipartUpload(anyString());
          verify(folioS3Client, times(1))
            .getPresignedMultipartUploadUrl(
              anyString(),
              eq("upload-id"),
              eq(1)
            );
          Mockito.verifyNoMoreInteractions(folioS3Client);

          assertSame("Fails with correct exception", exception, err);
        })
      );
  }

  @Test
  public void testLaterPartSuccessful(TestContext context) {
    when(
      folioS3Client.getPresignedMultipartUploadUrl(
        S3_TEST_KEY,
        "upload-id",
        100
      )
    )
      .thenReturn("upload-url-100");

    minioStorageService
      .getFileUploadPartUrl(S3_TEST_KEY, "upload-id", 100)
      .onComplete(
        context.asyncAssertSuccess(fileInfo -> {
          verify(folioS3Client, times(1))
            .getPresignedMultipartUploadUrl(
              fileInfo.getKey(),
              "upload-id",
              100
            );
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
          assertEquals("Key did not change", S3_TEST_KEY, fileInfo.getKey());
        })
      );
  }

  @Test
  public void testReadFileSuccessful(TestContext context) {
    String testData = "Testing";
    InputStream sampleDataStream = new ByteArrayInputStream(
      testData.getBytes(StandardCharsets.UTF_8)
    );

    Mockito.doReturn(sampleDataStream).when(folioS3Client).read(S3_TEST_KEY);

    minioStorageService
      .readFile(S3_TEST_KEY)
      .onComplete(
        context.asyncAssertSuccess(inStream -> {
          Mockito.verify(folioS3Client, times(1)).read(S3_TEST_KEY);
          Mockito.verifyNoMoreInteractions(folioS3Client);

          try {
            assertEquals(
              "Proper test data is returned",
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

    Mockito.doThrow(exception).when(folioS3Client).read(S3_TEST_KEY);

    minioStorageService
      .readFile(S3_TEST_KEY)
      .onComplete(
        context.asyncAssertFailure(err -> {
          Mockito.verify(folioS3Client, times(1)).read(S3_TEST_KEY);
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

    doReturn(S3_TEST_KEY)
      .when(folioS3Client)
      .write(S3_TEST_KEY, sampleDataStream);

    minioStorageService
      .write(S3_TEST_KEY, sampleDataStream)
      .onComplete(
        context.asyncAssertSuccess(path -> {
          verify(folioS3Client, times(1)).write(S3_TEST_KEY, sampleDataStream);
          Mockito.verifyNoMoreInteractions(folioS3Client);

          assertEquals("Correct path is returned", S3_TEST_KEY, path);
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

    doThrow(exception).when(folioS3Client).write(S3_TEST_KEY, sampleDataStream);

    minioStorageService
      .write(S3_TEST_KEY, sampleDataStream)
      .onComplete(
        context.asyncAssertFailure(err -> {
          verify(folioS3Client, times(1)).write(S3_TEST_KEY, sampleDataStream);
          Mockito.verifyNoMoreInteractions(folioS3Client);
          assertSame("Fails with correct exception", exception, err);
        })
      );
  }

  @Test
  public void testRemoveFileSuccessful(TestContext context) throws IOException {
    Mockito.doReturn(S3_TEST_KEY).when(folioS3Client).remove(S3_TEST_KEY);

    minioStorageService
      .remove(S3_TEST_KEY)
      .onComplete(
        context.asyncAssertSuccess(_v -> {
          Mockito.verify(folioS3Client, times(1)).remove(S3_TEST_KEY);
          Mockito.verifyNoMoreInteractions(folioS3Client);
        })
      );
  }

  @Test
  public void testRemoveFileFailure(TestContext context) throws IOException {
    S3ClientException exception = new S3ClientException("test exception");

    Mockito.doThrow(exception).when(folioS3Client).remove(S3_TEST_KEY);

    minioStorageService
      .remove(S3_TEST_KEY)
      .onComplete(
        context.asyncAssertFailure(err -> {
          Mockito.verify(folioS3Client, times(1)).remove(S3_TEST_KEY);
          Mockito.verifyNoMoreInteractions(folioS3Client);
          assertSame("Fails with correct exception", exception, err);
        })
      );
  }

  @Test
  public void testDownloadPresignedSuccessful(TestContext context) {
    when(folioS3Client.getPresignedUrl(S3_TEST_KEY)).thenReturn("download-url");
    when(folioS3Client.list(S3_TEST_KEY))
      .thenReturn(Arrays.asList(S3_TEST_KEY));

    minioStorageService
      .getFileDownloadUrl(S3_TEST_KEY)
      .onComplete(
        context.asyncAssertSuccess(fileInfo -> {
          verify(folioS3Client, times(1)).getPresignedUrl(S3_TEST_KEY);
          verify(folioS3Client, times(1)).list(S3_TEST_KEY);
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
  public void testDownloadPresignedSuccessfulWithSimilarFiles(
    TestContext context
  ) {
    when(folioS3Client.getPresignedUrl(S3_TEST_KEY)).thenReturn("download-url");
    when(folioS3Client.list(S3_TEST_KEY))
      .thenReturn(
        Arrays.asList(S3_TEST_KEY + "A", S3_TEST_KEY + "B", S3_TEST_KEY)
      );

    minioStorageService
      .getFileDownloadUrl(S3_TEST_KEY)
      .onComplete(
        context.asyncAssertSuccess(fileInfo -> {
          verify(folioS3Client, times(1)).getPresignedUrl(S3_TEST_KEY);
          verify(folioS3Client, times(1)).list(S3_TEST_KEY);
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
  public void testDownloadPresignedKeyNotPresentWithSimilarFiles(
    TestContext context
  ) {
    when(folioS3Client.getPresignedUrl(S3_TEST_KEY)).thenReturn("download-url");
    when(folioS3Client.list(S3_TEST_KEY))
      .thenReturn(Arrays.asList(S3_TEST_KEY + "A", S3_TEST_KEY + "B"));

    minioStorageService
      .getFileDownloadUrl(S3_TEST_KEY)
      .onComplete(
        context.asyncAssertFailure(err -> {
          verify(folioS3Client, times(1)).getPresignedUrl(S3_TEST_KEY);
          verify(folioS3Client, times(1)).list(S3_TEST_KEY);
          Mockito.verifyNoMoreInteractions(folioS3Client);

          assertThat(err, isA(NotFoundException.class));
        })
      );
  }

  @Test
  public void testDownloadPresignedKeyNotPresentWithNoMatch(
    TestContext context
  ) {
    when(folioS3Client.getPresignedUrl(S3_TEST_KEY)).thenReturn("download-url");
    when(folioS3Client.list(S3_TEST_KEY))
      .thenReturn(Arrays.asList(S3_TEST_KEY + "A", S3_TEST_KEY + "B"));

    minioStorageService
      .getFileDownloadUrl(S3_TEST_KEY)
      .onComplete(
        context.asyncAssertFailure(err -> {
          verify(folioS3Client, times(1)).getPresignedUrl(S3_TEST_KEY);
          verify(folioS3Client, times(1)).list(S3_TEST_KEY);
          Mockito.verifyNoMoreInteractions(folioS3Client);

          assertThat(err, isA(NotFoundException.class));
        })
      );
  }

  @Test
  public void testDownloadPresignedS3Failure(TestContext context) {
    when(folioS3Client.list(S3_TEST_KEY))
      .thenThrow(new S3ClientException("test exception"));

    minioStorageService
      .getFileDownloadUrl(S3_TEST_KEY)
      .onComplete(
        context.asyncAssertFailure(err -> {
          verify(folioS3Client, times(1)).list(S3_TEST_KEY);
          Mockito.verifyNoMoreInteractions(folioS3Client);

          assertThat(err, isA(S3ClientException.class));
        })
      );
  }

  @Test
  public void testIncorrectPrefix(TestContext context) throws IOException {
    minioStorageService
      .getFileDownloadUrl("not/prefixed/correctly")
      .onComplete(context.asyncAssertFailure());

    minioStorageService
      .getFileUploadPartUrl("not/prefixed/correctly", "upload-id", 2)
      .onComplete(context.asyncAssertFailure());

    minioStorageService
      .write("not/prefixed/correctly", new ByteArrayInputStream(new byte[1]))
      .onComplete(context.asyncAssertFailure());

    minioStorageService
      .remove("not/prefixed/correctly")
      .onComplete(context.asyncAssertFailure());
  }
}
