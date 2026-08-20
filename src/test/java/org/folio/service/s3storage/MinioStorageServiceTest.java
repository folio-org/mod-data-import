package org.folio.service.s3storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.NotFoundException;
import org.apache.commons.io.IOUtils;
import org.folio.s3.client.FolioS3Client;
import org.folio.s3.exception.S3ClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class MinioStorageServiceTest {

  private static final String S3_TEST_KEY = "data-import/test-key";
  private final Vertx vertx = Vertx.vertx();
  @Mock
  private FolioS3ClientFactory folioS3ClientFactory;

  @Mock
  private FolioS3Client folioS3Client;

  private MinioStorageService minioStorageService;

  @BeforeEach
  void setUp() {
    this.minioStorageService = new MinioStorageServiceImpl(folioS3ClientFactory, vertx);
    when(folioS3ClientFactory.getFolioS3Client()).thenReturn(folioS3Client);
  }

  @DisplayName("should return presigned upload URL and upload ID when initiating first part upload")
  @Test
  void shouldReturnPresignedUrlAndUploadId_whenInitiatingFirstPartUpload(VertxTestContext testContext) {
    when(folioS3Client.initiateMultipartUpload(anyString())).thenReturn("upload-id");
    when(folioS3Client.getPresignedMultipartUploadUrl(anyString(), eq("upload-id"), eq(1)))
      .thenReturn("upload-url");

    minioStorageService
      .getFileUploadFirstPartUrl("test-file", "test-tenant")
      .onComplete(
        testContext.succeeding(fileInfo ->
          testContext.verify(() -> {
            verify(folioS3Client, times(1)).initiateMultipartUpload(fileInfo.getKey());
            verify(folioS3Client, times(1)).getPresignedMultipartUploadUrl(fileInfo.getKey(), "upload-id", 1);
            verifyNoMoreInteractions(folioS3Client);

            assertThat(fileInfo.getUrl()).as("Presigned URL is returned").isEqualTo("upload-url");
            assertThat(fileInfo.getUploadId()).as("Upload ID is returned").isEqualTo("upload-id");
            assertThat(fileInfo.getKey()).as("Key format is correct")
              .matches("^data-import/test-tenant/\\d+-test-file$");

            testContext.completeNow();
          })
        )
      );
  }

  @DisplayName("should fail with S3ClientException when initiateMultipartUpload throws")
  @Test
  void shouldFail_withS3ClientException_whenInitiateMultipartUploadThrows(VertxTestContext testContext) {
    S3ClientException exception = new S3ClientException("test exception");
    when(folioS3Client.initiateMultipartUpload(anyString())).thenThrow(exception);

    minioStorageService
      .getFileUploadFirstPartUrl(S3_TEST_KEY, "test-tenant")
      .onComplete(
        testContext.failing(err ->
          testContext.verify(() -> {
            verify(folioS3Client, times(1)).initiateMultipartUpload(anyString());
            verifyNoMoreInteractions(folioS3Client);

            assertThat(err).as("Fails with correct exception").isSameAs(exception);

            testContext.completeNow();
          })
        )
      );
  }

  @DisplayName("should fail with S3ClientException when getPresignedMultipartUploadUrl throws")
  @Test
  void shouldFail_withS3ClientException_whenGetPresignedUrlThrows(VertxTestContext testContext) {
    S3ClientException exception = new S3ClientException("test exception");
    when(folioS3Client.initiateMultipartUpload(anyString())).thenReturn("upload-id");
    when(folioS3Client.getPresignedMultipartUploadUrl(anyString(), eq("upload-id"), eq(1)))
      .thenThrow(exception);

    minioStorageService
      .getFileUploadFirstPartUrl(S3_TEST_KEY, "test-tenant")
      .onComplete(
        testContext.failing(err ->
          testContext.verify(() -> {
            verify(folioS3Client, times(1)).initiateMultipartUpload(anyString());
            verify(folioS3Client, times(1)).getPresignedMultipartUploadUrl(anyString(), eq("upload-id"), eq(1));
            verifyNoMoreInteractions(folioS3Client);

            assertThat(err).as("Fails with correct exception").isSameAs(exception);

            testContext.completeNow();
          })
        )
      );
  }

  @DisplayName("should return presigned URL and unchanged key when getting later part upload URL")
  @Test
  void shouldReturnPresignedUrl_andUnchangedKey_whenGettingLaterPartUrl(VertxTestContext testContext) {
    when(folioS3Client.getPresignedMultipartUploadUrl(S3_TEST_KEY, "upload-id", 100))
      .thenReturn("upload-url-100");

    minioStorageService
      .getFileUploadPartUrl(S3_TEST_KEY, "upload-id", 100)
      .onComplete(
        testContext.succeeding(fileInfo ->
          testContext.verify(() -> {
            verify(folioS3Client, times(1)).getPresignedMultipartUploadUrl(fileInfo.getKey(), "upload-id", 100);
            verifyNoMoreInteractions(folioS3Client);

            assertThat(fileInfo.getUrl()).as("Presigned URL is returned").isEqualTo("upload-url-100");
            assertThat(fileInfo.getUploadId()).as("Upload ID is returned").isEqualTo("upload-id");
            assertThat(fileInfo.getKey()).as("Key did not change").isEqualTo(S3_TEST_KEY);

            testContext.completeNow();
          })
        )
      );
  }

  @DisplayName("should return input stream with correct data when reading file successfully")
  @Test
  void shouldReturnInputStream_withCorrectData_whenReadingFileSuccessfully(VertxTestContext testContext) {
    String testData = "Testing";
    InputStream sampleDataStream = new ByteArrayInputStream(testData.getBytes(StandardCharsets.UTF_8));
    doReturn(sampleDataStream).when(folioS3Client).read(S3_TEST_KEY);

    minioStorageService
      .readFile(S3_TEST_KEY)
      .onComplete(
        testContext.succeeding(inStream ->
          testContext.verify(() -> {
            verify(folioS3Client, times(1)).read(S3_TEST_KEY);
            verifyNoMoreInteractions(folioS3Client);

            try {
              assertThat(IOUtils.toString(inStream, StandardCharsets.UTF_8))
                .as("Proper test data is returned")
                .isEqualTo(testData);
            } catch (IOException e) {
              testContext.failNow(e);
              return;
            }

            testContext.completeNow();
          })
        )
      );
  }

  @DisplayName("should fail with S3ClientException when read throws")
  @Test
  void shouldFail_withS3ClientException_whenReadThrows(VertxTestContext testContext) {
    S3ClientException exception = new S3ClientException("test exception");
    doThrow(exception).when(folioS3Client).read(S3_TEST_KEY);

    minioStorageService
      .readFile(S3_TEST_KEY)
      .onComplete(
        testContext.failing(err ->
          testContext.verify(() -> {
            verify(folioS3Client, times(1)).read(S3_TEST_KEY);
            verifyNoMoreInteractions(folioS3Client);
            assertThat(err).as("Fails with correct exception").isSameAs(exception);

            testContext.completeNow();
          })
        )
      );
  }

  @DisplayName("should return key when writing file successfully")
  @Test
  void shouldReturnKey_whenWritingFileSuccessfully(VertxTestContext testContext) throws IOException {
    String testData = "Testing";
    InputStream sampleDataStream = new ByteArrayInputStream(testData.getBytes(StandardCharsets.UTF_8));
    doReturn(S3_TEST_KEY).when(folioS3Client).write(S3_TEST_KEY, sampleDataStream);

    minioStorageService
      .write(S3_TEST_KEY, sampleDataStream)
      .onComplete(
        testContext.succeeding(path ->
          testContext.verify(() -> {
            verify(folioS3Client, times(1)).write(S3_TEST_KEY, sampleDataStream);
            verifyNoMoreInteractions(folioS3Client);

            assertThat(path).as("Correct path is returned").isEqualTo(S3_TEST_KEY);

            testContext.completeNow();
          })
        )
      );
  }

  @DisplayName("should fail with S3ClientException when write throws")
  @Test
  void shouldFail_withS3ClientException_whenWriteThrows(VertxTestContext testContext) throws IOException {
    String testData = "Testing";
    InputStream sampleDataStream = new ByteArrayInputStream(testData.getBytes(StandardCharsets.UTF_8));
    S3ClientException exception = new S3ClientException("test exception");
    doThrow(exception).when(folioS3Client).write(S3_TEST_KEY, sampleDataStream);

    minioStorageService
      .write(S3_TEST_KEY, sampleDataStream)
      .onComplete(
        testContext.failing(err ->
          testContext.verify(() -> {
            verify(folioS3Client, times(1)).write(S3_TEST_KEY, sampleDataStream);
            verifyNoMoreInteractions(folioS3Client);
            assertThat(err).as("Fails with correct exception").isSameAs(exception);

            testContext.completeNow();
          })
        )
      );
  }

  @DisplayName("should succeed when removing file successfully")
  @Test
  void shouldSucceed_whenRemovingFileSuccessfully(VertxTestContext testContext) {
    doReturn(S3_TEST_KEY).when(folioS3Client).remove(S3_TEST_KEY);

    minioStorageService
      .remove(S3_TEST_KEY)
      .onComplete(
        testContext.succeeding(v ->
          testContext.verify(() -> {
            verify(folioS3Client, times(1)).remove(S3_TEST_KEY);
            verifyNoMoreInteractions(folioS3Client);

            testContext.completeNow();
          })
        )
      );
  }

  @DisplayName("should fail with S3ClientException when remove throws")
  @Test
  void shouldFail_withS3ClientException_whenRemoveThrows(VertxTestContext testContext) {
    S3ClientException exception = new S3ClientException("test exception");
    doThrow(exception).when(folioS3Client).remove(S3_TEST_KEY);

    minioStorageService
      .remove(S3_TEST_KEY)
      .onComplete(
        testContext.failing(err ->
          testContext.verify(() -> {
            verify(folioS3Client, times(1)).remove(S3_TEST_KEY);
            verifyNoMoreInteractions(folioS3Client);
            assertThat(err).as("Fails with correct exception").isSameAs(exception);

            testContext.completeNow();
          })
        )
      );
  }

  @DisplayName("should return presigned download URL when file exists")
  @Test
  void shouldReturnPresignedDownloadUrl_whenFileExists(VertxTestContext testContext) {
    when(folioS3Client.getPresignedUrl(S3_TEST_KEY)).thenReturn("download-url");
    when(folioS3Client.list(S3_TEST_KEY)).thenReturn(List.of(S3_TEST_KEY));

    minioStorageService
      .getFileDownloadUrl(S3_TEST_KEY)
      .onComplete(
        testContext.succeeding(fileInfo ->
          testContext.verify(() -> {
            verify(folioS3Client, times(1)).getPresignedUrl(S3_TEST_KEY);
            verify(folioS3Client, times(1)).list(S3_TEST_KEY);
            verifyNoMoreInteractions(folioS3Client);

            assertThat(fileInfo.getUrl()).as("Presigned URL is returned").isEqualTo("download-url");

            testContext.completeNow();
          })
        )
      );
  }

  @DisplayName("should return presigned download URL when file exists alongside similar-named files")
  @Test
  void shouldReturnPresignedDownloadUrl_whenFileExistsWithSimilarFiles(VertxTestContext testContext) {
    when(folioS3Client.getPresignedUrl(S3_TEST_KEY)).thenReturn("download-url");
    when(folioS3Client.list(S3_TEST_KEY))
      .thenReturn(Arrays.asList(S3_TEST_KEY + "A", S3_TEST_KEY + "B", S3_TEST_KEY));

    minioStorageService
      .getFileDownloadUrl(S3_TEST_KEY)
      .onComplete(
        testContext.succeeding(fileInfo ->
          testContext.verify(() -> {
            verify(folioS3Client, times(1)).getPresignedUrl(S3_TEST_KEY);
            verify(folioS3Client, times(1)).list(S3_TEST_KEY);
            verifyNoMoreInteractions(folioS3Client);

            assertThat(fileInfo.getUrl()).as("Presigned URL is returned").isEqualTo("download-url");

            testContext.completeNow();
          })
        )
      );
  }

  @DisplayName("should fail with NotFoundException when only similar-named files exist but not the exact key")
  @Test
  void shouldFail_withNotFoundException_whenOnlySimilarFilesExist(VertxTestContext testContext) {
    when(folioS3Client.list(S3_TEST_KEY))
      .thenReturn(Arrays.asList(S3_TEST_KEY + "A", S3_TEST_KEY + "B"));

    minioStorageService
      .getFileDownloadUrl(S3_TEST_KEY)
      .onComplete(
        testContext.failing(err ->
          testContext.verify(() -> {
            verify(folioS3Client, times(1)).list(S3_TEST_KEY);
            verifyNoMoreInteractions(folioS3Client);

            assertThat(err).isInstanceOf(NotFoundException.class);

            testContext.completeNow();
          })
        )
      );
  }

  @DisplayName("should fail with S3ClientException when list throws during download URL retrieval")
  @Test
  void shouldFail_withS3ClientException_whenListThrowsDuringDownload(VertxTestContext testContext) {
    when(folioS3Client.list(S3_TEST_KEY)).thenThrow(new S3ClientException("test exception"));

    minioStorageService
      .getFileDownloadUrl(S3_TEST_KEY)
      .onComplete(
        testContext.failing(err ->
          testContext.verify(() -> {
            verify(folioS3Client, times(1)).list(S3_TEST_KEY);
            verifyNoMoreInteractions(folioS3Client);

            assertThat(err).isInstanceOf(S3ClientException.class);

            testContext.completeNow();
          })
        )
      );
  }

  @DisplayName("should fail all operations when key does not have the required prefix")
  @Test
  void shouldFailAllOperations_whenKeyDoesNotHaveRequiredPrefix(VertxTestContext testContext)
    throws IOException {
    Future.all(
      minioStorageService.getFileDownloadUrl("not/prefixed/correctly")
        .transform(ar -> ar.failed() ? Future.<Void>succeededFuture()
                                     : Future.failedFuture(new AssertionError("Expected getFileDownloadUrl to fail"))),
      minioStorageService.getFileUploadPartUrl("not/prefixed/correctly", "upload-id", 2)
        .transform(ar -> ar.failed() ? Future.<Void>succeededFuture() : Future.failedFuture(
          new AssertionError("Expected getFileUploadPartUrl to fail"))),
      minioStorageService.write("not/prefixed/correctly", new ByteArrayInputStream(new byte[1]))
        .transform(ar -> ar.failed() ? Future.<Void>succeededFuture()
                                     : Future.failedFuture(new AssertionError("Expected write to fail"))),
      minioStorageService.remove("not/prefixed/correctly")
        .transform(ar -> ar.failed() ? Future.<Void>succeededFuture()
                                     : Future.failedFuture(new AssertionError("Expected remove to fail")))
    ).onComplete(testContext.succeedingThenComplete());
  }
}
