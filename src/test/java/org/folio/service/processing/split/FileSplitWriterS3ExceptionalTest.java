package org.folio.service.processing.split;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.file.OpenOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.folio.s3.exception.S3ClientException;
import org.folio.service.s3storage.MinioStorageService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class FileSplitWriterS3ExceptionalTest {

  protected static Vertx vertx = Vertx.vertx();
  private static final String TEST_FILE = "src/test/resources/10.mrc";
  private static final String TEST_KEY = "10.mrc";

  @TempDir
  Path tempDir;

  Promise<CompositeFuture> chunkUploadingCompositeFuturePromise = Promise.promise();

  @Mock
  private MinioStorageService minioStorageService;

  private FileSplitWriter writer;

  @BeforeEach
  void setUp() throws IOException {
    writer = new FileSplitWriter(
      FileSplitWriterOptions
        .builder()
        .minioStorageService(minioStorageService)
        .chunkUploadingCompositeFuturePromise(chunkUploadingCompositeFuturePromise)
        .outputKey(TEST_KEY)
        .chunkFolder(Files.createTempDirectory(tempDir, "s3writer").toString())
        .maxRecordsPerChunk(3)
        .uploadFilesToS3(true)
        .deleteLocalFiles(false)
        .build()
    );
  }

  @DisplayName("should fail composite future when write throws IOException before upload")
  @Test
  void shouldFailCompositeFuture_whenWriteThrowsBeforeUpload(
    VertxTestContext testContext
  ) throws IOException {
    when(minioStorageService.write(any(), any())).thenThrow(new IOException());

    vertx
      .getOrCreateContext()
      .owner()
      .fileSystem()
      .open(TEST_FILE, new OpenOptions().setRead(true))
      .onComplete(
        testContext.succeeding(file -> {
          file.pipeTo(writer).onComplete(testContext.succeeding(v -> { }));
          chunkUploadingCompositeFuturePromise
            .future()
            .onComplete(
              testContext.succeeding(cf ->
                cf.onComplete(testContext.failingThenComplete())
              )
            );
        })
      );
  }

  @DisplayName("should fail composite future when S3 upload returns failed future")
  @Test
  void shouldFailCompositeFuture_whenS3UploadReturnsFailed(
    VertxTestContext testContext
  ) throws IOException {
    when(minioStorageService.write(any(), any()))
      .thenReturn(Future.failedFuture(new S3ClientException("wrong bucket")));

    vertx
      .getOrCreateContext()
      .owner()
      .fileSystem()
      .open(TEST_FILE, new OpenOptions().setRead(true))
      .onComplete(
        testContext.succeeding(file -> {
          file.pipeTo(writer).onComplete(testContext.succeeding(v -> { }));
          chunkUploadingCompositeFuturePromise
            .future()
            .onComplete(
              testContext.succeeding(cf ->
                cf.onComplete(testContext.failingThenComplete())
              )
            );
        })
      );
  }
}
