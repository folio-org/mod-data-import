package org.folio.service.processing.split;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import org.folio.service.s3storage.MinioStorageService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class FileSplitServiceTest {

  protected static final Vertx vertx = Vertx.vertx();

  @Mock
  MinioStorageService minioStorageService;

  @TempDir
  Path tempDir;

  FileSplitService fileSplitService;

  @BeforeEach
  void setUp() {
    this.fileSplitService = new FileSplitService(vertx, minioStorageService, 1000);
  }

  @Test
  @DisplayName("should split file from S3 into 10 chunks and verify interactions when source has 10000 records")
  void shouldSplitFileFromS3_into10Chunks_andVerifyInteractions(VertxTestContext testContext)
      throws IOException {
    when(minioStorageService.write(any(), any())).thenReturn(Future.succeededFuture());
    when(minioStorageService.readFile("test-key"))
      .thenReturn(
        Future.succeededFuture(
          new ByteArrayInputStream(Files.readAllBytes(Path.of("src/test/resources/10000.mrc")))
        )
      );
    when(minioStorageService.remove("test-key")).thenReturn(Future.succeededFuture());

    fileSplitService
      .splitFileFromS3(vertx.getOrCreateContext(), "test-key")
      .onComplete(
        testContext.succeeding(result ->
          testContext.verify(() -> {
            assertThat(result).containsExactlyInAnyOrder(
              "test-key_1", "test-key_2", "test-key_3", "test-key_4", "test-key_5",
              "test-key_6", "test-key_7", "test-key_8", "test-key_9", "test-key_10"
            );

            verify(minioStorageService, times(1)).readFile("test-key");
            verify(minioStorageService, times(10)).write(any(), any());
            verify(minioStorageService, times(1)).remove("test-key");
            verifyNoMoreInteractions(minioStorageService);

            testContext.completeNow();
          })
        )
      );
  }

  @Test
  @DisplayName("should fail with InvalidPathException when key contains NUL character")
  void shouldFail_withInvalidPathException_whenKeyContainsNulCharacter(VertxTestContext testContext) {
    String key = "test-key" + '\0';
    when(minioStorageService.readFile(key))
      .thenReturn(Future.succeededFuture(new ByteArrayInputStream(new byte[1])));

    fileSplitService
      .splitFileFromS3(vertx.getOrCreateContext(), key)
      .onComplete(
        testContext.failing(result ->
          testContext.verify(() -> {
            assertThat(result).isInstanceOf(InvalidPathException.class);

            verify(minioStorageService, times(1)).readFile(key);
            verifyNoMoreInteractions(minioStorageService);

            testContext.completeNow();
          })
        )
      );
  }

  @Test
  @DisplayName("should split stream and return one chunk key when source has one byte")
  void shouldSplitStream_andReturnOneChunkKey_whenSourceHasOneByte(VertxTestContext testContext) throws IOException {
    when(minioStorageService.write(any(), any())).thenReturn(Future.succeededFuture());
    fileSplitService
      .splitStream(
        vertx.getOrCreateContext(),
        new ByteArrayInputStream(new byte[1]),
        "test-key"
      )
      .onComplete(
        testContext.succeeding(result ->
          testContext.verify(() -> {
            assertThat(result).containsExactlyInAnyOrder("test-key_1");
            testContext.completeNow();
          })
        )
      );
  }

  @Test
  @DisplayName("should succeed when temporary directory cannot be deleted after split")
  void shouldSucceed_whenTempDirCannotBeDeletedAfterSplit(VertxTestContext testContext)
      throws IOException {
    when(minioStorageService.write(any(), any())).thenReturn(Future.succeededFuture());
    File test = Files.createTempDirectory(tempDir, "split-service").toFile();

    try (
      MockedStatic<FileSplitUtilities> mock = Mockito.mockStatic(
        FileSplitUtilities.class,
        Mockito.CALLS_REAL_METHODS
      )
    ) {
      mock
        .when(() -> FileSplitUtilities.createTemporaryDir(anyString()))
        .thenReturn(test.toPath());

      Files.createFile(Path.of(test.getAbsolutePath(), "test-file"));

      fileSplitService
        .splitStream(
          vertx.getOrCreateContext(),
          new ByteArrayInputStream(new byte[1]),
          "test-key"
        )
        .onComplete(testContext.succeedingThenComplete());
    }
  }
}
