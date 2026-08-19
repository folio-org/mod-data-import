package org.folio.service.processing.split;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.file.OpenOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.folio.service.s3storage.MinioStorageService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class FileSplitWriterS3Test {

  protected static Vertx vertx = Vertx.vertx();

  @TempDir
  Path tempDir;

  @Mock
  private MinioStorageService minioStorageService;

  @Captor
  private ArgumentCaptor<InputStream> captor;

  static Stream<Arguments> getCases() {
    return Stream.of(
      Arguments.of("src/test/resources/10.mrc", "out.mrc", 11),
      Arguments.of("src/test/resources/10.mrc", "out.mrc", 10),
      Arguments.of("src/test/resources/10.mrc", "out.mrc", 9),
      Arguments.of("src/test/resources/10.mrc", "out.mrc", 5),
      Arguments.of("src/test/resources/10.mrc", "out.mrc", 3),
      Arguments.of("src/test/resources/10.mrc", "out.mrc", 1),
      Arguments.of("src/test/resources/0.mrc", "none.mrc", 1),
      Arguments.of("src/test/resources/1.mrc", "single.mrc", 10),
      Arguments.of("src/test/resources/1.mrc", "single.mrc", 1),
      Arguments.of("src/test/resources/100.mrc", "big.mrc", 60)
    );
  }

  @ParameterizedTest(name = "[{index}] {0} chunkSize={2}")
  @MethodSource("getCases")
  @DisplayName("should upload each chunk to S3 with correct name and content")
  void shouldUploadEachChunk_toS3_withCorrectNameAndContent(
      String sourceFile, String key, int chunkSize,
      VertxTestContext testContext) throws IOException {
    when(minioStorageService.write(any(), any())).thenReturn(Future.succeededFuture("result"));

    File chunkDir = Files.createTempDirectory(tempDir, "s3writer").toFile();
    Promise<CompositeFuture> chunkUploadingCompositeFuturePromise = Promise.promise();
    FileSplitWriter writer = new FileSplitWriter(
      FileSplitWriterOptions.builder()
        .minioStorageService(minioStorageService)
        .chunkUploadingCompositeFuturePromise(chunkUploadingCompositeFuturePromise)
        .outputKey(key)
        .chunkFolder(chunkDir.toString())
        .maxRecordsPerChunk(chunkSize)
        .uploadFilesToS3(true)
        .deleteLocalFiles(false)
        .build()
    );

    vertx.getOrCreateContext().owner().fileSystem()
      .open(sourceFile, new OpenOptions().setRead(true))
      .onComplete(testContext.succeeding(file -> {
        file.pipeTo(writer).onComplete(testContext.succeeding(v -> {}));
        chunkUploadingCompositeFuturePromise.future().onComplete(
          testContext.succeeding(cf ->
            cf.onComplete(testContext.succeeding(result -> {
              for (Object obj : result.list()) {
                String path = Path.of(chunkDir.toString(), (String) obj).toString();
                try (FileInputStream fileStream = new FileInputStream(path)) {
                  verify(minioStorageService).write(
                    eq(Path.of(path).getFileName().toString()),
                    captor.capture()
                  );
                  assertThat(captor.getValue().readAllBytes())
                    .isEqualTo(fileStream.readAllBytes());
                } catch (IOException err) {
                  testContext.failNow(err);
                  return;
                }
              }

              verifyNoMoreInteractions(minioStorageService);
              testContext.completeNow();
            }))
          )
        );
      }));
  }
}
