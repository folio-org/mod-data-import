package org.folio.service.processing.split;

import static org.assertj.core.api.Assertions.assertThat;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.file.OpenOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(VertxExtension.class)
class FileSplitWriterDeleteLocalTest {

  protected static Vertx vertx = Vertx.vertx();
  private static final String TEST_FILE = "src/test/resources/10.mrc";
  private static final String TEST_KEY = "10.mrc";
  @TempDir
  Path tempDir;

  @Test
  @DisplayName("should delete local chunk files after splitting when deleteLocalFiles is enabled")
  void shouldDeleteLocalChunkFiles_whenDeleteLocalFilesEnabled(VertxTestContext testContext) {
    vertx
      .getOrCreateContext()
      .owner()
      .fileSystem()
      .open(TEST_FILE, new OpenOptions().setRead(true))
      .onComplete(
        testContext.succeeding(file -> {
          Promise<CompositeFuture> chunkUploadingCompositeFuturePromise = Promise.promise();

          try {
            File folder = Files.createTempDirectory(tempDir, "delete-local").toFile();

            FileSplitWriter writer = new FileSplitWriter(
              FileSplitWriterOptions
                .builder()
                .chunkUploadingCompositeFuturePromise(
                  chunkUploadingCompositeFuturePromise
                )
                .outputKey(TEST_KEY)
                .chunkFolder(folder.toString())
                .maxRecordsPerChunk(3)
                .uploadFilesToS3(false)
                .deleteLocalFiles(true)
                .build()
            );

            file.pipeTo(writer).onComplete(testContext.succeeding(v -> { }));
            chunkUploadingCompositeFuturePromise
              .future()
              .onComplete(
                testContext.succeeding(result ->
                  testContext.verify(() -> {
                    assertThat(result.list()).hasSize(4);
                    vertx.setTimer(
                      100,
                      _v ->
                        testContext.verify(() -> {
                          assertThat(folder).isEmptyDirectory();
                          testContext.completeNow();
                        })
                    );
                  })
                )
              );
          } catch (IOException err) {
            testContext.failNow(err);
          }
        })
      );
  }
}
