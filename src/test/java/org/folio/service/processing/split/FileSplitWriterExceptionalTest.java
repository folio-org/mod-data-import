package org.folio.service.processing.split;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
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
class FileSplitWriterExceptionalTest {

  protected static Vertx vertx = Vertx.vertx();
  private static final String TEST_FILE = "src/test/resources/10.mrc";
  private static final String TEST_KEY = "10.mrc";
  @TempDir
  Path tempDir;

  @Test
  @DisplayName("should fail to pipe and call exception handler when directory is deleted")
  void shouldFailToPipe_andCallExceptionHandler_whenDirectoryDeleted(VertxTestContext testContext) {
    var exceptionHandled = Promise.<Void>promise();

    vertx
      .getOrCreateContext()
      .owner()
      .fileSystem()
      .open(TEST_FILE, new OpenOptions().setRead(true))
      .onComplete(
        testContext.succeeding(file -> {
          Promise<CompositeFuture> chunkUploadingCompositeFuturePromise =
            Promise.promise();

          try {
            File folder = Files.createTempDirectory(tempDir, "writer").toFile();
            String path = folder.getPath();

            FileSplitWriter writer = new FileSplitWriter(
              FileSplitWriterOptions
                .builder()
                .chunkUploadingCompositeFuturePromise(
                  chunkUploadingCompositeFuturePromise
                )
                .outputKey(TEST_KEY)
                .chunkFolder(path)
                .maxRecordsPerChunk(1)
                .uploadFilesToS3(false)
                .deleteLocalFiles(false)
                .build()
            );

            writer.exceptionHandler(err -> exceptionHandled.tryComplete());

            for (File f : folder.listFiles()) {
              Files.delete(Path.of(f.getPath()));
            }
            Files.delete(Path.of(folder.getPath()));

            var pipeFuture = file.pipeTo(writer)
              .transform(ar -> ar.failed()
                               ? Future.<Void>succeededFuture()
                               : Future.failedFuture(new AssertionError("Expected pipeTo to fail")));

            Future.all(exceptionHandled.future(), pipeFuture)
              .onComplete(testContext.succeedingThenComplete());
          } catch (IOException err) {
            testContext.failNow(err);
          }
        })
      );
  }

  @Test
  @DisplayName("should fail promise when directory is deleted and no exception handler set")
  void shouldFailPromise_whenDirectoryDeletedAndNoExceptionHandlerSet(
    VertxTestContext testContext
  ) throws IOException {
    Promise<CompositeFuture> chunkUploadingCompositeFuturePromise = Promise.promise();

    File folder = Files.createTempDirectory(tempDir, "writer").toFile();
    String path = folder.getPath();

    FileSplitWriter writer = new FileSplitWriter(
      FileSplitWriterOptions
        .builder()
        .chunkUploadingCompositeFuturePromise(chunkUploadingCompositeFuturePromise)
        .outputKey(TEST_KEY)
        .chunkFolder(path)
        .maxRecordsPerChunk(1)
        .uploadFilesToS3(false)
        .deleteLocalFiles(false)
        .build()
    );

    for (File f : folder.listFiles()) {
      Files.delete(Path.of(f.getPath()));
    }
    Files.delete(Path.of(folder.getPath()));

    writer.write(
      Buffer.buffer(
        new byte[] {
          FileSplitUtilities.MARC_RECORD_TERMINATOR,
          FileSplitUtilities.MARC_RECORD_TERMINATOR,
          FileSplitUtilities.MARC_RECORD_TERMINATOR,
          }
      )
    );

    chunkUploadingCompositeFuturePromise
      .future()
      .onComplete(testContext.failingThenComplete());
  }
}
