package org.folio.service.processing.split;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.OpenOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class FileSplitWriterExceptionalTest {

  protected static Vertx vertx = Vertx.vertx();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final String TEST_FILE = "src/test/resources/10.mrc";
  private static final String TEST_KEY = "10.mrc";

  @Test
  public void testInvalidDirectory(TestContext context) throws IOException {
    Async async = context.strictAsync(1); // ensure only one exception

    vertx
      .getOrCreateContext()
      .owner()
      .fileSystem()
      .open(TEST_FILE, new OpenOptions().setRead(true))
      .onComplete(
        context.asyncAssertSuccess(file -> {
          Promise<CompositeFuture> chunkUploadingCompositeFuturePromise = Promise.promise();

          try {
            // we will delete this later, so writing will error
            File folder = temporaryFolder.newFolder();
            String path = folder.getPath();

            FileSplitWriter writer = new FileSplitWriter(
              FileSplitWriterOptions
                .builder()
                .vertxContext(vertx.getOrCreateContext())
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

            writer.exceptionHandler(err -> async.countDown());

            for (File f : folder.listFiles()) {
              Files.delete(Path.of(f.getPath()));
            }
            Files.delete(Path.of(folder.getPath()));

            // should not be able to pipe, resulting in failure
            file.pipeTo(writer).onComplete(context.asyncAssertFailure());
          } catch (IOException err) {
            context.fail(err);
          }
        })
      );
  }

  @Test
  public void testInvalidDirectoryNoHandler(TestContext context)
    throws IOException {
    Promise<CompositeFuture> chunkUploadingCompositeFuturePromise = Promise.promise();

    try {
      // we will delete this later, so writing will error
      File folder = temporaryFolder.newFolder();
      String path = folder.getPath();

      FileSplitWriter writer = new FileSplitWriter(
        FileSplitWriterOptions
          .builder()
          .vertxContext(vertx.getOrCreateContext())
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

      for (File f : folder.listFiles()) {
        Files.delete(Path.of(f.getPath()));
      }
      Files.delete(Path.of(folder.getPath()));

      // should not be able to write, resulting in failure, but with no handler
      // so it will be reported internally only
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
        .onComplete(context.asyncAssertFailure());
    } catch (IOException err) {
      context.fail(err);
    }
  }
}
