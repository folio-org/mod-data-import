package org.folio.service.processing.split;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.file.OpenOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.File;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class FileSplitWriterDeleteLocalTest {

  protected static Vertx vertx = Vertx.vertx();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final String TEST_FILE = "src/test/resources/10.mrc";
  private static final String TEST_KEY = "10.mrc";

  @Test
  public void testCleanup(TestContext context) throws IOException {
    vertx
      .getOrCreateContext()
      .owner()
      .fileSystem()
      .open(TEST_FILE, new OpenOptions().setRead(true))
      .onComplete(
        context.asyncAssertSuccess(file -> {
          Promise<CompositeFuture> chunkUploadingCompositeFuturePromise = Promise.promise();

          try {
            File folder = temporaryFolder.newFolder();

            FileSplitWriter writer = new FileSplitWriter(
              FileSplitWriterOptions
                .builder()
                .vertxContext(vertx.getOrCreateContext())
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

            file.pipeTo(writer).onComplete(context.asyncAssertSuccess());
            chunkUploadingCompositeFuturePromise
              .future()
              .onComplete(
                context.asyncAssertSuccess(result -> {
                  assertThat(result.list(), hasSize(4));
                  // need to add a small delay since the actual deletion of files can be async
                  // depending on OS implementations
                  vertx.setTimer(
                    100,
                    _v ->
                      context.verify(__v ->
                        assertThat(folder.listFiles().length, is(0))
                      )
                  );
                })
              );
          } catch (IOException err) {
            context.fail(err);
          }
        })
      );
  }
}
