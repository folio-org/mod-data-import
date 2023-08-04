package org.folio.service.split;

import static org.folio.util.VertxMatcherAssert.asyncAssertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.io.File;
import java.io.IOException;

import org.folio.service.processing.split.FileSplitWriter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.file.OpenOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class FileSplitWriterDeleteLocalTest {

  protected static Vertx vertx = Vertx.vertx();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final String TEST_FILE = "src/test/resources/10.mrc";
  private static final String TEST_KEY = "10.mrc";

  @Test
  public void testCleanup(TestContext context) throws IOException {
    vertx.getOrCreateContext().owner().fileSystem()
        .open(TEST_FILE, new OpenOptions().setRead(true))
        .onComplete(context.asyncAssertSuccess(file -> {
          Promise<CompositeFuture> chunkUploadingCompositeFuturePromise = Promise.promise();

          try {
            File folder = temporaryFolder.newFolder();

            FileSplitWriter writer = new FileSplitWriter(chunkUploadingCompositeFuturePromise, TEST_KEY,
                folder.toString(), 3, false, true);

            file.pipeTo(writer).onComplete(context.asyncAssertSuccess());
            chunkUploadingCompositeFuturePromise.future().onComplete(context.asyncAssertSuccess(result -> {
              asyncAssertThat(context, result.list(), hasSize(4));
              asyncAssertThat(context, folder.listFiles().length, is(0));
            }));
          } catch (IOException err) {
            context.fail(err);
          }
        }));
  }
}
