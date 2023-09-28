package org.folio.service.processing.split;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class FileSplitWriterUnusedMethodTest {

  protected static Vertx vertx = Vertx.vertx();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testUnusedMethods(TestContext context) throws IOException {
    FileSplitWriter writer = new FileSplitWriter(
      FileSplitWriterOptions
        .builder()
        .vertxContext(vertx.getOrCreateContext())
        .chunkUploadingCompositeFuturePromise(Promise.promise())
        .outputKey("")
        .chunkFolder(temporaryFolder.newFolder().toString())
        .maxRecordsPerChunk(1)
        .uploadFilesToS3(false)
        .deleteLocalFiles(false)
        .build()
    );

    assertThat(writer.setWriteQueueMaxSize(0), is(writer));
    assertThat(writer.writeQueueFull(), is(false));
    assertThat(writer.drainHandler(null), is(writer));
  }
}
