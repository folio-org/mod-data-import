package org.folio.service.split;

import io.vertx.core.*;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import org.folio.service.processing.split.FileSplitUtilities;
import org.folio.service.processing.split.FileSplitWriter;
import org.folio.service.s3storage.MinioStorageService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(VertxUnitRunner.class)
public class FileSplitWriterTest {

  protected static Vertx vertx = Vertx.vertx();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Mock
  private MinioStorageService minioStorageService;

  private static final String VALID_MARC_SOURCE_PATH_10 = "src/test/resources/10.mrc";

  private static final String VALID_MARC_KEY = "10.mrc";


  @Before
  public void setUp(TestContext context) {
    MockitoAnnotations.openMocks(this);
  }


  @Test
  public void shouldSplitFileIntoCorrectChunks(TestContext context) throws IOException {

    Async async = context.async();
    Context vertxContext = vertx.getOrCreateContext();
    File localStorageFolder = temporaryFolder.newFolder();

    var fileSystem = vertxContext.owner().fileSystem();
    Future<AsyncFile> asyncFileFuture = fileSystem
      .open(VALID_MARC_SOURCE_PATH_10, new OpenOptions().setRead(true))
      .onComplete(ar -> {
        if (ar.succeeded()) {
          AsyncFile file = ar.result();
          try {
            Promise<CompositeFuture> chunkUploadingCompositeFuturePromise = Promise.promise();

            FileSplitWriter writer = new FileSplitWriter(vertxContext, chunkUploadingCompositeFuturePromise, VALID_MARC_KEY ,localStorageFolder.getPath());
            writer.setParams( FileSplitUtilities.MARC_RECORD_TERMINATOR, 3, false, false);
            file.pipeTo(writer).onComplete(ar1 -> {
              if (ar1.succeeded()) {
                File[] splitFiles = localStorageFolder.listFiles();
                assertEquals(4, splitFiles.length);
                // More assertions to be made on the split files - names and content
                async.complete();
              } else {
                context.fail("shouldSplitFileIntoCorrectChunks should not fail");
              }
            });
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        } else {
          context.fail("shouldSplitFileIntoCorrectChunks failed opening test marc file");
        }

      });
  }

}
