package org.folio.service.split;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;

import org.folio.service.processing.split.FileSplitWriter;
import org.folio.service.s3storage.MinioStorageService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.file.OpenOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class FileSplitWriterS3ExceptionalTest {

  private static final String TEST_FILE = "src/test/resources/10.mrc";
  private static final String TEST_KEY = "10.mrc";

  protected static Vertx vertx = Vertx.vertx();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  Promise<CompositeFuture> chunkUploadingCompositeFuturePromise = Promise.promise();

  @Mock
  private MinioStorageService minioStorageService;
  @Captor
  private ArgumentCaptor<InputStream> captor;

  @InjectMocks
  private FileSplitWriter writer;

  @Before
  public void setUp() throws IOException {
    writer = new FileSplitWriter(chunkUploadingCompositeFuturePromise, TEST_KEY, temporaryFolder.newFolder().toString(),
        3, true, false);

    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testExceptionBeforeUpload(TestContext context) throws IOException {
    when(minioStorageService.write(any(), any())).thenThrow(new IOException());

    vertx.getOrCreateContext().owner().fileSystem()
        .open(TEST_FILE, new OpenOptions().setRead(true))
        .onComplete(context.asyncAssertSuccess(file -> {
          file.pipeTo(writer).onComplete(context.asyncAssertSuccess());
          chunkUploadingCompositeFuturePromise.future().onComplete(
              context.asyncAssertSuccess(cf -> cf.onComplete(context.asyncAssertFailure())));
        }));
  }

  @Test
  public void testExceptionDuringUpload(TestContext context) throws IOException {
    when(minioStorageService.write(any(), any())).thenReturn(Future.failedFuture("test"));

    vertx.getOrCreateContext().owner().fileSystem()
        .open(TEST_FILE, new OpenOptions().setRead(true))
        .onComplete(context.asyncAssertSuccess(file -> {
          file.pipeTo(writer).onComplete(context.asyncAssertSuccess());
          chunkUploadingCompositeFuturePromise.future().onComplete(
              context.asyncAssertSuccess(cf -> cf.onComplete(context.asyncAssertFailure())));
        }));
  }
}
