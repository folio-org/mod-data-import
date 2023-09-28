package org.folio.service.processing.split;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
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
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunnerWithParametersFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import org.folio.service.s3storage.MinioStorageService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(VertxUnitRunnerWithParametersFactory.class)
public class FileSplitWriterS3Test {

  protected static Vertx vertx = Vertx.vertx();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  File chunkDir;

  Promise<CompositeFuture> chunkUploadingCompositeFuturePromise = Promise.promise();

  @Mock
  private MinioStorageService minioStorageService;

  @Captor
  private ArgumentCaptor<InputStream> captor;

  private FileSplitWriter writer;

  private String sourceFile;
  private String key;
  private int chunkSize;

  // tuples of [source file, key, chunk size]
  // expected chunk files are checked by FileSplitWriterRegularTest
  @Parameters
  public static Collection<Object[]> getCases() {
    return Arrays.asList(
      new Object[] { "src/test/resources/10.mrc", "out.mrc", 11 },
      new Object[] { "src/test/resources/10.mrc", "out.mrc", 10 },
      new Object[] { "src/test/resources/10.mrc", "out.mrc", 9 },
      new Object[] { "src/test/resources/10.mrc", "out.mrc", 5 },
      new Object[] { "src/test/resources/10.mrc", "out.mrc", 3 },
      new Object[] { "src/test/resources/10.mrc", "out.mrc", 1 },
      new Object[] { "src/test/resources/0.mrc", "none.mrc", 1 },
      new Object[] { "src/test/resources/1.mrc", "single.mrc", 10 },
      new Object[] { "src/test/resources/1.mrc", "single.mrc", 1 },
      new Object[] { "src/test/resources/100.mrc", "big.mrc", 60 }
    );
  }

  public FileSplitWriterS3Test(String sourceFile, String key, int chunkSize)
    throws IOException {
    this.sourceFile = sourceFile;
    this.key = key;
    this.chunkSize = chunkSize;
  }

  @Before
  public void setUp() throws IOException {
    chunkDir = temporaryFolder.newFolder();

    MockitoAnnotations.openMocks(this);

    writer =
      new FileSplitWriter(
        FileSplitWriterOptions
          .builder()
          .vertxContext(vertx.getOrCreateContext())
          .minioStorageService(minioStorageService)
          .chunkUploadingCompositeFuturePromise(
            chunkUploadingCompositeFuturePromise
          )
          .outputKey(key)
          .chunkFolder(chunkDir.toString())
          .maxRecordsPerChunk(chunkSize)
          .uploadFilesToS3(true)
          .deleteLocalFiles(false)
          .build()
      );
  }

  @Test
  public void testUploadToS3(TestContext context) throws IOException {
    when(minioStorageService.write(any(), any()))
      .thenReturn(Future.succeededFuture("result"));

    vertx
      .getOrCreateContext()
      .owner()
      .fileSystem()
      .open(sourceFile, new OpenOptions().setRead(true))
      .onComplete(
        context.asyncAssertSuccess(file -> {
          file.pipeTo(writer).onComplete(context.asyncAssertSuccess());
          chunkUploadingCompositeFuturePromise
            .future()
            .onComplete(
              context.asyncAssertSuccess(cf ->
                cf.onComplete(
                  context.asyncAssertSuccess(result -> {
                    for (Object obj : result.list()) {
                      String path = Path
                        .of(chunkDir.toString(), (String) obj)
                        .toString();

                      try (
                        FileInputStream fileStream = new FileInputStream(path)
                      ) {
                        verify(minioStorageService)
                          .write(
                            eq(Path.of(path).getFileName().toString()),
                            captor.capture()
                          );
                        assertThat(
                          captor.getValue().readAllBytes(),
                          is(equalTo(fileStream.readAllBytes()))
                        );
                      } catch (IOException err) {
                        context.fail(err);
                      }
                    }

                    verifyNoMoreInteractions(minioStorageService);
                  })
                )
              )
            );
        })
      );
  }
}
