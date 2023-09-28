package org.folio.service.processing.split;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.folio.service.s3storage.MinioStorageService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

@RunWith(VertxUnitRunner.class)
public class FileSplitServiceTest {

  protected static final Vertx vertx = Vertx.vertx();

  @Mock
  MinioStorageService minioStorageService;

  FileSplitService fileSplitService;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);

    this.fileSplitService =
      new FileSplitService(vertx, minioStorageService, 1000);
    when(minioStorageService.write(any(), any()))
      .thenReturn(Future.succeededFuture());
  }

  @Test
  public void testSplitFileFromS3(TestContext context) throws IOException {
    when(minioStorageService.readFile("test-key"))
      .thenReturn(
        Future.succeededFuture(
          new ByteArrayInputStream(
            Files.readAllBytes(Path.of("src/test/resources/10000.mrc"))
          )
        )
      );
    when(minioStorageService.remove("test-key"))
      .thenReturn(Future.succeededFuture());

    fileSplitService
      .splitFileFromS3(vertx.getOrCreateContext(), "test-key")
      .onComplete(
        context.asyncAssertSuccess(result -> {
          try {
            assertThat(
              result,
              containsInAnyOrder(
                "test-key_1",
                "test-key_2",
                "test-key_3",
                "test-key_4",
                "test-key_5",
                "test-key_6",
                "test-key_7",
                "test-key_8",
                "test-key_9",
                "test-key_10"
              )
            );

            verify(minioStorageService, times(1)).readFile("test-key");
            verify(minioStorageService, times(10)).write(any(), any());
            verify(minioStorageService, times(1)).remove("test-key");

            verifyNoMoreInteractions(minioStorageService);
          } catch (IOException e) {
            context.fail(e);
          }
        })
      );
  }

  @Test
  @SuppressWarnings("java:S2699")
  public void testSplitFileFromS3Exceptional(TestContext context)
    throws IOException {
    when(minioStorageService.readFile("test-key"))
      .thenReturn(
        Future.succeededFuture(new ByteArrayInputStream(new byte[1]))
      );

    try (
      MockedStatic<FileSplitUtilities> mock = Mockito.mockStatic(
        FileSplitUtilities.class,
        Mockito.CALLS_REAL_METHODS
      )
    ) {
      mock
        .when(() -> FileSplitUtilities.createTemporaryDir(anyString()))
        .thenThrow(IOException.class);

      fileSplitService
        .splitFileFromS3(vertx.getOrCreateContext(), "test-key")
        .onComplete(
          context.asyncAssertFailure(result -> {
            assertThat(result, is(instanceOf(UncheckedIOException.class)));

            verify(minioStorageService, times(1)).readFile("test-key");

            verifyNoMoreInteractions(minioStorageService);
          })
        );
    }
  }

  @Test
  public void testSplitStream(TestContext context) throws IOException {
    fileSplitService
      .splitStream(
        vertx.getOrCreateContext(),
        new ByteArrayInputStream(new byte[1]),
        "test-key"
      )
      .onComplete(
        context.asyncAssertSuccess(result ->
          assertThat(result, containsInAnyOrder("test-key_1"))
        )
      );
  }

  @Test
  public void testBadTemporaryDirectory(TestContext context)
    throws IOException {
    File test = temporaryFolder.newFolder();
    System.out.println(test.toString());

    // mockito mock static FileSplitUtilities::createTemporaryDir(String key)
    try (
      MockedStatic<FileSplitUtilities> mock = Mockito.mockStatic(
        FileSplitUtilities.class,
        Mockito.CALLS_REAL_METHODS
      )
    ) {
      mock
        .when(() -> FileSplitUtilities.createTemporaryDir(anyString()))
        .thenReturn(test.toPath());

      // can't be deleted after completion if there's a file in there
      Files.createFile(Path.of(test.getAbsolutePath(), "test-file"));

      fileSplitService
        .splitStream(
          vertx.getOrCreateContext(),
          new ByteArrayInputStream(new byte[1]),
          "test-key"
        )
        .onComplete(context.asyncAssertSuccess());
    }
  }
}
