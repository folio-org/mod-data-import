package org.folio.service.processing.split;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verifyNoInteractions;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.OpenOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.service.s3storage.MinioStorageService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class FileSplitWriterRegularTest {

  protected static Vertx vertx = Vertx.vertx();

  @TempDir
  Path tempDir;

  @Mock
  private MinioStorageService minioStorageService;

  static Stream<Arguments> getCases() {
    return Stream.of(
      Arguments.of("src/test/resources/10.mrc", "out.mrc", 11, new String[] {"out_1.mrc"}),
      Arguments.of("src/test/resources/10.mrc", "out.mrc", 10, new String[] {"out_1.mrc"}),
      Arguments.of("src/test/resources/10.mrc", "out.mrc", 9, new String[] {"out_1.mrc", "out_2.mrc"}),
      Arguments.of("src/test/resources/10.mrc", "out.mrc", 5, new String[] {"out_1.mrc", "out_2.mrc"}),
      Arguments.of("src/test/resources/10.mrc", "out.mrc", 3,
        new String[] {"out_1.mrc", "out_2.mrc", "out_3.mrc", "out_4.mrc"}),
      Arguments.of("src/test/resources/10.mrc", "out.mrc", 1,
        new String[] {
          "out_1.mrc", "out_2.mrc", "out_3.mrc", "out_4.mrc", "out_5.mrc",
          "out_6.mrc", "out_7.mrc", "out_8.mrc", "out_9.mrc", "out_10.mrc",
          }),
      Arguments.of("src/test/resources/0.mrc", "none.mrc", 1, new String[] {"none_1.mrc"}),
      Arguments.of("src/test/resources/1.mrc", "single.mrc", 10, new String[] {"single_1.mrc"}),
      Arguments.of("src/test/resources/1.mrc", "single.mrc", 1, new String[] {"single_1.mrc"}),
      Arguments.of("src/test/resources/100.mrc", "big.mrc", 60, new String[] {"big_1.mrc", "big_2.mrc"}),
      Arguments.of("src/test/resources/5000.mrc", "5000.mrc", 1000,
        new String[] {"5000_1.mrc", "5000_2.mrc", "5000_3.mrc", "5000_4.mrc", "5000_5.mrc"}),
      Arguments.of("src/test/resources/10000.mrc", "10000.mrc", 1000,
        new String[] {
          "10000_1.mrc", "10000_2.mrc", "10000_3.mrc", "10000_4.mrc", "10000_5.mrc",
          "10000_6.mrc", "10000_7.mrc", "10000_8.mrc", "10000_9.mrc", "10000_10.mrc",
          }),
      Arguments.of("src/test/resources/22778.mrc", "22778.mrc", 2300,
        new String[] {
          "22778_1.mrc", "22778_2.mrc", "22778_3.mrc", "22778_4.mrc", "22778_5.mrc",
          "22778_6.mrc", "22778_7.mrc", "22778_8.mrc", "22778_9.mrc", "22778_10.mrc",
          }),
      Arguments.of("src/test/resources/50000.mrc", "50000.mrc", 5000,
        new String[] {
          "50000_1.mrc", "50000_2.mrc", "50000_3.mrc", "50000_4.mrc", "50000_5.mrc",
          "50000_6.mrc", "50000_7.mrc", "50000_8.mrc", "50000_9.mrc", "50000_10.mrc",
          })
    );
  }

  @ParameterizedTest(name = "[{index}] {0} chunkSize={2}")
  @MethodSource("getCases")
  @DisplayName("should split file into expected chunks and preserve content")
  void shouldSplitFile_intoExpectedChunks_andPreserveContent(
    String sourceFile, String key, int chunkSize, String[] expectedChunkFiles,
    VertxTestContext testContext) throws IOException {
    Promise<CompositeFuture> chunkUploadingCompositeFuturePromise = Promise.promise();
    FileSplitWriter writer = new FileSplitWriter(
      FileSplitWriterOptions.builder()
        .chunkUploadingCompositeFuturePromise(chunkUploadingCompositeFuturePromise)
        .outputKey(key)
        .chunkFolder(Files.createTempDirectory(tempDir, "writer").toString())
        .maxRecordsPerChunk(chunkSize)
        .uploadFilesToS3(false)
        .deleteLocalFiles(false)
        .build()
    );

    vertx.getOrCreateContext().owner().fileSystem()
      .open(sourceFile, new OpenOptions().setRead(true))
      .onComplete(testContext.succeeding(file -> file.pipeTo(writer)
        .onComplete(testContext.succeeding(v -> chunkUploadingCompositeFuturePromise.future()
          .onComplete(
            testContext.succeeding(cf ->
              cf.onComplete(testContext.succeeding(internalFuture -> {
                List<Path> paths = internalFuture.list().stream()
                  .map(obj -> Path.of((String) obj))
                  .toList();
                List<String> fileNames = paths.stream()
                  .map(path -> path.getFileName().toString())
                  .toList();

                assertThat(fileNames).containsExactly(expectedChunkFiles);

                int totalSize = 0;
                List<byte[]> fileContents = new ArrayList<>();

                for (Path path : paths) {
                  File actualFile = path.toFile();
                  totalSize += actualFile.length();
                  try (FileInputStream fileStream = new FileInputStream(actualFile)) {
                    fileContents.add(fileStream.readAllBytes());
                  } catch (IOException err) {
                    testContext.failNow(err);
                    return;
                  }
                }

                byte[] actual = new byte[totalSize];
                int pos = 0;
                for (byte[] content : fileContents) {
                  System.arraycopy(content, 0, actual, pos, content.length);
                  pos += content.length;
                }

                for (byte[] content : fileContents) {
                  if (content.length > 0) {
                    assertThat(content[content.length - 1])
                      .isEqualTo(FileSplitUtilities.MARC_RECORD_TERMINATOR);
                  }
                }

                file.read(Buffer.buffer(), 0, 0, totalSize + 1)
                  .onComplete(testContext.succeeding(expectedBuffer -> {
                    byte[] expected = expectedBuffer.getBytes();
                    assertThat(actual).isEqualTo(expected);

                    int[] totalRecords = {0};
                    try {
                      totalRecords[0] = countRecordsInMarcFile(new ByteArrayInputStream(actual));
                    } catch (IOException err) {
                      testContext.failNow(err);
                      return;
                    }

                    for (int i = 0; i < fileContents.size(); i++) {
                      try {
                        if (i == fileContents.size() - 1) {
                          assertThat(countRecordsInMarcFile(new ByteArrayInputStream(fileContents.get(i))))
                            .isEqualTo(totalRecords[0]);
                        } else {
                          assertThat(countRecordsInMarcFile(new ByteArrayInputStream(fileContents.get(i))))
                            .isEqualTo(chunkSize);
                          totalRecords[0] -= chunkSize;
                        }
                      } catch (IOException err) {
                        testContext.failNow(err);
                        return;
                      }
                    }

                    verifyNoInteractions(minioStorageService);
                    testContext.completeNow();
                  }));
              }))
            )
          )))));
  }

  private int countRecordsInMarcFile(InputStream stream) throws IOException {
    return FileSplitUtilities.countRecordsInFile(
      "placeholder.mrc",
      stream,
      new JobProfileInfo().withDataType(JobProfileInfo.DataType.MARC)
    );
  }
}
