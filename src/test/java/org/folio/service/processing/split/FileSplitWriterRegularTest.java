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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
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

  @DisplayName("should split file into expected chunks and preserve content")
  @ParameterizedTest(name = "[{index}] {0} chunkSize={2}")
  @MethodSource("getCases")
  void shouldSplitFile_intoExpectedChunks_andPreserveContent(
      String sourceFile, String key, int chunkSize, String[] expectedChunkFiles,
      Vertx vertx, VertxTestContext testContext) throws IOException {
    // arrange
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

    var pathsRef = new AtomicReference<List<Path>>();
    var contentsRef = new AtomicReference<List<byte[]>>();
    var actualRef = new AtomicReference<byte[]>();

    // act
    vertx.fileSystem()
      .open(sourceFile, new OpenOptions().setRead(true))
      .compose(file -> file.pipeTo(writer)
        .compose(v -> chunkUploadingCompositeFuturePromise.future())
        .compose(cf -> cf)
        .compose(result -> {
          pathsRef.set(toPaths(result.list()));
          contentsRef.set(readAllFiles(pathsRef.get()));
          actualRef.set(concatenate(contentsRef.get()));
          return file.read(Buffer.buffer(), 0, 0, actualRef.get().length + 1);
        }))
      .onComplete(testContext.succeeding(expectedBuffer -> testContext.verify(() -> {
        // assert
        assertThat(toFileNames(pathsRef.get())).containsExactly(expectedChunkFiles);
        assertTerminators(contentsRef.get());
        assertThat(actualRef.get()).isEqualTo(expectedBuffer.getBytes());
        assertChunkRecordCounts(contentsRef.get(), actualRef.get(), chunkSize);
        verifyNoInteractions(minioStorageService);
        testContext.completeNow();
      })));
  }

  private static List<Path> toPaths(List<?> objList) {
    return objList.stream().map(obj -> Path.of((String) obj)).toList();
  }

  private static List<String> toFileNames(List<Path> paths) {
    return paths.stream().map(path -> path.getFileName().toString()).toList();
  }

  private static List<byte[]> readAllFiles(List<Path> paths) {
    return paths.stream()
      .map(path -> {
        try (FileInputStream fis = new FileInputStream(path.toFile())) {
          return fis.readAllBytes();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      })
      .toList();
  }

  private static byte[] concatenate(List<byte[]> arrays) {
    int totalSize = arrays.stream().mapToInt(a -> a.length).sum();
    byte[] result = new byte[totalSize];
    int pos = 0;
    for (byte[] arr : arrays) {
      System.arraycopy(arr, 0, result, pos, arr.length);
      pos += arr.length;
    }
    return result;
  }

  private static void assertTerminators(List<byte[]> contents) {
    assertThat(contents)
      .filteredOn(content -> content.length > 0)
      .allSatisfy(content ->
        assertThat(content[content.length - 1])
          .isEqualTo(FileSplitUtilities.MARC_RECORD_TERMINATOR));
  }

  private void assertChunkRecordCounts(List<byte[]> contents, byte[] concatenated, int chunkSize) {
    int totalRecords = countRecordsInMarcFile(concatenated);
    int lastExpected = totalRecords - chunkSize * (contents.size() - 1);
    List<Integer> actualCounts = contents.stream().map(this::countRecordsInMarcFile).toList();
    // all chunks except the last should have exactly chunkSize records
    assertThat(actualCounts.subList(0, contents.size() - 1))
      .allSatisfy(count -> assertThat(count).isEqualTo(chunkSize));
    assertThat(actualCounts.getLast()).isEqualTo(lastExpected);
  }

  private int countRecordsInMarcFile(byte[] content) {
    return countRecordsInMarcFile(new ByteArrayInputStream(content));
  }

  private int countRecordsInMarcFile(InputStream stream) {
    try {
      return FileSplitUtilities.countRecordsInFile(
        "placeholder.mrc",
        stream,
        new JobProfileInfo().withDataType(JobProfileInfo.DataType.MARC)
      );
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
