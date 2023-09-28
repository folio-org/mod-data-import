package org.folio.service.processing.split;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verifyNoInteractions;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.OpenOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunnerWithParametersFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.service.s3storage.MinioStorageService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(VertxUnitRunnerWithParametersFactory.class)
public class FileSplitWriterRegularTest {

  protected static Vertx vertx = Vertx.vertx();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Mock
  private MinioStorageService minioStorageService;

  Promise<CompositeFuture> chunkUploadingCompositeFuturePromise = Promise.promise();

  FileSplitWriter writer;

  private String sourceFile;
  private String key;
  private int chunkSize;
  private String[] expectedChunkFiles;

  // tuples of [source file, key, chunk size, expected chunk files[]]
  @Parameters
  public static Collection<Object[]> getCases() {
    return Arrays.asList(
      new Object[] {
        "src/test/resources/10.mrc",
        "out.mrc",
        11,
        new String[] { "out_1.mrc" },
      },
      new Object[] {
        "src/test/resources/10.mrc",
        "out.mrc",
        10,
        new String[] { "out_1.mrc" },
      },
      new Object[] {
        "src/test/resources/10.mrc",
        "out.mrc",
        9,
        new String[] { "out_1.mrc", "out_2.mrc" },
      },
      new Object[] {
        "src/test/resources/10.mrc",
        "out.mrc",
        5,
        new String[] { "out_1.mrc", "out_2.mrc" },
      },
      new Object[] {
        "src/test/resources/10.mrc",
        "out.mrc",
        3,
        new String[] { "out_1.mrc", "out_2.mrc", "out_3.mrc", "out_4.mrc" },
      },
      new Object[] {
        "src/test/resources/10.mrc",
        "out.mrc",
        1,
        new String[] {
          "out_1.mrc",
          "out_2.mrc",
          "out_3.mrc",
          "out_4.mrc",
          "out_5.mrc",
          "out_6.mrc",
          "out_7.mrc",
          "out_8.mrc",
          "out_9.mrc",
          "out_10.mrc",
        },
      },
      new Object[] {
        "src/test/resources/0.mrc",
        "none.mrc",
        1,
        new String[] { "none_1.mrc" },
      },
      new Object[] {
        "src/test/resources/1.mrc",
        "single.mrc",
        10,
        new String[] { "single_1.mrc" },
      },
      new Object[] {
        "src/test/resources/1.mrc",
        "single.mrc",
        1,
        new String[] { "single_1.mrc" },
      },
      new Object[] {
        "src/test/resources/100.mrc",
        "big.mrc",
        60,
        new String[] { "big_1.mrc", "big_2.mrc" },
      },
      new Object[] {
        "src/test/resources/5000.mrc",
        "5000.mrc",
        1000,
        new String[] {
          "5000_1.mrc",
          "5000_2.mrc",
          "5000_3.mrc",
          "5000_4.mrc",
          "5000_5.mrc",
        },
      },
      new Object[] {
        "src/test/resources/10000.mrc",
        "10000.mrc",
        1000,
        new String[] {
          "10000_1.mrc",
          "10000_2.mrc",
          "10000_3.mrc",
          "10000_4.mrc",
          "10000_5.mrc",
          "10000_6.mrc",
          "10000_7.mrc",
          "10000_8.mrc",
          "10000_9.mrc",
          "10000_10.mrc",
        },
      },
      new Object[] {
        "src/test/resources/22778.mrc",
        "22778.mrc",
        2300,
        new String[] {
          "22778_1.mrc",
          "22778_2.mrc",
          "22778_3.mrc",
          "22778_4.mrc",
          "22778_5.mrc",
          "22778_6.mrc",
          "22778_7.mrc",
          "22778_8.mrc",
          "22778_9.mrc",
          "22778_10.mrc",
        },
      },
      new Object[] {
        "src/test/resources/50000.mrc",
        "50000.mrc",
        5000,
        new String[] {
          "50000_1.mrc",
          "50000_2.mrc",
          "50000_3.mrc",
          "50000_4.mrc",
          "50000_5.mrc",
          "50000_6.mrc",
          "50000_7.mrc",
          "50000_8.mrc",
          "50000_9.mrc",
          "50000_10.mrc",
        },
      }
    );
  }

  public FileSplitWriterRegularTest(
    String sourceFile,
    String key,
    int chunkSize,
    String[] expectedChunkFiles
  ) throws IOException {
    this.sourceFile = sourceFile;
    this.key = key;
    this.chunkSize = chunkSize;
    this.expectedChunkFiles = expectedChunkFiles;
  }

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);

    writer =
      new FileSplitWriter(
        FileSplitWriterOptions
          .builder()
          .vertxContext(vertx.getOrCreateContext())
          .chunkUploadingCompositeFuturePromise(
            chunkUploadingCompositeFuturePromise
          )
          .outputKey(key)
          .chunkFolder(temporaryFolder.newFolder().getPath())
          .maxRecordsPerChunk(chunkSize)
          .uploadFilesToS3(false)
          .deleteLocalFiles(false)
          .build()
      );
  }

  @Test
  public void testSplit(TestContext context) throws IOException {
    vertx
      .getOrCreateContext()
      .owner()
      .fileSystem()
      .open(sourceFile, new OpenOptions().setRead(true))
      .onComplete(
        context.asyncAssertSuccess(file -> {
          // start the splitting
          file
            .pipeTo(writer)
            .onComplete(
              context.asyncAssertSuccess(v -> {
                // splitting finished, "uploading" in progress
                chunkUploadingCompositeFuturePromise
                  .future()
                  .onComplete(
                    context.asyncAssertSuccess(cf ->
                      cf.onComplete(
                        context.asyncAssertSuccess(internalFuture -> {
                          // "uploading" finished, check the results
                          List<Path> paths = internalFuture
                            .list()
                            .stream()
                            .map(obj -> Path.of((String) obj))
                            .collect(Collectors.toList());
                          List<String> fileNames = paths
                            .stream()
                            .map(path -> path.getFileName().toString())
                            .collect(Collectors.toList());

                          // number of chunks and chunk names
                          assertThat(fileNames, contains(expectedChunkFiles));

                          // read and verify chunking
                          int totalSize = 0;
                          List<byte[]> fileContents = new ArrayList<>();

                          // get each file's contents
                          for (Path path : paths) {
                            File actualFile = path.toFile();
                            totalSize += actualFile.length();
                            try (
                              FileInputStream fileStream = new FileInputStream(
                                actualFile
                              )
                            ) {
                              fileContents.add(fileStream.readAllBytes());
                            } catch (IOException err) {
                              context.fail(err);
                            }
                          }

                          // recombine the split chunks
                          byte[] actual = new byte[totalSize];
                          int pos = 0;
                          for (byte[] content : fileContents) {
                            System.arraycopy(
                              content,
                              0,
                              actual,
                              pos,
                              content.length
                            );
                            pos += content.length;
                          }

                          // verify end delimiter
                          for (byte[] content : fileContents) {
                            if (content.length > 0) { // don't check empty chunks (for empty starting file)
                              assertThat(
                                content[content.length - 1],
                                is(FileSplitUtilities.MARC_RECORD_TERMINATOR)
                              );
                            }
                          }

                          // + 1 is sufficient in case the original file is larger since it will read in
                          // some extra after the splits, which is enough to fail the test
                          file
                            .read(Buffer.buffer(), 0, 0, totalSize + 1)
                            .onComplete(
                              context.asyncAssertSuccess(expectedBuffer -> {
                                try {
                                  byte[] expected = expectedBuffer.getBytes();
                                  // verify the chunks are equivalent
                                  assertThat(actual, is(expected));

                                  // verify counts of records in each are correct
                                  int totalRecords = countRecordsInMarcFile(
                                    new ByteArrayInputStream(actual)
                                  );

                                  for (
                                    int i = 0;
                                    i < fileContents.size();
                                    i++
                                  ) {
                                    if (i == fileContents.size() - 1) {
                                      // the last slice should have all remaining records
                                      assertThat(
                                        countRecordsInMarcFile(
                                          new ByteArrayInputStream(
                                            fileContents.get(i)
                                          )
                                        ),
                                        is(totalRecords)
                                      );
                                    } else {
                                      // all other slices should have a full chunk
                                      assertThat(
                                        countRecordsInMarcFile(
                                          new ByteArrayInputStream(
                                            fileContents.get(i)
                                          )
                                        ),
                                        is(chunkSize)
                                      );
                                      totalRecords -= chunkSize;
                                    }
                                  }

                                  verifyNoInteractions(minioStorageService);
                                } catch (IOException err) {
                                  context.fail(err);
                                }
                              })
                            );
                        })
                      )
                    )
                  );
              })
            );
        })
      );
  }

  private int countRecordsInMarcFile(InputStream stream) throws IOException {
    return FileSplitUtilities.countRecordsInFile(
      "placeholder.mrc",
      stream,
      new JobProfileInfo().withDataType(JobProfileInfo.DataType.MARC)
    );
  }
}
