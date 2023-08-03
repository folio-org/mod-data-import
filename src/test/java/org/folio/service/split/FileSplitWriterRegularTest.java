package org.folio.service.split;

import static org.folio.util.VertxMatcherAssert.asyncAssertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.folio.service.processing.split.FileSplitUtilities;
import org.folio.service.processing.split.FileSplitWriter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.OpenOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunnerWithParametersFactory;
import lombok.extern.log4j.Log4j2;

@Log4j2
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(VertxUnitRunnerWithParametersFactory.class)
public class FileSplitWriterRegularTest {

  protected static Vertx vertx = Vertx.vertx();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private String sourceFile;
  private String key;
  private int chunkSize;
  private String[] expectedChunkFiles;

  // tuples of [source file, key, chunk size, expected chunk files[]]
  @Parameters
  public static Collection<Object[]> getCases() {
    return Arrays.asList(

        new Object[] { "src/test/resources/10.mrc", "out.mrc", 11, new String[] { "out_1.mrc" } },
        new Object[] { "src/test/resources/10.mrc", "out.mrc", 10, new String[] { "out_1.mrc" } },
        new Object[] { "src/test/resources/10.mrc", "out.mrc", 9, new String[] { "out_1.mrc", "out_2.mrc" } },
        new Object[] { "src/test/resources/10.mrc", "out.mrc", 5, new String[] { "out_1.mrc", "out_2.mrc" } },
        new Object[] { "src/test/resources/10.mrc", "out.mrc", 3,
            new String[] { "out_1.mrc", "out_2.mrc", "out_3.mrc", "out_4.mrc", } },
        new Object[] { "src/test/resources/10.mrc", "out.mrc", 1,
            new String[] { "out_1.mrc", "out_2.mrc", "out_3.mrc", "out_4.mrc", "out_5.mrc", "out_6.mrc", "out_7.mrc",
                "out_8.mrc", "out_9.mrc", "out_10.mrc" } },

        new Object[] { "src/test/resources/0.mrc", "none.mrc", 1, new String[] { "none_1.mrc" } },

        new Object[] { "src/test/resources/1.mrc", "single.mrc", 10, new String[] { "single_1.mrc" } },
        new Object[] { "src/test/resources/1.mrc", "single.mrc", 1, new String[] { "single_1.mrc" } },

        new Object[] { "src/test/resources/100.mrc", "big.mrc", 60, new String[] { "big_1.mrc", "big_2.mrc" } });

  }

  public FileSplitWriterRegularTest(String sourceFile, String key, int chunkSize, String[] expectedChunkFiles) {
    this.sourceFile = sourceFile;
    this.key = key;
    this.chunkSize = chunkSize;
    this.expectedChunkFiles = expectedChunkFiles;
  }

  @Test
  public void testSplit(TestContext context) throws IOException {
    vertx.getOrCreateContext().owner().fileSystem()
        .open(sourceFile, new OpenOptions().setRead(true))
        .onComplete(context.asyncAssertSuccess(file -> {
          Promise<CompositeFuture> chunkUploadingCompositeFuturePromise = Promise.promise();

          try {
            FileSplitWriter writer = new FileSplitWriter(vertx.getOrCreateContext(),
                chunkUploadingCompositeFuturePromise,
                key, temporaryFolder.newFolder().getPath(),
                chunkSize, FileSplitUtilities.MARC_RECORD_TERMINATOR, false, false);

            // start the splitting
            file.pipeTo(writer).onComplete(context.asyncAssertSuccess(v -> {
              // splitting finished, "uploading" in progress
              chunkUploadingCompositeFuturePromise.future().onComplete(context.asyncAssertSuccess(cf -> {
                // "uploading" finished, check the results
                log.info("{}/{} -> {} = {}", sourceFile, chunkSize, expectedChunkFiles, cf.list());
                List<Path> paths = cf.list().stream().map(obj -> Path.of((String) obj))
                    .collect(Collectors.toList());
                List<String> fileNames = paths.stream().map(path -> path.getFileName().toString())
                    .collect(Collectors.toList());

                // number of chunks and chunk names
                asyncAssertThat(context, fileNames, contains(expectedChunkFiles));

                // read and verify chunking
                int totalSize = 0;
                List<byte[]> fileContents = new ArrayList<>();

                // get each file's contents
                for (Path path : paths) {
                  File actualFile = path.toFile();
                  totalSize += actualFile.length();
                  try (FileInputStream fileStream = new FileInputStream(actualFile)) {
                    fileContents.add(fileStream.readAllBytes());
                  } catch (IOException err) {
                    context.fail(err);
                  }
                }

                // recombine the split chunks
                byte[] actual = new byte[totalSize];
                int pos = 0;
                for (byte[] content : fileContents) {
                  System.arraycopy(content, 0, actual, pos, content.length);
                  pos += content.length;
                }

                // verify end delimiter
                for (byte[] content : fileContents) {
                  if (content.length > 0) { // don't check empty chunks (for empty starting file)
                    asyncAssertThat(context, content[content.length - 1],
                        is(FileSplitUtilities.MARC_RECORD_TERMINATOR));
                  }
                }

                // + 1 is sufficient in case the original file is larger since it will read in
                // some extra after the splits, which is enough to fail the test
                file.read(Buffer.buffer(), 0, 0, totalSize + 1)
                    .onComplete(context.asyncAssertSuccess(expectedBuffer -> {
                      try {
                        byte[] expected = expectedBuffer.getBytes();
                        // verify the chunks are equivalent
                        asyncAssertThat(context, actual, is(expected));

                        // verify counts of records in each are correct
                        int totalRecords = FileSplitUtilities.countRecordsInMarcFile(new ByteArrayInputStream(actual));

                        for (int i = 0; i < fileContents.size(); i++) {
                          if (i == fileContents.size() - 1) {
                            // the last slice should have all remaining records
                            asyncAssertThat(context, FileSplitUtilities.countRecordsInMarcFile(
                                new ByteArrayInputStream(fileContents.get(i))), is(totalRecords));
                          } else {
                            // all other slices should have a full chunk
                            asyncAssertThat(context, FileSplitUtilities.countRecordsInMarcFile(
                                new ByteArrayInputStream(fileContents.get(i))), is(chunkSize));
                            totalRecords -= chunkSize;
                          }
                        }
                      } catch (IOException err) {
                        context.fail(err);
                      }
                    }));
              }));
            }));
          } catch (IOException err) {
            context.fail(err);
          }
        }));
  }
}
