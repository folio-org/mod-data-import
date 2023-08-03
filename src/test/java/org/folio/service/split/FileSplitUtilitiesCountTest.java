package org.folio.service.split;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.folio.service.processing.split.FileSplitUtilities;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class FileSplitUtilitiesCountTest {

  private static final String EMPTY_MARC_SOURCE_PATH = "src/test/resources/0.mrc";

  // fits in one buffer read
  private static final String VALID_1_MARC_SOURCE_PATH = "src/test/resources/1.mrc";
  private static final int VALID_1_MARC_RECORD_COUNT = 1;

  // multiple buffer reads
  private static final String VALID_100_MARC_SOURCE_PATH = "src/test/resources/100.mrc";
  private static final int VALID_100_MARC_RECORD_COUNT = 100;

  // invalid format
  private static final String INVALID_MARC_SOURCE_PATH = "src/test/resources/invalidMarcFile.mrc";

  // tuples of [path, number of records]
  @Parameters
  public static Collection<Object[]> getCases() {
    return Arrays.asList(
        new Object[] { EMPTY_MARC_SOURCE_PATH, 0 },
        new Object[] { VALID_1_MARC_SOURCE_PATH, VALID_1_MARC_RECORD_COUNT },
        new Object[] { VALID_100_MARC_SOURCE_PATH, VALID_100_MARC_RECORD_COUNT },
        new Object[] { INVALID_MARC_SOURCE_PATH, 0 });
  }

  private String path;
  private int count;

  public FileSplitUtilitiesCountTest(String path, int count) {
    this.path = path;
    this.count = count;
  }

  @Test
  public void testCountAndStreamClose()
      throws IOException {
    BufferedInputStream inputStream = new BufferedInputStream(
        new FileInputStream(path));

    assertThat(FileSplitUtilities.countRecordsInMarcFile(inputStream), is(count));

    // countRecordsInMarcFile closes the stream, so it should be unavailable after
    // call, throwing an IOException
    assertThrows(IOException.class, () -> inputStream.available());
  }
}
