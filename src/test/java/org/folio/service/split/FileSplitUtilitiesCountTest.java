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

  // tuples of [path, number of records]
  @Parameters
  public static Collection<Object[]> getCases() {
    return Arrays.asList(
      new Object[] { "src/test/resources/0.mrc", 0 },
      // 1 buffer read
      new Object[] { "src/test/resources/1.mrc", 1 },
      // multiple buffer reads
      new Object[] { "src/test/resources/100.mrc", 100 },
      new Object[] { "src/test/resources/2500.mrc", 2500 },
      new Object[] { "src/test/resources/5000.mrc", 5000 },
      new Object[] { "src/test/resources/10000.mrc", 10000 },
      new Object[] { "src/test/resources/22778.mrc", 22778 },
      new Object[] { "src/test/resources/50000.mrc", 50000 },
      new Object[] { "src/test/resources/invalidMarcFile.mrc", 0 }
    );
  }

  private String path;
  private int count;

  public FileSplitUtilitiesCountTest(String path, int count) {
    this.path = path;
    this.count = count;
  }

  @Test
  public void testCountAndStreamClose() throws IOException {
    BufferedInputStream inputStream = new BufferedInputStream(
      new FileInputStream(path)
    );

    assertThat(
      FileSplitUtilities.countRecordsInMarcFile(inputStream),
      is(count)
    );

    // countRecordsInMarcFile closes the stream, so it should be unavailable after
    // call, throwing an IOException
    assertThrows(IOException.class, () -> inputStream.available());
  }
}
