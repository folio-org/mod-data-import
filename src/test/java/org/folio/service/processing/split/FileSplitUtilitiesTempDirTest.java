package org.folio.service.processing.split;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import java.io.File;
import java.io.IOException;
import org.junit.Test;

public class FileSplitUtilitiesTempDirTest {

  @Test
  public void testTemporaryDirectory() throws IOException {
    File tempDir = FileSplitUtilities.createTemporaryDir("test-key").toFile();

    // in case assertions fail
    tempDir.deleteOnExit();

    assertThat(tempDir.exists(), is(true));
    assertThat(tempDir.isDirectory(), is(true));
    assertThat(tempDir.getPath(), containsString("test-key"));

    tempDir.delete();
  }
}
