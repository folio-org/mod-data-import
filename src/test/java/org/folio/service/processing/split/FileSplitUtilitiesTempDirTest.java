package org.folio.service.processing.split;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FileSplitUtilitiesTempDirTest {

  @Test
  @DisplayName("should create temporary directory containing the key name")
  void shouldCreateTempDir_containingKeyName() throws IOException {
    File tempDir = FileSplitUtilities.createTemporaryDir("test-key").toFile();
    tempDir.deleteOnExit();

    assertThat(tempDir).exists().isDirectory();
    assertThat(tempDir.getPath()).contains("test-key");

    tempDir.delete();
  }
}
