package org.folio.service.processing.split;

import static org.assertj.core.api.Assertions.assertThat;

import io.vertx.core.Promise;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileSplitWriterUnusedMethodTest {

  @TempDir
  Path tempDir;

  @Test
  @DisplayName(
    "should return self from setWriteQueueMaxSize, return false from writeQueueFull, and return self from drainHandler")
  void shouldReturnSelfFromQueueMethods() throws IOException {
    FileSplitWriter writer = new FileSplitWriter(
      FileSplitWriterOptions
        .builder()
        .chunkUploadingCompositeFuturePromise(Promise.promise())
        .outputKey("")
        .chunkFolder(Files.createTempDirectory(tempDir, "").toString())
        .maxRecordsPerChunk(1)
        .uploadFilesToS3(false)
        .deleteLocalFiles(false)
        .build()
    );

    assertThat(writer.setWriteQueueMaxSize(0)).isSameAs(writer);
    assertThat(writer.writeQueueFull()).isFalse();
    assertThat(writer.drainHandler(null)).isSameAs(writer);
  }
}
