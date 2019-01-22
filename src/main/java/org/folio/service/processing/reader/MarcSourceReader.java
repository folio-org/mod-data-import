package org.folio.service.processing.reader;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.io.FileUtils;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamReader;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.folio.rest.RestVerticle.MODULE_SPECIFIC_ARGS;

/**
 * Implementation reads source records from the local file system in fixed-size buffer.
 * <code>next</code> method returns buffer content once the buffer is full or the target file has come to the end.
 */
public class MarcSourceReader implements SourceReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(MarcSourceReader.class);
  private static final int CHUNK_SIZE =
    Integer.parseInt(MODULE_SPECIFIC_ARGS.getOrDefault("file.processing.buffer.chunk.size", "50"));

  private MarcReader reader;
  private File file;

  public MarcSourceReader(File file) {
    this.file = file;
    try {
      this.reader = new MarcStreamReader(FileUtils.openInputStream(this.file));
    } catch (IOException e) {
      String errorMessage = "Can not initialize reader. Cause: " + e.getCause();
      LOGGER.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }
  }

  @Override
  public List<String> next() {
    RecordsBuffer recordsBuffer = new RecordsBuffer(CHUNK_SIZE);
    while (this.reader.hasNext()) {
      recordsBuffer.add(reader.next().toString());
      if (recordsBuffer.isFull()) {
        return recordsBuffer.getRecords();
      }
    }
    return recordsBuffer.getRecords();
  }

  @Override
  public boolean hasNext() {
    return this.reader.hasNext();
  }
}
