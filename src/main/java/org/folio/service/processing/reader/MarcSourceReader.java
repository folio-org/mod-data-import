package org.folio.service.processing.reader;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.io.FileUtils;
import org.marc4j.util.RawRecord;
import org.marc4j.util.RawRecordReader;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
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
  private static final Charset READ_BUFFER_CHARSET
    = Charset.forName(MODULE_SPECIFIC_ARGS.getOrDefault("file.processing.buffer.record.charset", "UTF_8"));

  private RawRecordReader reader;
  private File file;

  public MarcSourceReader(File file) {
    this.file = file;
    try {
      this.reader = new RawRecordReader(FileUtils.openInputStream(this.file));
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
      RawRecord rawRecord = reader.next();
      recordsBuffer.add(new String(rawRecord.getRecordBytes(), READ_BUFFER_CHARSET));
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
