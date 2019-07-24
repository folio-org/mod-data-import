package org.folio.service.processing.reader;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.io.FileUtils;
import org.folio.rest.jaxrs.model.RecordsMetadata;
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
public class MarcRawReader implements SourceReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(MarcRawReader.class);
  private static final Charset CHARSET = Charset.forName(MODULE_SPECIFIC_ARGS.getOrDefault("file.processing.buffer.record.charset", "UTF8"));
  private RawRecordReader reader;
  private int chunkSize;

  public MarcRawReader(File file, int chunkSize) {
    this.chunkSize = chunkSize;
    try {
      this.reader = new RawRecordReader(FileUtils.openInputStream(file));
    } catch (IOException e) {
      String errorMessage = "Can not initialize reader. Cause: " + e.getMessage();
      LOGGER.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }
  }

  @Override
  public List<String> next() {
    RecordsBuffer recordsBuffer = new RecordsBuffer(this.chunkSize);
    while (this.reader.hasNext()) {
      RawRecord rawRecord = this.reader.next();
      recordsBuffer.add(new String(rawRecord.getRecordBytes(), CHARSET));
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

  @Override
  public RecordsMetadata.ContentType getContentType() {
    return RecordsMetadata.ContentType.MARC_RAW;
  }
}
