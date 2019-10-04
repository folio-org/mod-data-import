package org.folio.service.processing.reader;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.RecordsMetadata;
import org.marc4j.MarcPermissiveStreamReader;
import org.marc4j.MarcStreamWriter;
import org.marc4j.marc.Record;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
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
  private static final Logger logger = LoggerFactory.getLogger(MarcRawReader.class);
  private MarcPermissiveStreamReader reader;
  private int chunkSize;
  private MutableInt recordsCounter;

  public MarcRawReader(File file, int chunkSize) {
    this.chunkSize = chunkSize;
    recordsCounter = new MutableInt(0);
    try {
      this.reader = new MarcPermissiveStreamReader(FileUtils.openInputStream(file), true, true);
    } catch (IOException e) {
      String errorMessage = "Can not initialize reader. Cause: " + e.getMessage();
      LOGGER.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }
  }

  @Override
  public List<InitialRecord> next() {
    RecordsBuffer recordsBuffer = new RecordsBuffer(this.chunkSize);
    while (this.reader.hasNext()) {
      Record rawRecord = this.reader.next();
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      MarcStreamWriter streamWriter = new MarcStreamWriter(bos, CHARSET.name());
      streamWriter.write(rawRecord);
      streamWriter.close();
      try {
        recordsBuffer.add(new InitialRecord().withRecord(bos.toString(CHARSET.name())).withOrder(recordsCounter.getAndIncrement()));
      } catch (UnsupportedEncodingException e) {
        logger.error("Error during reading MARC record. Record will be skipped.", e);
      }
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
