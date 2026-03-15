package org.folio.service.processing.reader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.folio.kafka.SimpleConfigurationReader;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.RecordsMetadata;
import org.marc4j.MarcException;
import org.marc4j.MarcPermissiveStreamReader;
import org.marc4j.MarcStreamWriter;
import org.marc4j.marc.Record;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Implementation reads source records from the local file system in fixed-size buffer.
 * <code>next</code> method returns buffer content once the buffer is full or the target file has come to the end.
 */
public class MarcRawReader implements SourceReader {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final String RECORD_CHARSET_PARAM = "file.processing.buffer.record.charset";

  private final Charset charset;
  private MarcPermissiveStreamReader reader;
  private InputStream inputStream;
  private int chunkSize;
  private MutableInt recordsCounter;

  public MarcRawReader(File file, int chunkSize) {
    String charsetConfig = SimpleConfigurationReader.getValue(RECORD_CHARSET_PARAM, StandardCharsets.UTF_8.name());
    this.charset = Charset.forName(charsetConfig);
    this.chunkSize = chunkSize;
    recordsCounter = new MutableInt(0);

    try {
      this.inputStream = FileUtils.openInputStream(file);
      this.reader = new MarcPermissiveStreamReader(inputStream, true, true);
    } catch (IOException e) {
      String errorMessage = "Can not initialize reader. Cause: " + e.getMessage();
      LOGGER.warn(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }
  }

  @Override
  public List<InitialRecord> next() {
    RecordsBuffer recordsBuffer = new RecordsBuffer(this.chunkSize);
    while (this.reader.hasNext()) {
      Record rawRecord;
      try {
        rawRecord = this.reader.next();
      } catch (MarcException e) {
        LOGGER.warn("next:: Something happened when getting next raw record", e);
        throw new RecordsReaderException(e);
      }

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      MarcStreamWriter streamWriter = new MarcStreamWriter(bos, charset.name());
      streamWriter.write(rawRecord);
      streamWriter.close();
      recordsBuffer.add(new InitialRecord().withRecord(bos.toString(charset)).withOrder(recordsCounter.getAndIncrement()));
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

  @Override
  public void close() {
    try {
      if (inputStream != null) {
        inputStream.close();
      }
    } catch (IOException e) {
      LOGGER.warn("close:: Error closing input stream", e);
    }
  }
}
