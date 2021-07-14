package org.folio.service.processing.reader;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.RecordsMetadata;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Implementation reads source records in json format from the local file system in fixed-size buffer.
 * <code>next</code> method returns buffer content once the buffer is full or the target file has come to the end.
 */
public class MarcJsonReader implements SourceReader {

  private static final Logger LOGGER = LogManager.getLogger();

  public static final String JSON_EXTENSION = "json";
  private JsonReader reader;
  private int chunkSize;
  private MutableInt recordsCounter;

  public MarcJsonReader(File file, int chunkSize) {
    this.chunkSize = chunkSize;
    recordsCounter = new MutableInt(0);
    try {
      this.reader = new JsonReader(new InputStreamReader(FileUtils.openInputStream(file)));
    } catch (IOException e) {
      LOGGER.error("Cannot initialize reader", e);
      throw new RecordsReaderException(e);
    }
  }

  @Override
  public List<InitialRecord> next() {
    RecordsBuffer recordsBuffer = new RecordsBuffer(this.chunkSize);
    try {
      Gson gson = new GsonBuilder().create();
      if (reader.peek().equals(JsonToken.BEGIN_ARRAY)) {
        reader.beginArray();
      }
      while (reader.hasNext()) {
        JsonObject currentRecord = gson.fromJson(reader, JsonObject.class);
        recordsBuffer.add(new InitialRecord().withRecord(currentRecord.toString()).withOrder(recordsCounter.getAndIncrement()));
        if (recordsBuffer.isFull()) {
          return recordsBuffer.getRecords();
        }
      }
    } catch (IOException e) {
      LOGGER.error("Error reading next record", e);
      throw new RecordsReaderException(e);
    }
    return recordsBuffer.getRecords();
  }

  @Override
  public boolean hasNext() {
    try {
      boolean hasNext = reader.hasNext();
      if (!hasNext) {
        reader.close();
      }
      return hasNext;
    } catch (IOException e) {
      LOGGER.error("Error checking for the next record", e);
      throw new RecordsReaderException(e);
    }
  }

  @Override
  public RecordsMetadata.ContentType getContentType() {
    return RecordsMetadata.ContentType.MARC_JSON;
  }
}
