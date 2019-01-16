package org.folio.service.processing.reader;

import java.util.ArrayList;
import java.util.List;

import static org.folio.rest.RestVerticle.MODULE_SPECIFIC_ARGS;

/**
 * Represents buffer with source records.
 */
public class RecordsBuffer {

  private static final int CHUNK_SIZE =
    Integer.parseInt(MODULE_SPECIFIC_ARGS.getOrDefault("file.processing.buffer.chunk.size", "100"));

  private List<String> records = new ArrayList<>(CHUNK_SIZE);

  RecordsBuffer() {
  }

  public List<String> getRecords() {
    return this.records;
  }

  public void add(List<String> records) {
    this.records.addAll(records);
  }

  public boolean isFull() {
    return this.records.size() > CHUNK_SIZE;
  }
}
