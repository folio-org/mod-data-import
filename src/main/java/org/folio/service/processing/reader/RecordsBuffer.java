package org.folio.service.processing.reader;

import java.util.ArrayList;
import java.util.List;
import org.folio.rest.jaxrs.model.InitialRecord;

/**
 * Represents buffer with source records.
 */
public class RecordsBuffer {

  private final List<InitialRecord> records;
  private final int chunkSize;

  RecordsBuffer(int chunkSize) {
    this.chunkSize = chunkSize;
    this.records = new ArrayList<>(chunkSize);
  }

  public List<InitialRecord> getRecords() {
    return this.records;
  }

  public void add(InitialRecord records) {
    this.records.add(records);
  }

  public boolean isFull() {
    return this.records.size() >= chunkSize;
  }
}
