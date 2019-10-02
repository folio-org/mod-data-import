package org.folio.service.processing.reader;

import org.folio.rest.jaxrs.model.InitialRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents buffer with source records.
 */
public class RecordsBuffer {

  private List<InitialRecord> records;
  private int chunkSize;

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
