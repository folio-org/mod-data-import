package org.folio.service.processing.reader;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents buffer with source records.
 */
public class RecordsBuffer {

  private List<String> records;
  private int chunkSize;

  RecordsBuffer(int chunkSize) {
    this.chunkSize = chunkSize;
    this.records = new ArrayList<>(chunkSize);
  }

  public List<String> getRecords() {
    return this.records;
  }

  public void add(List<String> records) {
    this.records.addAll(records);
  }

  public boolean isFull() {
    return this.records.size() >= chunkSize;
  }
}
