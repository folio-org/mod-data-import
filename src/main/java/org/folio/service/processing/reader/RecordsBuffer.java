package org.folio.service.processing.reader;

import org.folio.rest.jaxrs.model.Record;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents buffer with source records.
 */
public class RecordsBuffer {

  private List<Record> records;
  private int chunkSize;

  RecordsBuffer(int chunkSize) {
    this.chunkSize = chunkSize;
    this.records = new ArrayList<>(chunkSize);
  }

  public List<Record> getRecords() {
    return this.records;
  }

  public void add(Record records) {
    this.records.add(records);
  }

  public boolean isFull() {
    return this.records.size() >= chunkSize;
  }
}
