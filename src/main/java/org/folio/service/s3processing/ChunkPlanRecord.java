package org.folio.service.s3processing;

public class ChunkPlanRecord {

  public int getStartPosition() {
    return startPosition;
  }

  public int getEndPosition() {
    return endPosition;
  }

  public int getRecordNumber() {
    return recordNumber;
  }

  private int startPosition;

  private int endPosition;

  private int recordNumber;

  public ChunkPlanRecord(int recordNumber, int startPosition, int endPosition) {
    this.startPosition = startPosition;
    this.endPosition = endPosition;
    this.recordNumber = recordNumber;
  }
}
