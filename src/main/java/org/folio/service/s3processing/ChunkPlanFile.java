package org.folio.service.s3processing;

public class ChunkPlanFile {

  public int getStartRecord() {
    return startRecord;
  }

  public int getStopRecord() {
    return stopRecord;
  }

  public int getStartPosition() {
    return startPosition;
  }

  public int getEndPosition() {
    return endPosition;
  }

  public int getFileNumber() {
    return fileNumber;
  }

  public int getNumberOfRecords() {
    return numberOfRecords;
  }

  private int startRecord;

  private int stopRecord;

  private int startPosition;

  private int endPosition;

  private int fileNumber;

  private int numberOfRecords;

  public ChunkPlanFile(int fileNumber, int startRecord, int stopRecord, int startPosition, int endPosition, int numberOfRecords) {
    this.startRecord = startRecord;
    this.stopRecord = stopRecord;
    this.startPosition = startPosition;
    this.endPosition = endPosition;
    this.fileNumber = fileNumber;
    this.numberOfRecords = numberOfRecords;
  }
}
