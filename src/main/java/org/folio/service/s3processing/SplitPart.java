package org.folio.service.s3processing;

public class SplitPart {

  public void setPartNumber(int partNumber) {
    this.partNumber = partNumber;
  }

  public void setS3Key(String Key) {
    this.key = Key;
  }

  public void setBeginRecord(int beginRecord) {
    this.beginRecord = beginRecord;
  }

  public void setEndRecord(int endRecord) {
    this.endRecord = endRecord;
  }

  public void setNumRecords(int numRecords) {
    this.numRecords = numRecords;
  }

  public int getPartNumber() {
    return partNumber;
  }

  public String getS3Key() {
    return key;
  }

  public int getBeginRecord() {
    return beginRecord;
  }

  public int getEndRecord() {
    return endRecord;
  }

  public int getNumRecords() {
    return numRecords;
  }

  private int partNumber;

  private String key;

  private int beginRecord;

  private int endRecord;

  private int numRecords;

  public SplitPart(int partNumber, String key) {
    this.partNumber = partNumber;
    this.key = key;
  }
}
