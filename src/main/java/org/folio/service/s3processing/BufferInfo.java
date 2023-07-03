package org.folio.service.s3processing;
import java.util.ArrayList;
import java.util.List;

public class BufferInfo {

  private List<Integer> RecordTerminatorPositions;
  private int numRecordsInBuffer;
  private boolean partialRecordInBuffer;
  private int partialRecordPosition;
  private int bufferSize;

  public BufferInfo(byte[] byteBuffer, int numberOfBytes) {

    // Get end record indicators in the buffer
    RecordTerminatorPositions = new ArrayList<>();

    for (int i = 0; i < numberOfBytes; i++) {
      if (byteBuffer[i] == (byte) 29) {
        RecordTerminatorPositions.add(i);
      }
    }

    bufferSize = numberOfBytes;
    numRecordsInBuffer = RecordTerminatorPositions.size();
    int lastRecordEndPosition = RecordTerminatorPositions.get(RecordTerminatorPositions.size() - 1);
    partialRecordPosition = lastRecordEndPosition + 1;

    partialRecordInBuffer = partialRecordPosition < numberOfBytes;
    if (!partialRecordInBuffer) {
      partialRecordPosition = -1;
    }

  }

  public int getNumRecordsInBuffer() {
    return numRecordsInBuffer;
  }

  public boolean isPartialRecordInBuffer() {
    return partialRecordInBuffer;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public int getPartialRecordPosition() {
    return partialRecordPosition;
  }

  public int getRecordTerminatorPosition(int recordNumber) {
    return RecordTerminatorPositions.get(recordNumber - 1);
  }
}

