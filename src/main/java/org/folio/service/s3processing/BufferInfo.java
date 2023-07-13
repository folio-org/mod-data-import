package org.folio.service.s3processing;
import java.util.ArrayList;
import java.util.List;

public class BufferInfo {

  private List<Integer> RecordTerminatorPositions;
  private int numCompleteRecordsInBuffer;

  public BufferInfo(byte[] byteBuffer, int numberOfBytes, byte recordTerminatorCharacter) {

    // Get end record indicators in the buffer
    RecordTerminatorPositions = new ArrayList<>();

    for (int i = 0; i < numberOfBytes; i++) {
      if (byteBuffer[i] == recordTerminatorCharacter) {
        RecordTerminatorPositions.add(i);
      }
    }
    numCompleteRecordsInBuffer = RecordTerminatorPositions.size();
  }

  public int getNumCompleteRecordsInBuffer() {
    return numCompleteRecordsInBuffer;
  }

  public int getRecordTerminatorPosition(int recordNumber) {
    return RecordTerminatorPositions.get(recordNumber - 1);
  }

}

