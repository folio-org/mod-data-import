package org.folio.service.s3processing;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ChunkPlanner {

  private String key;

  private InputStream inStream;

  private int numRecordsPerFile;

  private byte[] byteBuffer;

  private static final int BUFFER_SIZE = 8192;

  private static final byte RECORD_TERMINATOR = 29;

  private ChunkPlan chunkPlan;


  public ChunkPlanner(String key, InputStream inStream, int numRecordsPerFile) {
    this.key = key;
    this.inStream = inStream;
    this.numRecordsPerFile = numRecordsPerFile;
    this.chunkPlan = new ChunkPlan();
  }

  public ChunkPlan planChunking() throws IOException {

    byte[] byteBuffer = new byte[BUFFER_SIZE];
    int numberOfBytesInBuffer = 0;
    int numberOfBytesPreviousBuffers = 0;
    List<Integer> terminatorPositions = new ArrayList<>();

    // Step 1 -- get all terminators in buffer and count number of records
    while ((numberOfBytesInBuffer = inStream.read(byteBuffer, 0, BUFFER_SIZE)) > 0) {
      terminatorPositions.addAll(getRecordTerminators(byteBuffer, numberOfBytesInBuffer, RECORD_TERMINATOR, numberOfBytesPreviousBuffers));
      numberOfBytesPreviousBuffers += numberOfBytesInBuffer;
    }
    //Step 2 -- Using list of terminator positions -- create a record plan
    // List of (record number, start position in overall stream, end position in overall stream)
    AtomicInteger startPosition = new AtomicInteger();
    AtomicInteger recordNumber = new AtomicInteger();
    terminatorPositions.stream().forEach(t ->
      {
        recordNumber.set(recordNumber.get() + 1);
        chunkPlan.addRecord(recordNumber.get(), startPosition.get(), t );
        startPosition.set(t + 1);
      }
    );

    // Step 3 -- Using list of planned records -- create a file plan
    // List of (file number, start Record, end Record, start position in overall stream, end position in overall stream)
    int numberOfRecordsInFile = chunkPlan.getChunkPlanRecords().size();
    int numberCompleteFiles = numberOfRecordsInFile / numRecordsPerFile;
    int remainder = numberOfRecordsInFile % numRecordsPerFile;

    int startRecordInFile = 1;

    for (int i = 0; i < numberCompleteFiles; i++) {
      ChunkPlanRecord startRecord = chunkPlan.getChunkPlanRecords().get(startRecordInFile);
      ChunkPlanRecord endRecord = chunkPlan.getChunkPlanRecords().get(startRecordInFile+numRecordsPerFile-1);
      chunkPlan.addFile(i + 1, startRecordInFile ,startRecordInFile+numRecordsPerFile-1,startRecord.getStartPosition(),endRecord.getEndPosition(), numRecordsPerFile);
      startRecordInFile += numRecordsPerFile;
    }

    if (remainder > 0) {
      ChunkPlanRecord startRecord = chunkPlan.getChunkPlanRecords().get(startRecordInFile);
      ChunkPlanRecord endRecord = chunkPlan.getChunkPlanRecords().get(startRecordInFile+remainder-1);

      chunkPlan.addFile(numberCompleteFiles+1, startRecordInFile ,startRecordInFile+remainder-1, startRecord.getStartPosition(),endRecord.getEndPosition(), remainder);
    }

    return chunkPlan;
  }

  private static List<Integer> getRecordTerminators(byte[] byteBuffer, int numberOfBytesInBuffer, byte b, int numberOfBytesInPreviousBuffers) {

    List<Integer> recordTerminatorPositions = new ArrayList<>();

    for (int i = 0; i < numberOfBytesInBuffer; i++) {
      if (byteBuffer[i] == b) {
        recordTerminatorPositions.add(i + numberOfBytesInPreviousBuffers);
      }
    }
    return recordTerminatorPositions;

  }
}
