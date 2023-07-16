package org.folio.service.s3processing;

import java.util.*;

public class ChunkPlan {

  private Map<Integer, ChunkPlanRecord> chunkPlanRecords = null;

  private Map<Integer, ChunkPlanFile> chunkPlanFiles = null;


  public ChunkPlan() {
    chunkPlanRecords = new LinkedHashMap<Integer, ChunkPlanRecord>();
    chunkPlanFiles = new LinkedHashMap<Integer, ChunkPlanFile>();
  }

  public void addRecord(int recordNumber, int startPosition, int endPosition) {
    chunkPlanRecords.put(recordNumber, new ChunkPlanRecord(recordNumber, startPosition, endPosition));
  }

  public void addFile(int fileNumber, int startRecord, int endRecord, int startPosition, int endPosition, int numberRecords) {
    chunkPlanFiles.put(fileNumber, new ChunkPlanFile(fileNumber, startRecord, endRecord, startPosition, endPosition, numberRecords));
  }

  public Map<Integer, ChunkPlanRecord> getChunkPlanRecords() {
    return chunkPlanRecords;
  }


  public Map<Integer, ChunkPlanFile> getChunkPlanFiles() {
    return chunkPlanFiles;
  }

}
