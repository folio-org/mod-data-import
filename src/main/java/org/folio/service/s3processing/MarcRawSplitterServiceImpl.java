package org.folio.service.s3processing;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.exception.FileCannotBeSplitException;
import org.folio.exception.InvalidMarcFileException;
import org.folio.service.s3storage.MinioStorageService;
import org.folio.service.s3storage.S3StorageWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class MarcRawSplitterServiceImpl implements MarcRawSplitterService {

  private static final int BUFFER_SIZE = 8192;

  private static final byte RECORD_TERMINATOR = 29;

  private static final Logger LOGGER = LogManager.getLogger();

  @Autowired
  private MinioStorageService minioStorageService;

  @Autowired
  private Vertx vertx;


  public MarcRawSplitterServiceImpl(
    Vertx vertx,
    MinioStorageService minioStorageService
  ) {
    this.vertx = vertx;
    this.minioStorageService = minioStorageService;
  }

  public Future<Integer> countRecordsInFile(InputStream inStream) {

    Promise<Integer> integerPromise = Promise.promise();

    vertx.executeBlocking(
      (Promise<Integer> blockingFuture) -> {
        try {
          byte[] byteBuffer = new byte[BUFFER_SIZE];
          int numberOfBytes;
          int numRecords = 0;

          int offset = 0;
          do {
            numberOfBytes = inStream.read(byteBuffer, offset, BUFFER_SIZE);
            for (int i = 0; i < numberOfBytes; i++)
              if (byteBuffer[i] == RECORD_TERMINATOR) {
                ++numRecords;
              }
          } while (numberOfBytes >= 0);

          blockingFuture.complete(numRecords);
        } catch (Exception ex) {
          blockingFuture.fail(ex);
        } finally {
          try {
            inStream.close();
          } catch (IOException e) {
            blockingFuture.fail(e);
          }
        }
      },
      (
        AsyncResult<Integer> asyncResult) -> {
        if (asyncResult.failed()) {
          integerPromise.fail(asyncResult.cause());
        } else {
          integerPromise.complete(asyncResult.result());
        }
      }
    );
    return integerPromise.future();

  }


  public Future<Map<Integer, SplitPart>> splitFile(String key, InputStream inStream, int numRecordsPerFile) {

    Promise<Map<Integer, SplitPart>> partsPromise = Promise.promise();

    vertx.executeBlocking(
      (Promise<Map<Integer, SplitPart>> blockingFuture) -> {
        S3StorageWriter partFileWriter = null;
        Map<Integer, SplitPart> partsList = new HashMap<>();
        SplitPart part = null;

        long begin = System.nanoTime();

        try {
          byte[] byteBuffer = new byte[BUFFER_SIZE];
          inStream.mark(0);
          ChunkPlanner planner = new ChunkPlanner(key, inStream, numRecordsPerFile);
          ChunkPlan plan = null;
          plan = planner.planChunking();
          Map<Integer, ChunkPlanFile> chunkPlanFiles = plan.getChunkPlanFiles();

          // Need to reset input stream since -- we are re-reading the file after the output has been planned
          // Issue stream cannot be reset here
          inStream.reset();

          for (int partNumber = 1; partNumber <= chunkPlanFiles.size(); partNumber++) {

            ChunkPlanFile plannedFile = chunkPlanFiles.get(partNumber);

            // Create file
            String partKey = buildPartKey(key, partNumber);
            partFileWriter = minioStorageService.writer(partKey);

            // Read input Stream and write output stream - until all bytes needed for current split file have been written
            int bytesWritten = 0;
            int bytesToWrite = plannedFile.getEndPosition() - plannedFile.getStartPosition() + 1;

            while (bytesWritten < bytesToWrite) {
              int readBufferSize = (bytesToWrite < BUFFER_SIZE) ? bytesToWrite : BUFFER_SIZE;
              int numBytesInBuffer = 0;
              numBytesInBuffer = inStream.read(byteBuffer, 0, readBufferSize);

              partFileWriter.write(byteBuffer, 0, numBytesInBuffer);
              bytesWritten += numBytesInBuffer;
            }
            // Close file
            partFileWriter.close();

            part = new SplitPart(partNumber, partKey);
            part.setNumRecords(plannedFile.getNumberOfRecords());
            part.setBeginRecord(plannedFile.getStartRecord());
            part.setEndRecord(plannedFile.getEndPosition());
            partsList.put(part.getPartNumber(), part);

            LOGGER.info("splitFile:: File number {} - number of records {}, beginning record {}, ending record {}, key {}.",
              part.getPartNumber(),
              part.getNumRecords(),
              part.getBeginRecord(),
              part.getEndRecord(),
              part.getKey());
          }

          long end = System.nanoTime();
          long time = end - begin;

          LOGGER.info("Time to split key {} into {} parts nanoseconds = {} seconds = {}", key, partsList.size(), time, TimeUnit.SECONDS.convert(time, TimeUnit.NANOSECONDS));
          blockingFuture.complete(partsList);
        } catch (Exception e) {
          blockingFuture.fail(e);
        } finally {
          try {
            inStream.close();
            partFileWriter.close();
          } catch (IOException e) {
            blockingFuture.fail(e);
          }
        }
      },
      (
        AsyncResult<Map<Integer, SplitPart>> asyncResult) -> {
        if (asyncResult.failed()) {
          partsPromise.fail(asyncResult.cause());
        } else {
          partsPromise.complete(asyncResult.result());
        }
      }
    );
    return partsPromise.future();
  }

  public Map<Integer, SplitPart> splitFileImmediate(String key, InputStream inStream, int numRecordsPerFile) throws IOException {

    S3StorageWriter partFileWriter = null;
    Map<Integer, SplitPart> partsList = new HashMap<>();
    SplitPart part = null;

    long begin = System.nanoTime();

    try {
      byte[] byteBuffer = new byte[BUFFER_SIZE];
      inStream.mark(0);
      ChunkPlanner planner = new ChunkPlanner(key, inStream, numRecordsPerFile);
      ChunkPlan plan = null;
      plan = planner.planChunking();
      Map<Integer, ChunkPlanFile> chunkPlanFiles = plan.getChunkPlanFiles();

      // Need to reset input stream since -- we are re-reading the file after the output has been planned

      inStream.reset();

      for (int partNumber = 1; partNumber <= chunkPlanFiles.size(); partNumber++) {

        ChunkPlanFile plannedFile = chunkPlanFiles.get(partNumber);

        // Create file
        String partKey = buildPartKey(key, partNumber);
        partFileWriter = minioStorageService.writer(partKey);

        // Read input Stream and write output stream - until all bytes needed for current split file have been written
        int bytesWritten = 0;
        int bytesToWrite = plannedFile.getEndPosition() - plannedFile.getStartPosition() + 1;

        while (bytesWritten < bytesToWrite) {
          int readBufferSize = (bytesToWrite < BUFFER_SIZE) ? bytesToWrite : BUFFER_SIZE;
          int numBytesInBuffer = 0;
          numBytesInBuffer = inStream.read(byteBuffer, 0, readBufferSize);

          partFileWriter.write(byteBuffer, 0, numBytesInBuffer);
          bytesWritten += numBytesInBuffer;
        }
        // Close file
        partFileWriter.close();

        part = new SplitPart(partNumber, partKey);
        part.setNumRecords(plannedFile.getNumberOfRecords());
        part.setBeginRecord(plannedFile.getStartRecord());
        part.setEndRecord(plannedFile.getEndPosition());
        partsList.put(part.getPartNumber(), part);

        LOGGER.info("splitFile:: File number {} - number of records {}, beginning record {}, ending record {}, key {}.",
          part.getPartNumber(),
          part.getNumRecords(),
          part.getBeginRecord(),
          part.getEndRecord(),
          part.getKey());
      }

      long end = System.nanoTime();
      long time = end - begin;

      LOGGER.info("Time to split key {} into {} parts nanoseconds = {} seconds = {}", key, partsList.size(), time, TimeUnit.SECONDS.convert(time, TimeUnit.NANOSECONDS));
      return partsList;
    } catch (Exception e) {
      throw (e);
    } finally {
      try {
        inStream.close();
        partFileWriter.close();
      } catch (IOException e) {
        throw (e);
      }
    }

  }


  private static String buildPartKey(String key, int partNumber) {
    String[] keyNameParts = key.split("\\.");

    if (keyNameParts.length > 1) {
      String partUpdate = String.format(
        "%s_%s",
        keyNameParts[keyNameParts.length - 2],
        partNumber
      );
      keyNameParts[keyNameParts.length - 2] = partUpdate;
      return String.join(".", keyNameParts);
    }
    return String.format(
      "%s_%s",
      key,
      partNumber
    );
  }

}
