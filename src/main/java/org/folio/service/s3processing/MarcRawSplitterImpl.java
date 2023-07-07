package org.folio.service.s3processing;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.service.s3storage.MinioStorageService;
import org.folio.service.s3storage.RemoteStorageByteWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class MarcRawSplitterImpl implements MarcRawSplitter {

  private static final int BUFFER_SIZE = 8192;

  private static final byte RECORD_TERMINATOR = 29;

  private static final Logger LOGGER = LogManager.getLogger();

  @Autowired
  private MinioStorageService minioStorageService;

  @Autowired
  private Vertx vertx;

  public Future<Integer> countRecordsInFile(InputStream inStream) throws IOException {

    Promise<Integer> integerPromise = Promise.promise();

    vertx.executeBlocking(
      (Promise<Integer> blockingFuture) -> {
        try {
          byte[] byteBuffer = new byte[BUFFER_SIZE];
          int numberOfBytes;
          int numRecords=0;

          int offset = 0;
          do {
            numberOfBytes = inStream.read(byteBuffer, offset, BUFFER_SIZE);
            for (int i =0; i < numberOfBytes ; i++)
              if (byteBuffer[i] == (byte) RECORD_TERMINATOR) {
                ++numRecords;
              }
          } while (numberOfBytes >= 0);

          blockingFuture.complete(numRecords);
        } catch (Exception ex) {
          blockingFuture.fail(ex);
        }
        finally {
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

        RemoteStorageByteWriter partFileWriter = null;

        Map<Integer, SplitPart> partsList = new HashMap<>();
        SplitPart part = null;

        try {
          byte[] byteBuffer = new byte[BUFFER_SIZE];
          int numberOfBytes;
          boolean needNewSplitFile = true;
          int partNumber = 0;
          int numRecordsInFile = 0;
          int totalRecordsWritten = 0;

          while ( (numberOfBytes = inStream.read(byteBuffer, 0, BUFFER_SIZE) ) > 0)
          {
            BufferInfo bufferInfo = new BufferInfo(byteBuffer, numberOfBytes, RECORD_TERMINATOR);

            if (needNewSplitFile) {
              ++partNumber;
              String partKey = buildPartKey(key, partNumber);
              partFileWriter = minioStorageService.writer(partKey);
              part = new SplitPart(partNumber, key);
              needNewSplitFile = false;
            }

            if (bufferInfo.getNumCompleteRecordsInBuffer() == 0) {
              // No ending records are in buffer -- write complete buffer out to new file
              partFileWriter.write(byteBuffer, 0, numberOfBytes);
            } else {
              int recordsNeeded = numRecordsPerFile - numRecordsInFile;
              if (recordsNeeded > bufferInfo.getNumCompleteRecordsInBuffer()) {
                // Write all records from buffer including any partial records
                // More Records will need to be added from next chunk
                partFileWriter.write(byteBuffer, 0, numberOfBytes);
                numRecordsInFile+= bufferInfo.getNumCompleteRecordsInBuffer();
              } else  {
                // recordsNeeded <= num_marc_records_in_buffer
                partFileWriter.write(byteBuffer, 0, bufferInfo.getRecordTerminatorPosition(recordsNeeded) + 1);
                partFileWriter.close();
                part.setNumRecords(numRecordsInFile);
                part.setBeginRecord(totalRecordsWritten+1);
                part.setEndRecord(totalRecordsWritten+numRecordsInFile);
                partsList.put(part.getPartNumber(), part);
                totalRecordsWritten += numRecordsInFile;
                LOGGER.info("splitFile:: File number {} successfully processed.", part.getPartNumber());
                LOGGER.info("splitFile:: number of records in file. {}", part.getNumRecords());
                LOGGER.info("splitFile:: beginning record.{}", part.getBeginRecord());
                LOGGER.info("splitFile:: ending record {}", part.getEndRecord());
                LOGGER.info("splitFile:: s3 key {}", part.getS3Key());


                int fullRecordsToWrite = bufferInfo.getNumCompleteRecordsInBuffer() - recordsNeeded;

                int bufferPosition = 0;
                if (fullRecordsToWrite > 0) {
                  bufferPosition = bufferInfo.getRecordTerminatorPosition(recordsNeeded) + 1;
                } else {
                  bufferPosition = bufferInfo.getPartialRecordPosition();
                }
                numRecordsInFile = 0;

                // Determine what is left in the buffer
                // it could be part of a record or multiple records + part of a record
                // Any Partial record(s) in buffer need to go into the next file
                if ((bufferPosition < numberOfBytes)&&(bufferPosition != -1)) {
                  ++partNumber;
                  String outfile = buildPartKey(key, partNumber);

                  partFileWriter = minioStorageService.writer(outfile);
                  part = new SplitPart(partNumber, key);
                  partFileWriter.write(byteBuffer, bufferPosition, numberOfBytes - bufferPosition);
                  numRecordsInFile = fullRecordsToWrite;
                  needNewSplitFile = false;
                }
              }
            }

          }
          blockingFuture.complete(partsList);
        } catch (FileNotFoundException e) {
          blockingFuture.fail(e);
        } catch (IOException e) {
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

  private static String buildPartKey(String key, int partNumber) {
    // 1K.mrc part number = 1 - returns 1K_1.mrc
    String[] keyNameParts = key.split("\\.");

    if (keyNameParts.length > 1) {
      String partUpdate = String.format(
        "%s_%s",
        keyNameParts[keyNameParts.length - 2] ,
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
