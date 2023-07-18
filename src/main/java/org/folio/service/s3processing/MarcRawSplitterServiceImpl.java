package org.folio.service.s3processing;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.service.s3storage.MinioStorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;

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
}
