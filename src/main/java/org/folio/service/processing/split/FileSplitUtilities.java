package org.folio.service.processing.split;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.NoArgsConstructor;
import lombok.AccessLevel;

import java.io.IOException;
import java.io.InputStream;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FileSplitUtilities {

  public static final byte MARC_RECORD_TERMINATOR = (byte) 0x1d;

  private static final int BUFFER_SIZE = 8192;

  public static String buildPartKey(String key, int partNumber) {
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

  public static Integer countRecordsInMarcFile(InputStream inStream) throws IOException {
    try {
      byte[] byteBuffer = new byte[BUFFER_SIZE];
      int numberOfBytes;
      int numRecords = 0;

      int offset = 0;
      do {
        numberOfBytes = inStream.read(byteBuffer, offset, BUFFER_SIZE);
        for (int i = 0; i < numberOfBytes; i++)
          if (byteBuffer[i] == MARC_RECORD_TERMINATOR) {
            ++numRecords;
          }
      } while (numberOfBytes >= 0);
      return numRecords;
    } finally {
      inStream.close();
    }
  }
}
