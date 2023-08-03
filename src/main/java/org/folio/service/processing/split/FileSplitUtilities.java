package org.folio.service.processing.split;

import java.io.IOException;
import java.io.InputStream;

public class FileSplitUtilities {

  public static final byte MARC_RECORD_TERMINATOR = (byte) 0x1d;

  private static final int BUFFER_SIZE = 8192;

  private FileSplitUtilities() {
    throw new UnsupportedOperationException(
        "Cannot instantiate utility class.");
  }

  /**
   * Creates the S3 key for a split chunk within a larger file
   */
  public static String buildChunkKey(String baseKey, int partNumber) {
    String[] keyNameParts = baseKey.split("\\.");

    if (keyNameParts.length > 1) {
      String partUpdate = String.format(
          "%s_%s",
          keyNameParts[keyNameParts.length - 2],
          partNumber);
      keyNameParts[keyNameParts.length - 2] = partUpdate;
      return String.join(".", keyNameParts);
    }

    return String.format("%s_%s", baseKey, partNumber);
  }

  /**
   * Counts records in a given {@link InputStream}, closing it afterwards.
   *
   * @throws IOException if the stream cannot be read
   */
  public static int countRecordsInMarcFile(InputStream inStream)
      throws IOException {
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
