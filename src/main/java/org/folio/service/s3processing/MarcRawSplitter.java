package org.folio.service.s3processing;

import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.service.s3storage.MinioStorageService;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.IntStream;


public class MarcRawSplitter  {

  private static final int BUFFER_SIZE = 8192;

  private static final byte RECORD_TERMINATOR = 29;

  private static final Logger LOGGER = LogManager.getLogger();

  private String key;

  private InputStream inStream;

  private int numRecordsPerFile;

  private byte[] byteBuffer;

  @Autowired
  private MinioStorageService minioStorageService;

  @Autowired
  private Vertx vertx;

  public MarcRawSplitter(String key, InputStream inStream, int numRecordsPerFile) {
    this.key = key;
    this.inStream = inStream;
    this.numRecordsPerFile = numRecordsPerFile;
    this.byteBuffer = new byte[BUFFER_SIZE];
  }


  public Map<Integer, SplitPart> splitFile() throws IOException {

    int numberOfBytes = 0;
    int numberOfParts = 0;

    while ((numberOfBytes = inStream.read(byteBuffer, 0, BUFFER_SIZE)) > 0) {

      ByteBuffer buffer = ByteBuffer.wrap(byteBuffer);
      int[] recordTerminators = getRecordTerminators(buffer, RECORD_TERMINATOR);
      boolean bufferNeedsProcessing = true;

      while (bufferNeedsProcessing)  {



      }


    }

    return null;

  }

  private SplitPart createNewPartFile() {
    return null;
  }

  private void writeBytesToPartFile(SplitPart part) {
    return;
  }

  private void writeRecordsToPartFile(SplitPart part) {
    return;
  }

  private void closePartFile(SplitPart part) {
    return;
  }

  private static int[] getRecordTerminators(ByteBuffer buf, byte b) {
    return IntStream.range(buf.position(), buf.limit())
      .filter(i -> buf.get(i) == b)
      .toArray();
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
