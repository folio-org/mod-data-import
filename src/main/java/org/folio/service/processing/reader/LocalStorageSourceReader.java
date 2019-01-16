package org.folio.service.processing.reader;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.service.processing.splitter.RecordSplitter;
import org.folio.service.processing.splitter.RecordSplitterBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.List;

import static org.folio.rest.RestVerticle.MODULE_SPECIFIC_ARGS;

/**
 * Implementation reads source records from the local file system in fixed-size buffer.
 * Once the buffer is read, the implementation calls {@link RecordSplitter} to divide source buffer into records.
 * <p>
 * Read iteration may cache (<code>sourceCache<code/>) the last record in order to prevent handling of partially-read record entry.
 * If the last record is cached (<code>sourceCache<code/>), next read iteration appends it to the beginning of source buffer.
 *
 * @see RecordSplitter
 */
public class LocalStorageSourceReader implements SourceReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalStorageSourceReader.class);
  private static final int READ_BUFFER_SIZE =
    Integer.parseInt(MODULE_SPECIFIC_ARGS.getOrDefault("file.processing.buffer.read.size", "1024"));
  private static final Charset READ_BUFFER_CHARSET
    = Charset.forName(MODULE_SPECIFIC_ARGS.getOrDefault("file.processing.buffer.read.charset", "UTF_8"));

  private File file;
  private RecordSplitter recordSplitter;
  private InputStreamReader inputStreamReader;
  private StringBuilder sourceCache = new StringBuilder();
  private boolean hasNext = true;

  public LocalStorageSourceReader(File file) {
    this.file = file;
    initInputStreamReader();
  }

  @Override
  public List<String> readNext() {
    RecordsBuffer recordsBuffer = new RecordsBuffer();
    /* Read data to the RecordsBuffer while it is not full and the file has not come to the end. */
    while (!recordsBuffer.isFull() && this.hasNext) {
      CharBuffer readBuffer = CharBuffer.allocate(READ_BUFFER_SIZE);
      try {
        while (this.inputStreamReader.read(readBuffer) > 0) {
          readBuffer.flip();
          this.hasNext = readBuffer.remaining() == readBuffer.capacity();
          String source = this.sourceCache.append(readBuffer).toString();
          readBuffer.clear();
          List<String> records = splitSourceIntoRecords(source);
          if (this.hasNext) {
            /*
              The last record may be read partially, we need to cache it
              in order to append it to the source which is read on the next iteration.
            */
            String lastRecord = records.remove(records.size() - 1);
            this.sourceCache.append(lastRecord);
          } else {
            this.close();
          }
          recordsBuffer.add(records);
        }
      } catch (IOException e) {
        readBuffer.clear();
        this.close();
        LOGGER.error("Can not read next buffer the file: {}. Cause: {}.", this.file.getPath(), e.getCause());
//      TODO throw runtime exception
      }
    }
    return recordsBuffer.getRecords();
  }

  @Override
  public void close() {
    this.hasNext = false;
    this.sourceCache = null;
    try {
      this.inputStreamReader.close();
    } catch (IOException e) {
      LOGGER.error("Can not close stream reader for the file: {}. Cause: {}.", this.file.getPath(), e.getCause());
      // TODO throw runtime exception
    }
  }

  private void initInputStreamReader() {
    if (this.inputStreamReader == null) {
      try {
        InputStream inputStream = new FileInputStream(this.file);
        /*  When the InputStreamReader is closed it will also close the InputStream instance it reads from. */
        try (InputStreamReader inputStreamReader = new InputStreamReader(inputStream, READ_BUFFER_CHARSET)) {
          this.inputStreamReader = inputStreamReader;
        } catch (IOException e) {
          LOGGER.error("Can not close InputStream for the file: {}. Cause: {}.", this.file.getPath(), e.getCause());
          // TODO throw new RuntimeException
        }
      } catch (FileNotFoundException e) {
        LOGGER.error("Can not open file: {}. Cause: {}.", this.file.getPath(), e.getCause());
        // TODO throw new RuntimeException
      }
    }
  }

  private List<String> splitSourceIntoRecords(String source) {
    if (recordSplitter == null) {
      recordSplitter = RecordSplitterBuilder.build(source);
    }
    return recordSplitter.split(source);
  }
}
