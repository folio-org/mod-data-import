package org.folio.service.processing.reader;

import io.xlate.edi.stream.EDIInputFactory;
import io.xlate.edi.stream.EDIStreamEvent;
import io.xlate.edi.stream.EDIStreamException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.RecordsMetadata;

/**
 * Implementation reads source records in EDIFACT format from the local file system.
 * <code>next</code> method returns list of {@link InitialRecord InitialRecord.class}
 * when the target file has come to the end.
 */
public class EdifactReader implements SourceReader {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final Charset DEFAULT_CHARSET = StandardCharsets.ISO_8859_1;
  private final int chunkSize;
  private EdifactParser edifactParser;
  private Stream<String> segmentsStream;
  private Iterator<String> segmentIterator;

  public EdifactReader(File file, int chunkSize) {
    this.chunkSize = chunkSize;
    try (var stream = new FileInputStream(file)) {
      try (var reader = EDIInputFactory.newFactory().createEDIStreamReader(stream)) {
        if (reader.next() == EDIStreamEvent.START_INTERCHANGE) {
          var delimiters = reader.getDelimiters();
          edifactParser = new EdifactParser(delimiters);
          segmentsStream = Files.lines(file.toPath(), DEFAULT_CHARSET)
            .map(l -> l.split(edifactParser.getSegmentSeparator()))
            .flatMap(Arrays::stream);
          segmentIterator = segmentsStream.iterator();
        }
      }
    } catch (IOException e) {
      LOGGER.warn("EdifactReader:: Error during handling the file: {}", file.getName(), e);
      throw new RecordsReaderException(e);
    } catch (EDIStreamException e) {
      LOGGER.warn("EdifactReader:: Can not initialize reader. Cause: {}", e.getMessage());
      throw new RecordsReaderException(e);
    }
  }

  @Override
  public List<InitialRecord> next() {
    edifactParser.cleanInitialRecords();
    while (segmentIterator.hasNext() && edifactParser.getInitialRecords().size() <= chunkSize) {
      edifactParser.handle(segmentIterator.next());
    }
    return edifactParser.getInitialRecords();
  }

  @Override
  public boolean hasNext() {
    return edifactParser.hasNext();
  }

  @Override
  public RecordsMetadata.ContentType getContentType() {
    return RecordsMetadata.ContentType.EDIFACT_RAW;
  }

  @Override
  public void close() {
    if (segmentsStream != null) {
      segmentsStream.close();
    }
  }
}

