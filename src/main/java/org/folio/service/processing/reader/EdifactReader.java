package org.folio.service.processing.reader;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.xlate.edi.stream.EDIInputFactory;
import io.xlate.edi.stream.EDIStreamEvent;
import io.xlate.edi.stream.EDIStreamException;
import io.xlate.edi.stream.EDIStreamReader;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.RecordsMetadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Implementation reads source records in EDIFACT format from the local file system.
 * <code>next</code> method returns list of {@link InitialRecord} when the target file has come to the end.
 */
public class EdifactReader implements SourceReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(EdifactReader.class);
  private static final Charset DEFAULT_CHARSET = StandardCharsets.ISO_8859_1;

  private EDIStreamReader reader;
  private EdifactParser edifactParser;
  private File file;
  private Map<String, Character> delimiters = null;

  public EdifactReader(File file) {
    try {
      this.file = file;
      EDIInputFactory factory = EDIInputFactory.newFactory();
      InputStream stream = new FileInputStream(file);
      reader = factory.createEDIStreamReader(stream);
      if (reader.next() == EDIStreamEvent.START_INTERCHANGE) {
        delimiters = reader.getDelimiters();
        edifactParser = new EdifactParser(delimiters);
      }
    } catch (IOException e) {
      LOGGER.error("Error during handling the file: " + file.getName(), e);
      throw new RecordsReaderException(e);
    } catch (EDIStreamException e) {
      LOGGER.error("Can not initialize reader. Cause: " + e.getMessage());
      throw new RecordsReaderException(e);
    }
  }

  @Override
  public List<InitialRecord> next() {
    try {
      Files.lines(file.toPath(), DEFAULT_CHARSET).map(l -> l.split(edifactParser.getSegmentSeparator()))
        .flatMap(Arrays::stream).forEach(edifactParser::handle);
    } catch (IOException e) {
      LOGGER.error("Error during handling the file: " + file.getName(), e);
      throw new RecordsReaderException(e);
    }
    return edifactParser.getInitialRecords();
  }

  @Override
  public boolean hasNext() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public RecordsMetadata.ContentType getContentType() {
    return RecordsMetadata.ContentType.EDIFACT_RAW;
  }
}

