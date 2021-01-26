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
    } catch (IOException | EDIStreamException e) {
      String errorMessage = "Can not initialize reader. Cause: " + e.getMessage();
      LOGGER.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }
  }


  @Override
  public List<InitialRecord> next() {
    try {
      Files.lines(file.toPath(), DEFAULT_CHARSET).map(l -> l.split(edifactParser.getSegmentSeparator()))
        .flatMap(Arrays::stream).forEach(edifactParser::handle);
    } catch (IOException e) {
      String errorMessage = "Error during handling the file: " + file.getName();
      LOGGER.error(errorMessage, e);
      throw new IllegalArgumentException(errorMessage);
    }
    return edifactParser.getInitialRecords();
  }

  @Override
  public boolean hasNext() {
    throw new IllegalArgumentException("Not implemented yet.");
  }

  @Override
  public RecordsMetadata.ContentType getContentType() {
    return RecordsMetadata.ContentType.EDI;
  }
}

