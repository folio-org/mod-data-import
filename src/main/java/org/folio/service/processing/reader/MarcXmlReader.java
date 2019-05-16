package org.folio.service.processing.reader;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.folio.rest.jaxrs.model.RawRecordsDto.ContentType;

/**
 * It reads marc records from an xml file by specified size of chunk.
 */
public class MarcXmlReader implements SourceReader {
  public static final String XML_EXTENSION = "xml";
  private static final Logger LOGGER = LoggerFactory.getLogger(MarcXmlReader.class);
  private Document document;
  private int chunkSize;
  private Iterator iterator;

  public MarcXmlReader(File file, int chunkSize) {
    this.chunkSize = chunkSize;
    try {
      this.document = new SAXReader().read(file);
    } catch (DocumentException e) {
      LOGGER.error("Can not read the xml file: %s", e, file);
      throw new RecordsReaderException(e);
    }
    this.iterator = document.getRootElement().elementIterator();
  }

  @Override
  public List<String> next() {
    RecordsBuffer buffer = new RecordsBuffer(this.chunkSize);
    while (iterator.hasNext()) {
      Element element = (Element) iterator.next();
      buffer.add(element.asXML());
      if (buffer.isFull()) {
        break;
      }
    }
    return buffer.getRecords();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public ContentType getContentType() {
    return ContentType.MARC_XML;
  }
}
