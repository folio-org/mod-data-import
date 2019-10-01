package org.folio.service.processing.reader;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.lang3.mutable.MutableInt;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordsMetadata;

import java.io.File;
import java.util.Iterator;
import java.util.List;

/**
 * It reads marc records from an xml file by specified size of chunk.
 */
public class MarcXmlReader implements SourceReader {
  public static final String XML_EXTENSION = "xml";
  private static final Logger LOGGER = LoggerFactory.getLogger(MarcXmlReader.class);
  private Document document;
  private int chunkSize;
  private Iterator iterator;
  private MutableInt recordsCounter;

  public MarcXmlReader(File file, int chunkSize) {
    this.chunkSize = chunkSize;
    recordsCounter = new MutableInt(0);
    try {
      this.document = new SAXReader().read(file);
    } catch (DocumentException e) {
      LOGGER.error("Can not read the xml file: %s", e, file);
      throw new RecordsReaderException(e);
    }
    this.iterator = document.getRootElement().elementIterator();
  }

  @Override
  public List<Record> next() {
    RecordsBuffer buffer = new RecordsBuffer(this.chunkSize);
    while (iterator.hasNext()) {
      Element element = (Element) iterator.next();
      buffer.add(new Record().withRecord(element.asXML()).withOrder(recordsCounter.getAndIncrement()));
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
  public RecordsMetadata.ContentType getContentType() {
    return RecordsMetadata.ContentType.MARC_XML;
  }
}
