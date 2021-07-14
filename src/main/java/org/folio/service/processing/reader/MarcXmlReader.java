package org.folio.service.processing.reader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.lang3.mutable.MutableInt;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.RecordsMetadata;

import java.io.File;
import java.util.Iterator;
import java.util.List;

/**
 * It reads marc records from an xml file by specified size of chunk.
 */
public class MarcXmlReader implements SourceReader {

  private static final Logger LOGGER = LogManager.getLogger();

  public static final String XML_EXTENSION = "xml";
  private Document document;
  private int chunkSize;
  private Iterator<Element> iterator;
  private MutableInt recordsCounter;

  public MarcXmlReader(File file, int chunkSize) {
    this.chunkSize = chunkSize;
    recordsCounter = new MutableInt(0);
    try {
      // SAXReader.createDefault() prevents XXE attacks by
      // disabling external DTDs and External Entities
      // https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-10683
      // https://github.com/dom4j/dom4j/releases/tag/version-2.1.3
      this.document = SAXReader.createDefault().read(file);
    } catch (DocumentException e) {
      LOGGER.error("Can not read the xml file: {}", file, e);
      throw new RecordsReaderException(e);
    }
    this.iterator = document.getRootElement().elementIterator();
  }

  @Override
  public List<InitialRecord> next() {
    RecordsBuffer buffer = new RecordsBuffer(this.chunkSize);
    while (iterator.hasNext()) {
      Element element = (Element) iterator.next();
      buffer.add(new InitialRecord().withRecord(element.asXML()).withOrder(recordsCounter.getAndIncrement()));
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
