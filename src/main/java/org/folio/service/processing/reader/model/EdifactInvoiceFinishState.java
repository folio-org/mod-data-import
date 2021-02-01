package org.folio.service.processing.reader.model;

import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.service.processing.reader.EdifactParser;

import java.util.Map;

/**
 * EdifactInvoiceFinishState is a class for preparing {@link InitialRecord InitialRecord.class} collection.
 */
public class EdifactInvoiceFinishState extends EdifactState {

  public EdifactInvoiceFinishState(EdifactParser edifactParser, Map<String, Character> delimiters) {
    super(edifactParser, delimiters);
  }

  @Override
  public void handle(String data) {
    String content = parser.getHeader() + parser.getInvoiceBody() + data + getSegmentSeparator()
      + getFooterTemplate() + parser.getControlReferenceValue() + getSegmentSeparator();
    parser.getInitialRecords().add(initInitialRecord(content).withOrder(parser.getInitialRecords().size()));
    parser.cleanInvoiceBody();
  }

  @Override
  public String getContent() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public void cleanContent() {
    throw new UnsupportedOperationException("Not supported.");
  }

  private InitialRecord initInitialRecord(String content) {
    InitialRecord initialRecord = new InitialRecord();
    initialRecord.setRecord(content);
    return initialRecord;
  }

}
