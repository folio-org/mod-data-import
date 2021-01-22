package org.folio.service.processing.reader.model;

import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.service.processing.reader.EdifactParser;

import java.util.Map;

public class EdifactInvoiceFinishState extends EdifactState {

  public EdifactInvoiceFinishState(EdifactParser edifactParser, Map<String, Character> delimiters) {
    super(edifactParser, delimiters);
  }

  @Override
  public void handle(String data) {
    String content = parser.getHeaderContent()
      + parser.getInvoiceContent() + data + getSegmentSeparator();
    parser.getInitialRecords().add(getInitialRecord(content));
    parser.cleanInvoiceContent();
  }

  @Override
  public String getContent() {
    throw new IllegalArgumentException("Not implemented.");
  }

  @Override
  public void cleanContent() {
    throw new IllegalArgumentException("Not implemented.");
  }

  private InitialRecord getInitialRecord(String content) {
    InitialRecord initialRecord = new InitialRecord();
    initialRecord.setRecord(content);
    return initialRecord;
  }

}
