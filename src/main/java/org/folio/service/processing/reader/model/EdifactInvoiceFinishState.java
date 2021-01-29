package org.folio.service.processing.reader.model;

import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.service.processing.reader.EdifactParser;

import java.util.List;
import java.util.Map;

public class EdifactInvoiceFinishState extends EdifactState {

  public EdifactInvoiceFinishState(EdifactParser edifactParser, Map<String, Character> delimiters) {
    super(edifactParser, delimiters);
  }

  @Override
  public void handle(String data) {
    String content = parser.getHeader()
      + parser.getInvoiceBody() + data + getSegmentSeparator();

    List<InitialRecord> records = parser.getInitialRecords();
    records.add(initInitialRecord(content).withOrder(records.size() + 1));
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
