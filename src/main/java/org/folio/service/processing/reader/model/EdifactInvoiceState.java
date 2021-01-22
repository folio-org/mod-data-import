package org.folio.service.processing.reader.model;

import org.folio.service.processing.reader.EdifactParser;

import java.util.Map;

public class EdifactInvoiceState extends EdifactState {

  private StringBuilder content = new StringBuilder();

  public EdifactInvoiceState(EdifactParser edifactParser, Map<String, Character> delimiters) {
    super(edifactParser, delimiters);
  }

  @Override
  public void handle(String data) {
    content.append(data).append(getSegmentSeparator());
  }

  @Override
  public String getContent() {
    return content.toString();
  }

  @Override
  public void cleanContent() {
    content.setLength(0);
  }

}
