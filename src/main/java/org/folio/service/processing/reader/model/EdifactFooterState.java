package org.folio.service.processing.reader.model;

import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.service.processing.reader.EdifactParser;

import java.util.Map;

public class EdifactFooterState extends EdifactState {

  StringBuilder content = new StringBuilder("UNZ").append(getDataElementSeparator())
    .append("1").append(getDataElementSeparator());

  public EdifactFooterState(EdifactParser edifactParser, Map<String, Character> delimiters) {
    super(edifactParser, delimiters);
  }

  @Override
  public void handle(String data) {
    content.append(data.substring(data.lastIndexOf(getDataElementSeparator()) + 1)).append(getSegmentSeparator());
    postUpdate(content.toString());
  }

  @Override
  public String getContent() {
    return content.toString();
  }

  @Override
  public void cleanContent() {
    content.setLength(0);
  }

  private void postUpdate(String footer) {
    for (InitialRecord initialRecord : parser.getInitialRecords()) {
      initialRecord.setRecord(initialRecord.getRecord() + footer);
    }
  }

}
