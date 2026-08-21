package org.folio.service.processing.reader.model;

import java.util.Map;
import org.folio.service.processing.reader.EdifactParser;

/**
 * EdifactFooterState is a class for return EDIFACT invoice and for receiving ControlReference from UNB tag.
 * ControlReference value will be use for building footer segment when processing with chunks.
 */
public class EdifactGeneralState extends EdifactState {

  private final StringBuilder content = new StringBuilder();
  private String controlReference = "";

  public EdifactGeneralState(EdifactParser edifactParser, Map<String, Character> delimiters) {
    super(edifactParser, delimiters);
  }

  @Override
  public void handle(String data) {
    if (data.contains(INTERCHANGE_TAG + getDataElementSeparator())) {
      controlReference = data.substring(data.lastIndexOf(getDataElementSeparator()) + 1);
    }
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

  public String getControlReference() {
    return controlReference;
  }
}
