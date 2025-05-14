package org.folio.service.processing.reader.model;

import org.folio.service.processing.reader.EdifactParser;

import java.util.Map;

/**
 * EdifactFooterState is a class for return EDIFACT footer segment.
 * This class have to used when processing in a stream.
 */
public class EdifactFooterState extends EdifactState {

  private StringBuilder content = new StringBuilder(getFooterTemplate());

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
    parser.getInitialRecords().forEach(initialRecord -> {
      int unzIndex = initialRecord.getRecord().indexOf(MESSAGE_END + getDataElementSeparator());
      if (unzIndex > -1) {
        initialRecord.setRecord(initialRecord.getRecord().substring(0, unzIndex) + footer);
      }
    });
  }

}
