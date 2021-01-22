package org.folio.service.processing.reader.model;

import org.folio.service.processing.reader.EdifactParser;

import java.util.Map;

import static io.xlate.edi.stream.EDIStreamConstants.Delimiters.DATA_ELEMENT;
import static io.xlate.edi.stream.EDIStreamConstants.Delimiters.SEGMENT;
import static org.folio.service.processing.reader.model.EdifactState.TYPE.HEADER;
import static org.folio.service.processing.reader.model.EdifactState.TYPE.INVOICE;

public abstract class EdifactState {

  public enum TYPE {
    HEADER,
    INVOICE,
    FINISH_INVOICE,
    FOOTER
  }

  final EdifactParser parser;
  final Map<String, Character> delimiters;

  public EdifactState(EdifactParser edifactParser, Map<String, Character> delimiters) {
    this.parser = edifactParser;
    this.delimiters = delimiters;
  }

  private TYPE position = HEADER;
  public TYPE getCurrentPosition(String data) {
    if (position.equals(TYPE.HEADER)) {
      position = headerOrStartInvoice(data);
    } else if (position.equals(INVOICE)) {
      position = startOrFinishInvoice(data);
    } else {
      position = footerOrStartInvoice(data);
    }
    return position;
  }

  public abstract void handle(String data);

  public abstract String getContent();

  public abstract void cleanContent();

  public String getSegmentSeparator() {
    return String.valueOf(delimiters.get(SEGMENT));
  }

  public String getDataElementSeparator() {
    return  String.valueOf(delimiters.get(DATA_ELEMENT));
  }

  private TYPE headerOrStartInvoice(String data) {
    return (data.contains("UNH" + getDataElementSeparator())) ? INVOICE : TYPE.HEADER;
  }

  private TYPE startOrFinishInvoice(String data) {
    return (data.contains("UNT" + getDataElementSeparator())) ? TYPE.FINISH_INVOICE : INVOICE;
  }

  private TYPE footerOrStartInvoice(String data) {
    return (data.contains("UNZ" + getDataElementSeparator())) ? TYPE.FOOTER : INVOICE;
  }

}
