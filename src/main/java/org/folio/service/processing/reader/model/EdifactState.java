package org.folio.service.processing.reader.model;

import static io.xlate.edi.stream.EDIStreamConstants.Delimiters.DATA_ELEMENT;
import static io.xlate.edi.stream.EDIStreamConstants.Delimiters.SEGMENT;
import static org.folio.service.processing.reader.model.EdifactState.Type.FINISH_INVOICE;
import static org.folio.service.processing.reader.model.EdifactState.Type.FOOTER;
import static org.folio.service.processing.reader.model.EdifactState.Type.HEADER;
import static org.folio.service.processing.reader.model.EdifactState.Type.INVOICE;

import java.util.Map;
import org.folio.service.processing.reader.EdifactParser;

/**
 * EdifactState is an abstract class for splitting EDIFACT on separate invoices.
 */
public abstract class EdifactState {

  public static final String NUMBER_INVOICES_IN_CHUNK = "1";

  public static final String INTERCHANGE_TAG = "UNB";
  public static final String START_INVOICE_TAG = "UNH";
  public static final String END_INVOICE_TAG = "UNT";
  public static final String MESSAGE_END = "UNZ";
  final EdifactParser parser;
  final Map<String, Character> delimiters;
  private Type position = HEADER;

  protected EdifactState(EdifactParser edifactParser, Map<String, Character> delimiters) {
    this.parser = edifactParser;
    this.delimiters = delimiters;
  }

  public Type getCurrentLogicalPositionInFile(String data) {
    if (position.equals(Type.HEADER)) {
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

  public String getFooterTemplate() {
    return MESSAGE_END + getDataElementSeparator() + NUMBER_INVOICES_IN_CHUNK + getDataElementSeparator();
  }

  public String getSegmentSeparator() {
    return String.valueOf(delimiters.get(SEGMENT));
  }

  public String getDataElementSeparator() {
    return String.valueOf(delimiters.get(DATA_ELEMENT));
  }

  private Type headerOrStartInvoice(String data) {
    return (data.contains(START_INVOICE_TAG + getDataElementSeparator())) ? INVOICE : HEADER;
  }

  private Type startOrFinishInvoice(String data) {
    return (data.contains(END_INVOICE_TAG + getDataElementSeparator())) ? FINISH_INVOICE : INVOICE;
  }

  private Type footerOrStartInvoice(String data) {
    return (data.contains(MESSAGE_END + getDataElementSeparator())) ? FOOTER : INVOICE;
  }

  public enum Type {
    HEADER,
    INVOICE,
    FINISH_INVOICE,
    FOOTER
  }
}
