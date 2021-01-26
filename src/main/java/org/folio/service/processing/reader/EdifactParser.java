package org.folio.service.processing.reader;

import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.service.processing.reader.model.EdifactFooterState;
import org.folio.service.processing.reader.model.EdifactGeneralState;
import org.folio.service.processing.reader.model.EdifactInvoiceFinishState;
import org.folio.service.processing.reader.model.EdifactState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.folio.service.processing.reader.model.EdifactState.TYPE;
import static org.folio.service.processing.reader.model.EdifactState.TYPE.FINISH_INVOICE;
import static org.folio.service.processing.reader.model.EdifactState.TYPE.FOOTER;
import static org.folio.service.processing.reader.model.EdifactState.TYPE.HEADER;
import static org.folio.service.processing.reader.model.EdifactState.TYPE.INVOICE;

/**
 * Parses the EDIFACT file into chunks corresponding to the invoices.
 */
public class EdifactParser {

  private final EdifactState state;
  private final Map<TYPE, EdifactState> handlers;
  private List<InitialRecord> invoices = new ArrayList<>();

  /**
   * Init EdifactParser.
   *
   * @param delimiters - delimiters for current EDIFACT file version.
   */
  public EdifactParser(Map<String, Character> delimiters) {
    handlers = Map.of(HEADER, new EdifactGeneralState(this, delimiters),
      INVOICE, new EdifactGeneralState(this, delimiters),
      FINISH_INVOICE, new EdifactInvoiceFinishState(this, delimiters),
      FOOTER, new EdifactFooterState(this, delimiters));
    this.state = handlers.get(HEADER);
  }

  /**
   * Returns segment separator.
   *
   * @return segment separator.
   */
  public String getSegmentSeparator() {
    return state.getSegmentSeparator();
  }

  /**
   * Returns EDIFACT header.
   *
   * @return - EDIFACT header.
   */
  public String getHeader() {
    return handlers.get(HEADER).getContent();
  }

  /**
   * Returns EDIFACT invoice record.
   *
   * @return - EDIFACT invoice record.
   */
  public String getInvoiceBody() {
    return handlers.get(INVOICE).getContent();
  }

  /**
   * Cleans collected EDIFACT invoice record.
   */
  public void cleanInvoiceBody() {
    handlers.get(INVOICE).cleanContent();
  }

  /**
   * Resulting collection of InitialRecords that contains separated invoices.
   *
   * @return - list of InitialRecords
   */
  public List<InitialRecord> getInitialRecords() {
    return invoices;
  }

  /**
   * Main method of the current parser. Executes the whole process of file parsing.
   *
   * @param data - EDIFACT file segment
   */
  public void handle(String data) {
    TYPE position = state.getCurrentLogicalPositionInFile(data);
    handlers.get(position).handle(data);
  }

}
