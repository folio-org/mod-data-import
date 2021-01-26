package org.folio.service.processing.reader;

import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.service.processing.reader.model.EdifactFooterState;
import org.folio.service.processing.reader.model.EdifactHeaderState;
import org.folio.service.processing.reader.model.EdifactInvoiceFinishState;
import org.folio.service.processing.reader.model.EdifactInvoiceState;
import org.folio.service.processing.reader.model.EdifactState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.folio.service.processing.reader.model.EdifactState.TYPE;
import static org.folio.service.processing.reader.model.EdifactState.TYPE.FINISH_INVOICE;
import static org.folio.service.processing.reader.model.EdifactState.TYPE.FOOTER;
import static org.folio.service.processing.reader.model.EdifactState.TYPE.HEADER;
import static org.folio.service.processing.reader.model.EdifactState.TYPE.INVOICE;

public class EdifactParser {

  private final EdifactState state;
  private final Map<TYPE, EdifactState> handlers;
  private List<InitialRecord> invoices = new ArrayList<>();

  public EdifactParser(Map<String, Character> delimiters) {
    handlers = Map.of(HEADER, new EdifactHeaderState(this, delimiters),
      INVOICE, new EdifactInvoiceState(this, delimiters),
      FINISH_INVOICE, new EdifactInvoiceFinishState(this, delimiters),
      FOOTER, new EdifactFooterState(this, delimiters));
    this.state = handlers.get(HEADER);
  }

  public String getSegmentSeparator() {
    return state.getSegmentSeparator();
  }

  public String getHeaderContent() {
    return handlers.get(HEADER).getContent();
  }

  public String getInvoiceContent() {
    return handlers.get(INVOICE).getContent();
  }

  public void cleanInvoiceContent() {
    handlers.get(INVOICE).cleanContent();
  }

  public List<InitialRecord> getInitialRecords() {
    return invoices;
  }

  public void handle(String data) {
    TYPE position = state.getCurrentLogicalPositionInFile(data);
    handlers.get(position).handle(data);
  }

}
