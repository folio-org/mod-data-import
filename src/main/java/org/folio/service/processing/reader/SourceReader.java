package org.folio.service.processing.reader;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.RawRecordsDto;


/**
 * The root interface for traversing and partitioning elements of a source records.
 */
public interface SourceReader {

  Future<RawRecordsDto> readNext();

  boolean hasNext();
}
