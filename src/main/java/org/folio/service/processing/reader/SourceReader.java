package org.folio.service.processing.reader;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.RawRecordsDto;


/**
 * The root interface for traversing and partitioning elements of a source records.
 */
public interface SourceReader {

  /**
   * Reads next chunk of raw records
   *
   * @return Future
   */
  Future<RawRecordsDto> readNext();

  /**
   * Returns {@code true} if the iteration has more elements.
   *
   * @return boolean
   */
  boolean hasNext();
}
