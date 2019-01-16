package org.folio.service.processing.reader;

import java.util.List;


/**
 * The root interface for traversing and partitioning elements of a source records.
 */
public interface SourceReader {

  /**
   * Reads next chunk of raw records
   *
   * @return list of source records
   */
  List<String> readNext();

  /**
   * Releases any system resources associated with the file reading process.
   */
  void close();
}
