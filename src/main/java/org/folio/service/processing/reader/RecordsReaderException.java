package org.folio.service.processing.reader;

/**
 * Exception that indicates an error while reading a file with records,
 * basically it is a wrapper for the IOException
 */
public class RecordsReaderException extends RuntimeException {

  public RecordsReaderException(Throwable cause) {
    super(cause);
  }

}
