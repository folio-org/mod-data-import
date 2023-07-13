package org.folio.service.s3storage;

public interface StorageWriter {

  void write(byte[] buffer, int offset, int numberBytes);

  void close();
}
