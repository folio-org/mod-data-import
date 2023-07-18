package org.folio.service.s3processing;

import io.vertx.core.Future;

import java.io.InputStream;

public interface MarcRawSplitterService {

  Future<Integer> countRecordsInFile(InputStream inStream);
}
