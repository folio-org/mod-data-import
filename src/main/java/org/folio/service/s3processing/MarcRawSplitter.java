package org.folio.service.s3processing;

import io.vertx.core.Future;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public interface MarcRawSplitter {

  Future<Integer> countRecordsInFile(InputStream inStream) throws IOException;

  Future<Map<Integer, SplitPart>> splitFile(String key, InputStream inStream, int numRecordsPerFile);
}
