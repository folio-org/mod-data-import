package org.folio.service.processing.reader;

import io.vertx.core.Future;
import io.vertx.core.file.FileSystem;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.rest.jaxrs.model.RawRecordsDto;

/**
 * Stub implementation, reads sample data from the file system.
 */
public class FileSystemStubSourceReader implements SourceReader {

  private static final Logger logger = LoggerFactory.getLogger(FileSystemStubSourceReader.class);
  private static final String SAMPLE_RECORDS_FILE_PATH = "src/main/resources/sample/records/marcRecord.sample";
  private FileSystem fileSystem;

  public FileSystemStubSourceReader(FileSystem fileSystem) {
    this.fileSystem = fileSystem;
  }

  @Override
  public Future<RawRecordsDto> readNext() {
    Future<RawRecordsDto> future = Future.future();
    fileSystem.readFile(SAMPLE_RECORDS_FILE_PATH, fileAr -> {
      if (fileAr.failed()) {
        logger.error("Can not read file by source path: " + SAMPLE_RECORDS_FILE_PATH);
        future.fail(fileAr.cause());
      } else {
        RawRecordsDto chunk = new RawRecordsDto();
        chunk.setLast(true);
        chunk.setTotal(1);
        chunk.getRecords().add(fileAr.result().toString());
        future.complete(chunk);
      }
    });
    return future;
  }

  @Override
  public boolean hasNext() {
    return false;
  }
}
