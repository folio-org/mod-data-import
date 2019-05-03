package org.folio.service.processing.reader;

import org.folio.rest.jaxrs.model.JobProfileInfo;

import java.io.File;

import static org.folio.rest.RestVerticle.MODULE_SPECIFIC_ARGS;

/**
 * Builds source reader depending on job profile type
 */
public class SourceReaderBuilder {

  private static final int CHUNK_SIZE =
    Integer.parseInt(MODULE_SPECIFIC_ARGS.getOrDefault("file.processing.buffer.chunk.size", "50"));
  private static final String JSON_EXTENSION = ".json";

  private SourceReaderBuilder() {
  }

  public static SourceReader build(File file, JobProfileInfo jobProfile) {
    if (jobProfile != null
      && jobProfile.getDataType() != null
      && jobProfile.getDataType().equals(JobProfileInfo.DataType.MARC)) {
      String name = file.getName();
      if (JSON_EXTENSION.equals(name.substring(name.lastIndexOf('.')))) {
        return new MarcJsonReader(file, CHUNK_SIZE);
      }
      return new MarcRawReader(file, CHUNK_SIZE);
    }
    throw new UnsupportedOperationException("Unsupported file format");
  }
}
