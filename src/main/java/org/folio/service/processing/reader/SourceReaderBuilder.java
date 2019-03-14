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

  private SourceReaderBuilder() {
  }

  public static SourceReader build(File file, JobProfileInfo jobProfile) {
    switch (jobProfile.getDataType()) {
      case MARC: {
        return new MarcSourceReader(file, CHUNK_SIZE);
      }
      default: {
        throw new UnsupportedOperationException("Another file format doesn't supports yet");
      }
    }
  }
}
