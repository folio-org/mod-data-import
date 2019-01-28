package org.folio.service.processing.reader;


import org.folio.rest.jaxrs.model.JobProfile;

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

  public static SourceReader build(File file, JobProfile jobProfile) { // NOSONAR
    // find proper SourceReader by profile file type
    return new MarcSourceReader(file, CHUNK_SIZE);
  }
}
