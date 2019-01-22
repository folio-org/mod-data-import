package org.folio.service.processing.reader;


import org.folio.rest.jaxrs.model.JobProfile;

import java.io.File;

/**
 * Builds source reader depending on job profile type
 */
public class SourceReaderBuilder {

  public static SourceReader build(File file, JobProfile jobProfile) { // NOSONAR
    // TODO find proper SourceReader by profile file type
    return new MarcSourceReader(file);
  }
}
