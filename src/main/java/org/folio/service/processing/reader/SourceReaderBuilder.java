package org.folio.service.processing.reader;

import org.apache.commons.io.FilenameUtils;
import org.folio.rest.jaxrs.model.JobProfileInfo;

import java.io.File;

import static java.util.Optional.ofNullable;
import static org.folio.rest.RestVerticle.MODULE_SPECIFIC_ARGS;
import static org.folio.service.processing.reader.MarcJsonReader.JSON_EXTENSION;
import static org.folio.service.processing.reader.MarcXmlReader.XML_EXTENSION;

/**
 * Builds source reader depending on job profile type
 */
public class SourceReaderBuilder {

  private static final String CHUNK_SIZE_KEY = "file.processing.buffer.chunk.size";
  private static final int CHUNK_SIZE = Integer.parseInt(MODULE_SPECIFIC_ARGS.getOrDefault(CHUNK_SIZE_KEY, "50"));


  private SourceReaderBuilder() {
  }

  public static SourceReader build(File file, JobProfileInfo jobProfile) {
    SourceReader sourceReader = null;
    String extension = FilenameUtils.getExtension(file.getName());

    if (isMarc(jobProfile)) {
      if (JSON_EXTENSION.equals(extension)) {
        sourceReader = new MarcJsonReader(file, CHUNK_SIZE);
      } else if (XML_EXTENSION.equals(extension)) {
        sourceReader = new MarcXmlReader(file, CHUNK_SIZE);
      } else {
        sourceReader = new MarcRawReader(file, CHUNK_SIZE);
      }
    }

    return ofNullable(sourceReader).orElseThrow(() -> new UnsupportedOperationException("Unsupported file format"));
  }

  private static boolean isMarc(JobProfileInfo jobProfile) {
    return jobProfile != null && jobProfile.getDataType() == JobProfileInfo.DataType.MARC;
  }
}
