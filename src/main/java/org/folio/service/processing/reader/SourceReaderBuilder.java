package org.folio.service.processing.reader;

import org.apache.commons.io.FilenameUtils;
import org.folio.rest.jaxrs.model.JobProfileInfo;

import java.io.File;
import java.util.Objects;

import static java.util.Optional.ofNullable;
import static org.folio.service.processing.reader.MarcJsonReader.JSON_EXTENSION;
import static org.folio.service.processing.reader.MarcXmlReader.XML_EXTENSION;

/**
 * Builds source reader depending on job profile type
 */
public class SourceReaderBuilder {

  private static final String MARC_RAW_CHUNK_SIZE_KEY = "file.processing.marc.raw.buffer.chunk.size";
  private static final String MARC_JSON_CHUNK_SIZE_KEY = "file.processing.marc.json.buffer.chunk.size";
  private static final String MARC_XML_CHUNK_SIZE_KEY = "file.processing.marc.xml.buffer.chunk.size";
  private static final String EDIFACT_CHUNK_SIZE_KEY = "file.processing.edifact.buffer.chunk.size";
  private static final int MARC_RAW_CHUNK_SIZE = Integer.parseInt(System.getProperty(MARC_RAW_CHUNK_SIZE_KEY, "50"));
  private static final int MARC_JSON_CHUNK_SIZE = Integer.parseInt(System.getProperty(MARC_JSON_CHUNK_SIZE_KEY, "50"));
  private static final int MARC_XML_CHUNK_SIZE = Integer.parseInt(System.getProperty(MARC_XML_CHUNK_SIZE_KEY, "10"));
  private static final int EDIFACT_CHUNK_SIZE = Integer.parseInt(System.getProperty(EDIFACT_CHUNK_SIZE_KEY, "10"));

  private SourceReaderBuilder() {
  }

  public static SourceReader build(File file, JobProfileInfo jobProfile) {
    SourceReader sourceReader = null;
    String extension = FilenameUtils.getExtension(file.getName());

    if (isMarc(jobProfile)) {
      if (JSON_EXTENSION.equals(extension)) {
        sourceReader = new MarcJsonReader(file, MARC_JSON_CHUNK_SIZE);
      } else if (XML_EXTENSION.equals(extension)) {
        sourceReader = new MarcXmlReader(file, MARC_XML_CHUNK_SIZE);
      } else {
        sourceReader = new MarcRawReader(file, MARC_RAW_CHUNK_SIZE);
      }
    } else if (isEdifact(jobProfile)) {
      sourceReader = new EdifactReader(file, EDIFACT_CHUNK_SIZE);
    }

    return ofNullable(sourceReader).orElseThrow(() -> new UnsupportedOperationException("Unsupported file format"));
  }

  private static boolean isEdifact(JobProfileInfo jobProfile) {
    return Objects.nonNull(jobProfile) && (jobProfile.getDataType() == JobProfileInfo.DataType.EDIFACT);
  }

  private static boolean isMarc(JobProfileInfo jobProfile) {
    return Objects.nonNull(jobProfile) && jobProfile.getDataType() == JobProfileInfo.DataType.MARC;
  }
}
