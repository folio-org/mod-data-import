package org.folio.service.processing.splitter;

/**
 * Enum defines source record format for proper record separation.
 */
public enum RecordFormat {
  MARC(
    ContentDelimiter.MARC_TXT,
    ContentDelimiter.MARC_JSON,
    ContentDelimiter.MARC_XML
  );

  private ContentDelimiter[] contentDelimiters;

  RecordFormat(ContentDelimiter... contentDelimiters) {
    this.contentDelimiters = contentDelimiters;
  }

  public ContentDelimiter findDelimiterBySource(String source) {
    for (ContentDelimiter contentDelimiter : contentDelimiters) {
      if (source.contains(contentDelimiter.recordDelimiter)) {
        return contentDelimiter;
      }
    }
    return null;
  }

  public enum ContentDelimiter {
    MARC_TXT("LEADER"),
    MARC_JSON("{\n\"leader\""),
    MARC_XML("<marc:record>");

    private String recordDelimiter;

    ContentDelimiter(String recordDelimiter) {
      this.recordDelimiter = recordDelimiter;
    }

    public String getRecordDelimiter() {
      return recordDelimiter;
    }
  }
}
