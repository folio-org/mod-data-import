package org.folio.service.processing.splitter;

public class RecordSplitterBuilder {

  public static RecordSplitter build(String source) {
    for (RecordFormat format : RecordFormat.values()) {
      RecordFormat.ContentDelimiter contentDelimiter = format.findDelimiterBySource(source);
      if (contentDelimiter != null) {
        return new RecordSplitterImpl(contentDelimiter);
      }
    }
    throw new IllegalArgumentException("Can not find proper RecordSplitter to split given source:" + source);
  }
}
