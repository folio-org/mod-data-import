package org.folio.service.processing.splitter;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Implementation does record separation based on content delimiter.
 *
 * @see RecordFormat
 * @see RecordSplitterBuilder
 */
public class RecordSplitterImpl implements RecordSplitter {

  private RecordFormat.ContentDelimiter contentDelimiter;

  public RecordSplitterImpl(RecordFormat.ContentDelimiter contentDelimiter) {
    this.contentDelimiter = contentDelimiter;
  }

  @Override
  public List<String> split(String incomingRecords) {
    StringTokenizer stringTokenizer =
      new StringTokenizer(incomingRecords, contentDelimiter.getRecordDelimiter());
    List<String> resultList = new ArrayList<>();
    while (stringTokenizer.hasMoreTokens()) {
      resultList.add(stringTokenizer.nextToken());
    }
    return resultList;
  }
}
