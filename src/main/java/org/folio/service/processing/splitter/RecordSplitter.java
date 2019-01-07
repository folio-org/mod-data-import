package org.folio.service.processing.splitter;

import java.util.List;

/**
 * Record splitter does proper source records separation
 * depending on the {@link RecordFormat}.
 *
 * @see RecordSplitterBuilder
 * @see RecordFormat
 */
public interface RecordSplitter {

  List<String> split(String records);
}
