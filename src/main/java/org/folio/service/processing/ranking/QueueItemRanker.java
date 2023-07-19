package org.folio.service.processing.ranking;

import java.util.Map;
import org.folio.rest.jaxrs.model.DataImportQueueItem;

public interface QueueItemRanker {
  /**
   * Calculate the score for a queue item based on the class's configuration
   */
  double score(DataImportQueueItem queueItem, Map<String, Long> tenantUsage);
}
