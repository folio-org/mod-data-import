package org.folio.service.processing.ranking;

import org.folio.rest.jaxrs.model.DataImportQueueItem;

public interface QueueItemRankerService {
  /**
   * Calculate the score for a queue item based on the service's configuration
   */
  double score(DataImportQueueItem queueItem);
}
