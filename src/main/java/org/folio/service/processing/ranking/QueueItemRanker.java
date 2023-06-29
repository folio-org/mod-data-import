package org.folio.service.processing.ranking;

import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.springframework.stereotype.Component;

@Component
public interface QueueItemRanker {
  /**
   * Calculate the score for a queue item based on the class's configuration
   */
  double score(DataImportQueueItem queueItem);
}
