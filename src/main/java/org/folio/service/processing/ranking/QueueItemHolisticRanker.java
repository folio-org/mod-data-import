package org.folio.service.processing.ranking;

import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Service for ranking queue items based on all of their properties
 */
public class QueueItemHolisticRanker implements QueueItemRankerService {

  @Autowired
  private QueueItemAgeRanker ageRanker;

  @Override
  public double score(DataImportQueueItem queueItem) {
    return ageRanker.score(queueItem);
  }
}
