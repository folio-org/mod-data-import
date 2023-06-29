package org.folio.service.processing.ranking;

import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Service for ranking queue items based on all of their properties
 */
@Component
public class QueueItemHolisticRanker implements QueueItemRanker {

  @Autowired
  private QueueItemAgeRanker ageRanker;

  @Override
  public double score(DataImportQueueItem queueItem) {
    return ageRanker.score(queueItem);
  }
}
