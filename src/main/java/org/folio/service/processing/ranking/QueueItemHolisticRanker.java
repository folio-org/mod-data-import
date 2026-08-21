package org.folio.service.processing.ranking;

import java.util.Map;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Service for ranking queue items based on all of their properties.
 */
@Component
public class QueueItemHolisticRanker implements QueueItemRanker {

  private final QueueItemAgeRanker ageRanker;
  private final QueueItemPartNumberRanker partNumberRanker;
  private final QueueItemSizeRanker sizeRanker;
  private final QueueItemTenantUsageRanker tenantUsageRanker;

  @Autowired
  public QueueItemHolisticRanker(QueueItemAgeRanker ageRanker,
                                 QueueItemPartNumberRanker partNumberRanker,
                                 QueueItemSizeRanker sizeRanker,
                                 QueueItemTenantUsageRanker tenantUsageRanker) {
    this.ageRanker = ageRanker;
    this.partNumberRanker = partNumberRanker;
    this.sizeRanker = sizeRanker;
    this.tenantUsageRanker = tenantUsageRanker;
  }

  @Override
  public double score(DataImportQueueItem queueItem, Map<String, Long> tenantUsage) {
    return ageRanker.score(queueItem, tenantUsage)
           + partNumberRanker.score(queueItem, tenantUsage)
           + sizeRanker.score(queueItem, tenantUsage)
           + tenantUsageRanker.score(queueItem, tenantUsage);
  }
}
