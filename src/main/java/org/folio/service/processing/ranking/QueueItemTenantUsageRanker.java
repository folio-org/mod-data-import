package org.folio.service.processing.ranking;

import java.util.Map;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Service for ranking queue items based on how much of the worker pool is in
 * use by a tenant, to prevent saturation by a single tenant in multi-tenant
 * environments.
 * In a single-tenant environment, this will have no effect, as all jobs will
 * have the same tenant and therefore be affected equally by this ranker.
 */
@Component
public class QueueItemTenantUsageRanker implements QueueItemRanker {

  private int scoreNoWorkers;
  private int scoreAllWorkers;

  // we default to zeroes since, if the env variables are not present,
  // then we should not score on this metric
  @Autowired
  public QueueItemTenantUsageRanker(
    @Value("${SCORE_TENANT_USAGE_MIN:0}") int scoreNoWorkers,
    @Value("${SCORE_TENANT_USAGE_MAX:0}") int scoreAllWorkers
  ) {
    this.scoreNoWorkers = scoreNoWorkers;
    this.scoreAllWorkers = scoreAllWorkers;
  }

  @Override
  public double score(
    DataImportQueueItem queueItem,
    Map<String, Long> tenantUsage
  ) {
    // this may not be an accurate calculation if not all workers are currently
    // in use.  however, if this is the case, that indicates that the pool is
    // not saturated (therefore no competition), meaning any tenant which wants
    // to run a job can start their job immediately, and this ranking has
    // effectively no effect.
    Long totalWorkers = tenantUsage.values().stream().reduce(0L, Long::sum);

    // same reasoning as above; if there are no workers in use, then we don't
    // care about the result of this metric (since all tenants should be
    // equivalently using 0 workers).  This is needed to prevent an exception
    // on division
    if (totalWorkers == 0) {
      totalWorkers = 1L;
    }

    return ScoreUtils.calculateLinearScore(
      (double) tenantUsage.getOrDefault(queueItem.getTenant(), 0L) /
      totalWorkers,
      scoreNoWorkers,
      scoreAllWorkers
    );
  }
}
