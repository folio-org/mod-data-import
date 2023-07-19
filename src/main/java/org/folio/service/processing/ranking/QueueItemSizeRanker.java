package org.folio.service.processing.ranking;

import java.util.Map;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Service for ranking queue items based on the parent job's size
 */
@Component
public class QueueItemSizeRanker implements QueueItemRanker {

  // we default to zeroes since, if the env variables are not present,
  // then we should not score on this metric
  @Value("${SCORE_JOB_SMALLEST:0}")
  private int scoreSmallest;

  @Value("${SCORE_JOB_LARGEST:0}")
  private int scoreLargest;

  @Value("${SCORE_JOB_LARGE_REFERENCE:0}")
  private int scoreLargeReference;

  @Override
  public double score(
    DataImportQueueItem queueItem,
    Map<String, Integer> tenantUsage
  ) {
    return ScoreUtils.calculateUnboundedLogarithmicScore(
      queueItem.getOriginalSize(),
      scoreSmallest,
      scoreLargest,
      scoreLargeReference
    );
  }
}
