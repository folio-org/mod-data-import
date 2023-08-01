package org.folio.service.processing.ranking;

import java.util.Map;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Service for ranking queue items based on the parent job's size
 */
@Component
public class QueueItemSizeRanker implements QueueItemRanker {

  private int scoreSmallest;
  private int scoreLargest;
  private int scoreLargeReference;

  // we default to zeroes since, if the env variables are not present,
  // then we should not score on this metric
  @Autowired
  public QueueItemSizeRanker(
    @Value("${SCORE_JOB_SMALLEST:0}") int scoreSmallest,
    @Value("${SCORE_JOB_LARGEST:0}") int scoreLargest,
    @Value("${SCORE_JOB_LARGE_REFERENCE:0}") int scoreLargeReference
  ) {
    this.scoreSmallest = scoreSmallest;
    this.scoreLargest = scoreLargest;
    this.scoreLargeReference = scoreLargeReference;
  }

  @Override
  public double score(
    DataImportQueueItem queueItem,
    Map<String, Long> tenantUsage
  ) {
    return ScoreUtils.calculateUnboundedLogarithmicScore(
      queueItem.getOriginalSize(),
      scoreSmallest,
      scoreLargest,
      scoreLargeReference
    );
  }
}
