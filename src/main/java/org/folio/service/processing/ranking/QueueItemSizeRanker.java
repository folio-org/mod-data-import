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

  @Autowired
  public QueueItemSizeRanker(
    @Value("${SCORE_JOB_SMALLEST:40}") int scoreSmallest,
    @Value("${SCORE_JOB_LARGEST:-40}") int scoreLargest,
    @Value("${SCORE_JOB_LARGE_REFERENCE:100000}") int scoreLargeReference
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
