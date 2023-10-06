package org.folio.service.processing.ranking;

import java.util.Map;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Service for ranking queue items based on their part number, to ensure first
 * parts are processed first
 */
@Component
public class QueueItemPartNumberRanker implements QueueItemRanker {

  private int scoreFirst;
  private int scoreLast;
  private int scoreLastReference;

  @Autowired
  public QueueItemPartNumberRanker(
    @Value("${SCORE_PART_NUMBER_FIRST:1}") int scoreFirst,
    @Value("${SCORE_PART_NUMBER_LAST:0}") int scoreLast,
    @Value("${SCORE_PART_NUMBER_LAST_REFERENCE:100}") int scoreLastReference
  ) {
    this.scoreFirst = scoreFirst;
    this.scoreLast = scoreLast;
    this.scoreLastReference = scoreLastReference;
  }

  @Override
  public double score(
    DataImportQueueItem queueItem,
    Map<String, Long> tenantUsage
  ) {
    return ScoreUtils.calculateUnboundedLogarithmicScore(
      // log calculation expects zero index
      queueItem.getPartNumber() - 1L,
      scoreFirst,
      scoreLast,
      scoreLastReference
    );
  }
}
