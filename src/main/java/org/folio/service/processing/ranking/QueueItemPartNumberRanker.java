package org.folio.service.processing.ranking;

import java.util.Map;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Service for ranking queue items based on their part number, to ensure first
 * parts are processed first
 */
@Component
public class QueueItemPartNumberRanker implements QueueItemRanker {

  // we default to zeroes since, if the env variables are not present,
  // then we should not score on this metric
  @Value("${SCORE_PART_NUMBER_FIRST:0}")
  private int scoreFirst;

  @Value("${SCORE_PART_NUMBER_LAST:0}")
  private int scoreLast;

  @Value("${SCORE_PART_NUMBER_LAST_REFERENCE:0}")
  private int scoreLastReference;

  @Override
  public double score(
    DataImportQueueItem queueItem,
    Map<String, Long> tenantUsage
  ) {
    return ScoreUtils.calculateUnboundedLogarithmicScore(
      queueItem.getPartNumber(),
      scoreFirst,
      scoreLast,
      scoreLastReference
    );
  }
}
