package org.folio.service.processing.ranking;

import java.time.Instant;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.springframework.beans.factory.annotation.Value;

/**
 * Service for ranking queue items based on their age
 */
public class QueueItemAgeRanker implements QueueItemRankerService {

  // we default to zeroes since, if the env variables are not present, we
  // should not score on age
  @Value("${SCORE_AGE_NEWEST:0}")
  private int scoreAgeNewest;

  @Value("${SCORE_AGE_OLDEST:0}")
  private int scoreAgeOldest;

  @Value("${SCORE_AGE_EXTREME_THRESHOLD:0}")
  private int scoreAgeExtremeThreshold;

  @Value("${SCORE_AGE_EXTREME_VALUE:0}")
  private int scoreAgeExtremeValue;

  @Override
  public double score(DataImportQueueItem queueItem) {
    Instant createdAt = Instant.parse(queueItem.getTimestamp());
    Instant now = Instant.now();

    long age = now.getEpochSecond() - createdAt.getEpochSecond();

    return ScoreUtils.calculateBoundedLogarithmicScore(
      age,
      scoreAgeNewest,
      scoreAgeOldest,
      scoreAgeExtremeThreshold,
      scoreAgeExtremeValue
    );
  }
}
