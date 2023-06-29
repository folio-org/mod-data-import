package org.folio.service.processing.ranking;

import java.time.Instant;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Service for ranking queue items based on their age
 */
@Component
public class QueueItemAgeRanker implements QueueItemRanker {

  private static final int SECONDS_PER_MINUTE = 60;

  // we default to zeroes since, if the env variables are not present,
  // then we should not score on this metric
  @Value("${SCORE_AGE_NEWEST:0}")
  private int scoreAgeNewest;

  @Value("${SCORE_AGE_OLDEST:0}")
  private int scoreAgeOldest;

  @Value("${SCORE_AGE_EXTREME_THRESHOLD_MINUTES:0}")
  private int scoreAgeExtremeThresholdMinutes;

  @Value("${SCORE_AGE_EXTREME_VALUE:0}")
  private int scoreAgeExtremeValue;

  @Override
  public double score(DataImportQueueItem queueItem) {
    Instant createdAt = Instant.parse(queueItem.getTimestamp());
    Instant now = Instant.now();

    long age =
      (now.getEpochSecond() - createdAt.getEpochSecond()) / SECONDS_PER_MINUTE;

    return ScoreUtils.calculateBoundedLogarithmicScore(
      age,
      scoreAgeNewest,
      scoreAgeOldest,
      scoreAgeExtremeThresholdMinutes,
      scoreAgeExtremeValue
    );
  }
}
