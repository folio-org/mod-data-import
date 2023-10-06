package org.folio.service.processing.ranking;

import java.time.Instant;
import java.util.Map;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Service for ranking queue items based on their age
 */
@Component
public class QueueItemAgeRanker implements QueueItemRanker {

  private static final int SECONDS_PER_MINUTE = 60;

  private int scoreAgeNewest;
  private int scoreAgeOldest;
  private int scoreAgeExtremeThresholdMinutes;
  private int scoreAgeExtremeValue;

  @Autowired
  public QueueItemAgeRanker(
    @Value("${SCORE_AGE_NEWEST:0}") int scoreAgeNewest,
    @Value("${SCORE_AGE_OLDEST:50}") int scoreAgeOldest,
    @Value(
      "${SCORE_AGE_EXTREME_THRESHOLD_MINUTES:480}"
    ) int scoreAgeExtremeThresholdMinutes,
    @Value("${SCORE_AGE_EXTREME_VALUE:10000}") int scoreAgeExtremeValue
  ) {
    this.scoreAgeNewest = scoreAgeNewest;
    this.scoreAgeOldest = scoreAgeOldest;
    this.scoreAgeExtremeThresholdMinutes = scoreAgeExtremeThresholdMinutes;
    this.scoreAgeExtremeValue = scoreAgeExtremeValue;
  }

  @Override
  public double score(
    DataImportQueueItem queueItem,
    Map<String, Long> tenantUsage
  ) {
    Instant createdAt = queueItem.getTimestamp().toInstant();
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
