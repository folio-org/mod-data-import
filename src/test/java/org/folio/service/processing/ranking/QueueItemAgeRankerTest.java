package org.folio.service.processing.ranking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Map;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class QueueItemAgeRankerTest extends AbstractQueueItemRankerTest {

  private final QueueItemAgeRanker ranker = new QueueItemAgeRanker(10, 100, 64, -1);

  @DisplayName("should return correct score based on item age")
  @Test
  void shouldReturnCorrectScore_whenItemAgeVaries() {
    assertThat(ranker.score(ofAge(0), Map.of())).isCloseTo(10, within(EPSILON));
    assertThat(ranker.score(ofAge(15), Map.of())).isCloseTo(70, within(EPSILON));
    assertThat(ranker.score(ofAge(63), Map.of())).isCloseTo(100, within(EPSILON));
    assertThat(ranker.score(ofAge(64), Map.of())).isCloseTo(-1, within(EPSILON));
    assertThat(ranker.score(ofAge(600), Map.of())).isCloseTo(-1, within(EPSILON));
  }

  private DataImportQueueItem ofAge(int age) {
    return new DataImportQueueItem()
      .withTimestamp(Date.from(Instant.now().minus(age, ChronoUnit.MINUTES)));
  }
}
