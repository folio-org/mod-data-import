package org.folio.service.processing.ranking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.util.Map;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class QueueItemPartNumberRankerTest extends AbstractQueueItemRankerTest {

  QueueItemPartNumberRanker ranker;

  public QueueItemPartNumberRankerTest() {
    this.ranker = new QueueItemPartNumberRanker(1, 0, 31);
  }

  private DataImportQueueItem ofPart(int partNumber) {
    return new DataImportQueueItem().withPartNumber(partNumber);
  }

  @DisplayName("should return correct score based on part number")
  @Test
  void shouldReturnCorrectScore_whenPartNumberVaries() {
    assertThat(ranker.score(ofPart(1), Map.of())).isCloseTo(1, within(EPSILON));
    assertThat(ranker.score(ofPart(2), Map.of())).isCloseTo(0.8, within(EPSILON));
    assertThat(ranker.score(ofPart(4), Map.of())).isCloseTo(0.6, within(EPSILON));
    assertThat(ranker.score(ofPart(8), Map.of())).isCloseTo(0.4, within(EPSILON));
    assertThat(ranker.score(ofPart(16), Map.of())).isCloseTo(0.2, within(EPSILON));
    assertThat(ranker.score(ofPart(32), Map.of())).isCloseTo(0, within(EPSILON));
    assertThat(ranker.score(ofPart(64), Map.of())).isCloseTo(-0.2, within(EPSILON));
  }
}
